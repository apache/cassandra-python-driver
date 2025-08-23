# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest

import logging
import time
from unittest.mock import Mock, patch

from cassandra import OperationTimedOut
from cassandra.cluster import (EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile,
                               _Scheduler, NoHostAvailable)
from cassandra.policies import HostStateListener, RoundRobinPolicy, WhiteListRoundRobinPolicy

from tests import connection_class, thread_pool_executor_class
from tests.util import late
from tests.integration import requiressimulacron, libevtest
from tests.integration.util import assert_quiescent_pool_state
# important to import the patch PROTOCOL_VERSION from the simulacron module
from tests.integration.simulacron import SimulacronBase, PROTOCOL_VERSION
from cassandra.connection import DEFAULT_CQL_VERSION, Connection
from tests.unit.cython.utils import cythontest
from tests.integration.simulacron.utils import (NO_THEN, PrimeOptions,
                                                prime_query, prime_request,
                                                start_and_prime_cluster_defaults,
                                                start_and_prime_singledc,
                                                clear_queries, RejectConnections,
                                                RejectType, AcceptConnections, PauseReads, ResumeReads)


class TrackDownListener(HostStateListener):
    def __init__(self):
        self.hosts_marked_down = []

    def on_down(self, host):
        self.hosts_marked_down.append(host)

    def on_up(self, host):
        pass

    def on_add(self, host):
        pass

    def on_remove(self, host):
        pass

class ThreadTracker(thread_pool_executor_class):
    called_functions = []

    def submit(self, fn, *args, **kwargs):
        self.called_functions.append(fn.__name__)
        return super(ThreadTracker, self).submit(fn, *args, **kwargs)


class OrderedRoundRobinPolicy(RoundRobinPolicy):

    def make_query_plan(self, working_keyspace=None, query=None):
        self._position += 1

        hosts = []
        for _ in range(10):
            hosts.extend(sorted(self._live_hosts, key=lambda x : x.address))

        return hosts


def _send_options_message(self):
    """
    Mock that doesn't the OptionMessage. It is required for the heart_beat_timeout
    test to avoid a condition where the CC tries to reconnect in the executor but can't
    since we prime that message."""
    self._compressor = None
    self.cql_version = DEFAULT_CQL_VERSION
    self._send_startup_message(no_compact=self.no_compact)


@requiressimulacron
class ConnectionTests(SimulacronBase):

    @patch('cassandra.connection.Connection._send_options_message', _send_options_message)
    def test_heart_beat_timeout(self):
        """
        Test to ensure the hosts are marked as down after a OTO is received.
        Also to ensure this happens within the expected timeout
        @since 3.10
        @jira_ticket PYTHON-762
        @expected_result all the hosts have been marked as down at some point

        @test_category metadata
        """
        number_of_dcs = 3
        nodes_per_dc = 20

        query_to_prime = "INSERT INTO test3rf.test (k, v) VALUES (0, 1);"

        idle_heartbeat_timeout = 5
        idle_heartbeat_interval = 1

        start_and_prime_cluster_defaults(number_of_dcs, nodes_per_dc)

        listener = TrackDownListener()
        executor = ThreadTracker(max_workers=8)

        # We need to disable compression since it's not supported in simulacron
        cluster = Cluster(compression=False,
                          idle_heartbeat_interval=idle_heartbeat_interval,
                          idle_heartbeat_timeout=idle_heartbeat_timeout,
                          protocol_version=PROTOCOL_VERSION,
                          executor_threads=8,
                          execution_profiles={
                              EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=RoundRobinPolicy())})
        self.addCleanup(cluster.shutdown)

        cluster.scheduler.shutdown()
        cluster.executor = executor
        cluster.scheduler = _Scheduler(executor)

        session = cluster.connect(wait_for_all_pools=True)
        cluster.register_listener(listener)

        log = logging.getLogger()
        log.setLevel('CRITICAL')
        self.addCleanup(log.setLevel, "DEBUG")

        prime_query(query_to_prime, then=NO_THEN)

        futures = []
        for _ in range(number_of_dcs * nodes_per_dc):
            future = session.execute_async(query_to_prime)
            futures.append(future)

        for f in futures:
            f._event.wait()
            self.assertIsInstance(f._final_exception, OperationTimedOut)

        prime_request(PrimeOptions(then=NO_THEN))

        # We allow from some extra time for all the hosts to be to on_down
        # The callbacks should start happening after idle_heartbeat_timeout + idle_heartbeat_interval
        time.sleep((idle_heartbeat_timeout + idle_heartbeat_interval) * 2.5)

        for host in cluster.metadata.all_hosts():
            self.assertIn(host, listener.hosts_marked_down)

        # In this case HostConnection._replace shouldn't be called
        self.assertNotIn("_replace", executor.called_functions)

    def test_callbacks_and_pool_when_oto(self):
        """
        Test to ensure the callbacks are correcltly called and the connection
        is returned when there is an OTO
        @since 3.12
        @jira_ticket PYTHON-630
        @expected_result the connection is correctly returned to the pool
        after an OTO, also the only the errback is called and not the callback
        when the message finally arrives.

        @test_category metadata
        """
        start_and_prime_singledc()

        cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False)
        session = cluster.connect()
        self.addCleanup(cluster.shutdown)

        query_to_prime = "SELECT * from testkesypace.testtable"

        server_delay = 2  # seconds
        prime_query(query_to_prime, then={"delay_in_ms": server_delay * 1000})

        future = session.execute_async(query_to_prime, timeout=1)
        callback, errback = Mock(name='callback'), Mock(name='errback')
        future.add_callbacks(callback, errback)
        self.assertRaises(OperationTimedOut, future.result)

        assert_quiescent_pool_state(self, cluster)

        time.sleep(server_delay + 1)
        # PYTHON-630 -- only the errback should be called
        errback.assert_called_once()
        callback.assert_not_called()

    @cythontest
    @libevtest
    def test_heartbeat_defunct_deadlock(self):
        """
        Ensure that there is no deadlock when request is in-flight and heartbeat defuncts connection
        @since 3.16
        @jira_ticket PYTHON-1044
        @expected_result an OperationTimeout is raised and no deadlock occurs

        @test_category connection
        """
        start_and_prime_singledc()

        # This is all about timing. We will need the QUERY response future to time out and the heartbeat to defunct
        # at the same moment. The latter will schedule a QUERY retry to another node in case the pool is not
        # already shut down.  If and only if the response future timeout falls in between the retry scheduling and
        # its execution the deadlock occurs. The odds are low, so we need to help fate a bit:
        # 1) Make one heartbeat messages be sent to every node
        # 2) Our QUERY goes always to the same host
        # 3) This host needs to defunct first
        # 4) Open a small time window for the response future timeout, i.e. block executor threads for retry
        #    execution and last connection to defunct
        query_to_prime = "SELECT * from testkesypace.testtable"
        query_host = "127.0.0.2"
        heartbeat_interval = 1
        heartbeat_timeout = 1
        lag = 0.05
        never = 9999

        class PatchedRoundRobinPolicy(RoundRobinPolicy):
            # Send always to same host
            def make_query_plan(self, working_keyspace=None, query=None):
                if query and query.query_string == query_to_prime:
                    return filter(lambda h: h == query_host, self._live_hosts)
                else:
                    return super(PatchedRoundRobinPolicy, self).make_query_plan()

        class PatchedCluster(Cluster):
            # Make sure that QUERY connection will timeout first
            def get_connection_holders(self):
                holders = super(PatchedCluster, self).get_connection_holders()
                return sorted(holders, reverse=True, key=lambda v: int(v._connection.host == query_host))

            # Block executor thread like closing a dead socket could do
            def connection_factory(self, *args, **kwargs):
                conn = super(PatchedCluster, self).connection_factory(*args, **kwargs)
                conn.defunct = late(seconds=2*lag)(conn.defunct)
                return conn

        cluster = PatchedCluster(
            protocol_version=PROTOCOL_VERSION,
            compression=False,
            idle_heartbeat_interval=heartbeat_interval,
            idle_heartbeat_timeout=heartbeat_timeout,
            load_balancing_policy=PatchedRoundRobinPolicy()
        )
        session = cluster.connect()
        self.addCleanup(cluster.shutdown)

        prime_query(query_to_prime, then={"delay_in_ms": never})

        # Make heartbeat due
        time.sleep(heartbeat_interval)

        future = session.execute_async(query_to_prime, timeout=heartbeat_interval+heartbeat_timeout+3*lag)
        # Delay thread execution like kernel could do
        future._retry_task = late(seconds=4*lag)(future._retry_task)

        prime_request(PrimeOptions(then={"result": "no_result", "delay_in_ms": never}))
        prime_request(RejectConnections("unbind"))

        self.assertRaisesRegex(OperationTimedOut, "Connection defunct by heartbeat", future.result)

    def test_close_when_query(self):
        """
        Test to ensure the driver behaves correctly if the connection is closed
        just when querying
        @since 3.12
        @expected_result NoHostAvailable is risen

        @test_category connection
        """
        start_and_prime_singledc()

        cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False)
        session = cluster.connect()
        self.addCleanup(cluster.shutdown)

        query_to_prime = "SELECT * from testkesypace.testtable"

        for close_type in ("disconnect", "shutdown_read", "shutdown_write"):
            then = {
                "result": "close_connection",
                "delay_in_ms": 0,
                "close_type": close_type,
                "scope": "connection"
            }

            prime_query(query_to_prime, rows=None, column_types=None, then=then)
            self.assertRaises(NoHostAvailable, session.execute, query_to_prime)

    def test_retry_after_defunct(self):
        """
        We test cluster._retry is called if an the connection is defunct
        in the middle of a query

        Finally we verify the driver recovers correctly in the event
        of a network partition

        @since 3.12
        @expected_result the driver is able to query even if a host is marked
        as down in the middle of the query, it will go to the next one if the timeout
        hasn't expired

        @test_category connection
        """
        number_of_dcs = 3
        nodes_per_dc = 2

        query_to_prime = "INSERT INTO test3rf.test (k, v) VALUES (0, 1);"

        idle_heartbeat_timeout = 1
        idle_heartbeat_interval = 5

        simulacron_cluster = start_and_prime_cluster_defaults(number_of_dcs, nodes_per_dc)

        dc_ids = sorted(simulacron_cluster.data_center_ids)
        last_host = dc_ids.pop()
        prime_query(query_to_prime,
                    cluster_name="{}/{}".format(simulacron_cluster.cluster_name, last_host))

        roundrobin_lbp = OrderedRoundRobinPolicy()
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False,
                          idle_heartbeat_interval=idle_heartbeat_interval,
                          idle_heartbeat_timeout=idle_heartbeat_timeout,
                          execution_profiles={
                              EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=roundrobin_lbp)})

        session = cluster.connect(wait_for_all_pools=True)
        self.addCleanup(cluster.shutdown)

        # This simulates we only have access to one DC
        for dc_id in dc_ids:
            datacenter_path = "{}/{}".format(simulacron_cluster.cluster_name, dc_id)
            prime_query(query_to_prime, then=NO_THEN, cluster_name=datacenter_path)
            prime_request(PrimeOptions(then=NO_THEN, cluster_name=datacenter_path))

        # Only the last datacenter will respond, therefore the first host won't
        # We want to make sure the returned hosts are 127.0.0.1,  127.0.0.2, ... 127.0.0.8
        roundrobin_lbp._position = 0

        # After 3 + 1 seconds the connection should be marked and down and another host retried
        response_future = session.execute_async(query_to_prime, timeout=4 * idle_heartbeat_interval
                                                                        + idle_heartbeat_timeout)
        response_future.result()
        self.assertGreater(len(response_future.attempted_hosts), 1)

        # No error should be raised here since the hosts have been marked
        # as down and there's still 1 DC available
        for _ in range(10):
            session.execute(query_to_prime)

        # Might take some time to close the previous connections and reconnect
        time.sleep(10)
        assert_quiescent_pool_state(self, cluster)
        clear_queries()

        time.sleep(10)
        assert_quiescent_pool_state(self, cluster)

    def test_idle_connection_is_not_closed(self):
        """
        Test to ensure that the connections aren't closed if they are idle
        @since 3.12
        @jira_ticket PYTHON-573
        @expected_result the connections aren't closed nor the hosts are
        set to down if the connection is idle

        @test_category connection
        """
        start_and_prime_singledc()

        idle_heartbeat_timeout = 1
        idle_heartbeat_interval = 1

        listener = TrackDownListener()
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False,
                          idle_heartbeat_interval=idle_heartbeat_interval,
                          idle_heartbeat_timeout=idle_heartbeat_timeout)
        session = cluster.connect(wait_for_all_pools=True)
        cluster.register_listener(listener)

        self.addCleanup(cluster.shutdown)

        time.sleep(20)

        self.assertEqual(listener.hosts_marked_down, [])

    def test_host_is_not_set_to_down_after_query_oto(self):
        """
        Test to ensure that the connections aren't closed if there's an
        OperationTimedOut in a normal query. This should only happen from the
        heart beat thread (in the case of a OperationTimedOut) with the default
        configuration
        @since 3.12
        @expected_result the connections aren't closed nor the hosts are
        set to down

        @test_category connection
        """
        start_and_prime_singledc()

        query_to_prime = "SELECT * FROM madeup_keyspace.madeup_table"

        prime_query(query_to_prime, then=NO_THEN)

        listener = TrackDownListener()
        cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False)
        session = cluster.connect(wait_for_all_pools=True)
        cluster.register_listener(listener)

        futures = []
        for _ in range(10):
            future = session.execute_async(query_to_prime)
            futures.append(future)

        for f in futures:
            f._event.wait()
            self.assertIsInstance(f._final_exception, OperationTimedOut)

        self.assertEqual(listener.hosts_marked_down, [])
        assert_quiescent_pool_state(self, cluster)

    def test_can_shutdown_connection_subclass(self):
        start_and_prime_singledc()
        class ExtendedConnection(connection_class):
            pass

        cluster = Cluster(protocol_version=PROTOCOL_VERSION,
                          contact_points=["127.0.0.2"],
                          connection_class=ExtendedConnection,
                          compression=False)
        cluster.connect()
        cluster.shutdown()

    def test_driver_recovers_nework_isolation(self):
        start_and_prime_singledc()

        idle_heartbeat_timeout = 3
        idle_heartbeat_interval = 1

        listener = TrackDownListener()

        cluster = Cluster(protocol_version=PROTOCOL_VERSION, contact_points=['127.0.0.1'],
                          idle_heartbeat_timeout=idle_heartbeat_timeout,
                          idle_heartbeat_interval=idle_heartbeat_interval,
                          executor_threads=16,
                          compression=False,
                          execution_profiles={
                              EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
                          })
        session = cluster.connect(wait_for_all_pools=True)

        cluster.register_listener(listener)

        prime_request(PrimeOptions(then=NO_THEN))
        prime_request(RejectConnections(RejectType.REJECT_STARTUP))

        time.sleep((idle_heartbeat_timeout + idle_heartbeat_interval) * 2)

        for host in cluster.metadata.all_hosts():
            self.assertIn(host, listener.hosts_marked_down)

        self.assertRaises(NoHostAvailable, session.execute, "SELECT * from system.local")

        clear_queries()
        prime_request(AcceptConnections())

        time.sleep(idle_heartbeat_timeout + idle_heartbeat_interval + 2)

        self.assertIsNotNone(session.execute("SELECT * from system.local"))

    def test_max_in_flight(self):
        """ Verify we don't exceed max_in_flight when borrowing connections or sending heartbeats """
        Connection.max_in_flight = 50
        start_and_prime_singledc()
        profile = ExecutionProfile(request_timeout=1, load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']))
        cluster = Cluster(
            protocol_version=PROTOCOL_VERSION,
            compression=False,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            idle_heartbeat_interval=.1,
            idle_heartbeat_timeout=.1,
        )
        session = cluster.connect(wait_for_all_pools=True)
        self.addCleanup(cluster.shutdown)

        query = session.prepare("INSERT INTO table1 (id) VALUES (?)")

        prime_request(PauseReads())

        futures = []
        # + 50 because simulacron doesn't immediately block all queries
        for i in range(Connection.max_in_flight + 50):
            futures.append(session.execute_async(query, ['a']))

        prime_request(ResumeReads())

        for future in futures:
            # We're veryfing we don't get an assertion error from Connection.get_request_id,
            # so skip any valid errors
            try:
                future.result()
            except OperationTimedOut:
                pass
            except NoHostAvailable:
                pass
