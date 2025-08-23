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

from functools import partial
from unittest.mock import patch
import logging
import sys
import threading
from threading import Thread, Event
import time
from unittest import SkipTest

from cassandra import ConsistencyLevel, OperationTimedOut, DependencyException
from cassandra.cluster import NoHostAvailable, ConnectionShutdown, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.protocol import QueryMessage
from cassandra.policies import HostFilterPolicy, RoundRobinPolicy, HostStateListener
from cassandra.pool import HostConnectionPool

from tests import is_monkey_patched
from tests.integration import use_singledc, get_node, CASSANDRA_IP, local, \
    requiresmallclockgranularity, greaterthancass20, TestCluster

try:
    import cassandra.io.asyncorereactor
    from cassandra.io.asyncorereactor import AsyncoreConnection
except DependencyException:
    AsyncoreConnection = None

try:
    from cassandra.io.libevreactor import LibevConnection
    import cassandra.io.libevreactor
except DependencyException:
    LibevConnection = None


log = logging.getLogger(__name__)


def setup_module():
    use_singledc()


class ConnectionTimeoutTest(unittest.TestCase):

    def setUp(self):
        self.cluster = TestCluster(execution_profiles={
            EXEC_PROFILE_DEFAULT: ExecutionProfile(
                load_balancing_policy=HostFilterPolicy(
                    RoundRobinPolicy(), predicate=lambda host: host.address == CASSANDRA_IP
                )
            )
        })

        self.session = self.cluster.connect()

    def tearDown(self):
        self.cluster.shutdown()

    @patch('cassandra.connection.Connection.max_in_flight', 2)
    def test_in_flight_timeout(self):
        """
        Test to ensure that connection id fetching will block when max_id is reached/

        In previous versions of the driver this test will cause a
        NoHostAvailable exception to be thrown, when the max_id is restricted

        @since 3.3
        @jira_ticket PYTHON-514
        @expected_result When many requests are run on a single node connection acquisition should block
        until connection is available or the request times out.

        @test_category connection timeout
        """
        futures = []
        query = '''SELECT * FROM system.local'''
        for _ in range(100):
            futures.append(self.session.execute_async(query))

        for future in futures:
            future.result()


class TestHostListener(HostStateListener):
    host_down = None

    def on_down(self, host):
        self.host_down = True

    def on_up(self, host):
        self.host_down = False


class HeartbeatTest(unittest.TestCase):
    """
    Test to validate failing a heartbeat check doesn't mark a host as down

    @since 3.3
    @jira_ticket PYTHON-286
    @expected_result host should be marked down when heartbeat fails. This
    happens after PYTHON-734

    @test_category connection heartbeat
    """

    def setUp(self):
        self.cluster = TestCluster(idle_heartbeat_interval=1)
        self.session = self.cluster.connect(wait_for_all_pools=True)

    def tearDown(self):
        self.cluster.shutdown()

    @local
    @greaterthancass20
    def test_heart_beat_timeout(self):
        # Setup a host listener to ensure the nodes don't go down
        test_listener = TestHostListener()
        host = "127.0.0.1:9042"
        node = get_node(1)
        initial_connections = self.fetch_connections(host, self.cluster)
        self.assertNotEqual(len(initial_connections), 0)
        self.cluster.register_listener(test_listener)
        # Pause the node
        try:
            node.pause()
            # Wait for connections associated with this host go away
            self.wait_for_no_connections(host, self.cluster)

            # Wait to seconds for the driver to be notified
            time.sleep(2)
            self.assertTrue(test_listener.host_down)
            # Resume paused node
        finally:
            node.resume()
        # Run a query to ensure connections are re-established
        current_host = ""
        count = 0
        while current_host != host and count < 100:
            rs = self.session.execute_async("SELECT * FROM system.local", trace=False)
            rs.result()
            current_host = str(rs._current_host)
            count += 1
            time.sleep(.1)
        self.assertLess(count, 100, "Never connected to the first node")
        new_connections = self.wait_for_connections(host, self.cluster)
        self.assertFalse(test_listener.host_down)
        # Make sure underlying new connections don't match previous ones
        for connection in initial_connections:
            self.assertFalse(connection in new_connections)

    def fetch_connections(self, host, cluster):
        # Given a cluster object and host grab all connection associated with that host
        connections = []
        holders = cluster.get_connection_holders()
        for conn in holders:
            if host == str(getattr(conn, 'host', '')):
                if isinstance(conn, HostConnectionPool):
                    if conn._connections is not None and len(conn._connections) > 0:
                        connections.append(conn._connections)
                else:
                    if conn._connection is not None:
                        connections.append(conn._connection)
        return connections

    def wait_for_connections(self, host, cluster):
        retry = 0
        while(retry < 300):
            retry += 1
            connections = self.fetch_connections(host, cluster)
            if len(connections) is not 0:
                return connections
            time.sleep(.1)
        self.fail("No new connections found")

    def wait_for_no_connections(self, host, cluster):
        retry = 0
        while(retry < 100):
            retry += 1
            connections = self.fetch_connections(host, cluster)
            if len(connections) is 0:
                return
            time.sleep(.5)
        self.fail("Connections never cleared")


class ConnectionTests(object):

    klass = None

    def setUp(self):
        self.klass.initialize_reactor()

    def get_connection(self, timeout=5):
        """
        Helper method to solve automated testing issues within Jenkins.
        Officially patched under the 2.0 branch through
        17998ef72a2fe2e67d27dd602b6ced33a58ad8ef, but left as is for the
        1.0 branch due to possible regressions for fixing an
        automated testing edge-case.
        """
        conn = None
        e = None
        for i in range(5):
            try:
                contact_point = CASSANDRA_IP
                conn = self.klass.factory(
                    endpoint=contact_point,
                    timeout=timeout,
                    protocol_version=TestCluster.DEFAULT_PROTOCOL_VERSION,
                    allow_beta_protocol_version=TestCluster.DEFAULT_ALLOW_BETA
                )
                break
            except (OperationTimedOut, NoHostAvailable, ConnectionShutdown) as e:
                continue

        if conn:
            return conn
        else:
            raise e

    def test_single_connection(self):
        """
        Test a single connection with sequential requests.
        """
        conn = self.get_connection()
        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"
        event = Event()

        def cb(count, *args, **kwargs):
            count += 1
            if count >= 10:
                conn.close()
                event.set()
            else:
                conn.send_msg(
                    QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
                    request_id=0,
                    cb=partial(cb, count))

        conn.send_msg(
            QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
            request_id=0,
            cb=partial(cb, 0))
        event.wait()

    def test_single_connection_pipelined_requests(self):
        """
        Test a single connection with pipelined requests.
        """
        conn = self.get_connection()
        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"
        responses = [False] * 100
        event = Event()

        def cb(response_list, request_num, *args, **kwargs):
            response_list[request_num] = True
            if all(response_list):
                conn.close()
                event.set()

        for i in range(100):
            conn.send_msg(
                QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
                request_id=i,
                cb=partial(cb, responses, i))

        event.wait()

    def test_multiple_connections(self):
        """
        Test multiple connections with pipelined requests.
        """
        conns = [self.get_connection() for i in range(5)]
        events = [Event() for i in range(5)]
        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"

        def cb(event, conn, count, *args, **kwargs):
            count += 1
            if count >= 10:
                conn.close()
                event.set()
            else:
                conn.send_msg(
                    QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
                    request_id=count,
                    cb=partial(cb, event, conn, count))

        for event, conn in zip(events, conns):
            conn.send_msg(
                QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE),
                request_id=0,
                cb=partial(cb, event, conn, 0))

        for event in events:
            event.wait()

    def test_multiple_threads_shared_connection(self):
        """
        Test sharing a single connections across multiple threads,
        which will result in pipelined requests.
        """
        num_requests_per_conn = 25
        num_threads = 5
        event = Event()

        conn = self.get_connection()
        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"

        def cb(all_responses, thread_responses, request_num, *args, **kwargs):
            thread_responses[request_num] = True
            if all(map(all, all_responses)):
                conn.close()
                event.set()

        def send_msgs(all_responses, thread_responses):
            for i in range(num_requests_per_conn):
                qmsg = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)
                with conn.lock:
                    request_id = conn.get_request_id()
                conn.send_msg(qmsg, request_id, cb=partial(cb, all_responses, thread_responses, i))

        all_responses = []
        threads = []
        for i in range(num_threads):
            thread_responses = [False] * num_requests_per_conn
            all_responses.append(thread_responses)
            t = Thread(target=send_msgs, args=(all_responses, thread_responses))
            threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        event.wait()

    def test_multiple_threads_multiple_connections(self):
        """
        Test several threads, each with their own Connection and pipelined
        requests.
        """
        num_requests_per_conn = 25
        num_conns = 5
        events = [Event() for i in range(5)]

        query = "SELECT keyspace_name FROM system.schema_keyspaces LIMIT 1"

        def cb(conn, event, thread_responses, request_num, *args, **kwargs):
            thread_responses[request_num] = True
            if all(thread_responses):
                conn.close()
                event.set()

        def send_msgs(conn, event):
            thread_responses = [False] * num_requests_per_conn
            for i in range(num_requests_per_conn):
                qmsg = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)
                with conn.lock:
                    request_id = conn.get_request_id()
                conn.send_msg(qmsg, request_id, cb=partial(cb, conn, event, thread_responses, i))

            event.wait()

        threads = []
        for i in range(num_conns):
            conn = self.get_connection()
            t = Thread(target=send_msgs, args=(conn, events[i]))
            threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

    @requiresmallclockgranularity
    def test_connect_timeout(self):
        # Underlying socket implementations don't always throw a socket timeout even with min float
        # This can be timing sensitive, added retry to ensure failure occurs if it can
        max_retry_count = 10
        exception_thrown = False
        for i in range(max_retry_count):
            start = time.time()
            try:
                conn = self.get_connection(timeout=sys.float_info.min)
                conn.close()
            except Exception as e:
                end = time.time()
                self.assertAlmostEqual(start, end, 1)
                exception_thrown = True
                break
        self.assertTrue(exception_thrown)

    def test_subclasses_share_loop(self):

        if self.klass not in (AsyncoreConnection, LibevConnection):
            raise SkipTest

        class C1(self.klass):
            pass

        class C2(self.klass):
            pass

        clusterC1 = TestCluster(connection_class=C1)
        clusterC1.connect(wait_for_all_pools=True)

        clusterC2 = TestCluster(connection_class=C2)
        clusterC2.connect(wait_for_all_pools=True)
        self.addCleanup(clusterC1.shutdown)
        self.addCleanup(clusterC2.shutdown)

        self.assertEqual(len(get_eventloop_threads(self.event_loop_name)), 1)


def get_eventloop_threads(name):
    all_threads = list(threading.enumerate())
    log.debug('all threads: {}'.format(all_threads))
    log.debug('all names: {}'.format([thread.name for thread in all_threads]))
    event_loops_threads = [thread for thread in all_threads if name == thread.name]

    return event_loops_threads


class AsyncoreConnectionTests(ConnectionTests, unittest.TestCase):

    klass = AsyncoreConnection
    event_loop_name = "asyncore_cassandra_driver_event_loop"

    def setUp(self):
        if is_monkey_patched():
            raise unittest.SkipTest("Can't test asyncore with monkey patching")
        if AsyncoreConnection is None:
            raise unittest.SkipTest('Unable to import asyncore module')
        ConnectionTests.setUp(self)

    def clean_global_loop(self):
        cassandra.io.asyncorereactor._global_loop._cleanup()
        cassandra.io.asyncorereactor._global_loop = None


class LibevConnectionTests(ConnectionTests, unittest.TestCase):

    klass = LibevConnection
    event_loop_name = "event_loop"

    def setUp(self):
        if is_monkey_patched():
            raise unittest.SkipTest("Can't test libev with monkey patching")
        if LibevConnection is None:
            raise unittest.SkipTest(
                'libev does not appear to be installed properly')
        ConnectionTests.setUp(self)

    def clean_global_loop(self):
        cassandra.io.libevreactor._global_loop._cleanup()
        cassandra.io.libevreactor._global_loop = None
