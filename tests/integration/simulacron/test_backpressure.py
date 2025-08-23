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
import time

from cassandra import OperationTimedOut
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, NoHostAvailable
from cassandra.policies import RoundRobinPolicy, WhiteListRoundRobinPolicy
from tests.integration import requiressimulacron, libevtest
from tests.integration.simulacron import SimulacronBase, PROTOCOL_VERSION
from tests.integration.simulacron.utils import ResumeReads, PauseReads, prime_request, start_and_prime_singledc


@requiressimulacron
@libevtest
class TCPBackpressureTests(SimulacronBase):
    def setUp(self):
        self.callback_successes = 0
        self.callback_errors = 0

    def callback_success(self, results):
        self.callback_successes += 1

    def callback_error(self, results):
        self.callback_errors += 1

    def _fill_buffers(self, session, query, expected_blocked=3, **execute_kwargs):
        futures = []
        buffer = '1' * 50000
        for _ in range(100000):
            future = session.execute_async(query, [buffer], **execute_kwargs)
            futures.append(future)

            total_blocked = 0
            for pool in session.get_pools():
                if not pool._connection._socket_writable:
                    total_blocked += 1
            if total_blocked >= expected_blocked:
                break
        else:
            raise Exception("Unable to fill TCP send buffer on expected number of nodes")
        return futures

    def test_paused_connections(self):
        """  Verify all requests come back as expected if node resumes within query timeout """
        start_and_prime_singledc()
        profile = ExecutionProfile(request_timeout=500, load_balancing_policy=RoundRobinPolicy())
        cluster = Cluster(
            protocol_version=PROTOCOL_VERSION,
            compression=False,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        )
        session = cluster.connect(wait_for_all_pools=True)
        self.addCleanup(cluster.shutdown)

        query = session.prepare("INSERT INTO table1 (id) VALUES (?)")

        prime_request(PauseReads())
        futures = self._fill_buffers(session, query)

        # Make sure we actually have some stuck in-flight requests
        for in_flight in [pool._connection.in_flight for pool in session.get_pools()]:
            self.assertGreater(in_flight, 100)
        time.sleep(.5)
        for in_flight in [pool._connection.in_flight for pool in session.get_pools()]:
            self.assertGreater(in_flight, 100)

        prime_request(ResumeReads())

        for future in futures:
            try:
                future.result()
            except NoHostAvailable as e:
                # We shouldn't have any timeouts here, but all of the queries beyond what can fit
                # in the tcp buffer will have returned with a ConnectionBusy exception
                self.assertIn("ConnectionBusy", str(e))

        # Verify that we can continue sending queries without any problems
        for host in session.cluster.metadata.all_hosts():
            session.execute(query, ["a"], host=host)

    def test_queued_requests_timeout(self):
        """ Verify that queued requests timeout as expected """
        start_and_prime_singledc()
        profile = ExecutionProfile(request_timeout=.1, load_balancing_policy=RoundRobinPolicy())
        cluster = Cluster(
            protocol_version=PROTOCOL_VERSION,
            compression=False,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        )
        session = cluster.connect(wait_for_all_pools=True)
        self.addCleanup(cluster.shutdown)

        query = session.prepare("INSERT INTO table1 (id) VALUES (?)")

        prime_request(PauseReads())

        futures = []
        for i in range(1000):
            future = session.execute_async(query, [str(i)])
            future.add_callbacks(callback=self.callback_success, errback=self.callback_error)
            futures.append(future)

        successes = 0
        for future in futures:
            try:
                future.result()
                successes += 1
            except OperationTimedOut:
                pass

        # Simulacron will respond to a couple queries before cutting off reads, so we'll just verify
        # that only "a few" successes happened here
        self.assertLess(successes, 50)
        self.assertLess(self.callback_successes, 50)
        self.assertEqual(self.callback_errors, len(futures) - self.callback_successes)

    def test_cluster_busy(self):
        """ Verify that once TCP buffer is full we get busy exceptions rather than timeouts """
        start_and_prime_singledc()
        profile = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        cluster = Cluster(
            protocol_version=PROTOCOL_VERSION,
            compression=False,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        )
        session = cluster.connect(wait_for_all_pools=True)
        self.addCleanup(cluster.shutdown)

        query = session.prepare("INSERT INTO table1 (id) VALUES (?)")

        prime_request(PauseReads())

        # These requests will get stuck in the TCP buffer and we have no choice but to let them time out
        self._fill_buffers(session, query, expected_blocked=3)

        # Now that our send buffer is completely full, verify we immediately get busy exceptions rather than timing out
        for i in range(1000):
            with self.assertRaises(NoHostAvailable) as e:
                session.execute(query, [str(i)])
            self.assertIn("ConnectionBusy", str(e.exception))

    def test_node_busy(self):
        """ Verify that once TCP buffer is full, queries continue to get re-routed to other nodes """
        start_and_prime_singledc()
        profile = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        cluster = Cluster(
            protocol_version=PROTOCOL_VERSION,
            compression=False,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        )
        session = cluster.connect(wait_for_all_pools=True)
        self.addCleanup(cluster.shutdown)

        query = session.prepare("INSERT INTO table1 (id) VALUES (?)")

        prime_request(PauseReads(dc_id=0, node_id=0))

        blocked_profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]))
        cluster.add_execution_profile('blocked_profile', blocked_profile)

        # Fill our blocked node's tcp buffer until we get a busy exception
        self._fill_buffers(session, query, expected_blocked=1, execution_profile='blocked_profile')

        # Now that our send buffer is completely full on one node,
        # verify queries get re-routed to other nodes and queries complete successfully
        for i in range(1000):
            session.execute(query, [str(i)])

