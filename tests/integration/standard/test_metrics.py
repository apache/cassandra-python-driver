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

from cassandra.connection import ConnectionShutdown
from cassandra.policies import HostFilterPolicy, RoundRobinPolicy, FallthroughRetryPolicy

import unittest

from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel, WriteTimeout, Unavailable, ReadTimeout
from cassandra.protocol import SyntaxException

from cassandra.cluster import NoHostAvailable, ExecutionProfile, EXEC_PROFILE_DEFAULT
from tests.integration import get_cluster, get_node, use_singledc, execute_until_pass, TestCluster
from greplin import scales
from tests.integration import BasicSharedKeyspaceUnitTestCaseRF3WM, BasicExistingKeyspaceUnitTestCase, local

import pprint as pp


def setup_module():
    use_singledc()

@local
class MetricsTests(unittest.TestCase):

    def setUp(self):
        contact_point = ['127.0.0.2']
        self.cluster = TestCluster(contact_points=contact_point, metrics_enabled=True,
                                   execution_profiles=
                                   {EXEC_PROFILE_DEFAULT:
                                       ExecutionProfile(
                                           load_balancing_policy=HostFilterPolicy(
                                               RoundRobinPolicy(), lambda host: host.address in contact_point),
                                           retry_policy=FallthroughRetryPolicy()
                                       )
                                   }
                                   )
        self.session = self.cluster.connect("test3rf", wait_for_all_pools=True)

    def tearDown(self):
        self.cluster.shutdown()

    def test_connection_error(self):
        """
        Trigger and ensure connection_errors are counted
        Stop all node with the driver knowing about the "DOWN" states.
        """
        # Test writes
        for i in range(0, 100):
            self.session.execute_async("INSERT INTO test (k, v) VALUES ({0}, {1})".format(i, i))

        # Stop the cluster
        get_cluster().stop(wait=True, gently=False)

        try:
            # Ensure the nodes are actually down
            query = SimpleStatement("SELECT * FROM test", consistency_level=ConsistencyLevel.ALL)
            # both exceptions can happen depending on when the connection has been detected as defunct
            with self.assertRaises((NoHostAvailable, ConnectionShutdown)):
                self.session.execute(query)
        finally:
            get_cluster().start(wait_for_binary_proto=True, wait_other_notice=True)
            # Give some time for the cluster to come back up, for the next test
            time.sleep(5)

        self.assertGreater(self.cluster.metrics.stats.connection_errors, 0)

    def test_write_timeout(self):
        """
        Trigger and ensure write_timeouts are counted
        Write a key, value pair. Pause a node without the coordinator node knowing about the "DOWN" state.
        Attempt a write at cl.ALL and receive a WriteTimeout.
        """

        # Test write
        self.session.execute("INSERT INTO test (k, v) VALUES (1, 1)")

        # Assert read
        query = SimpleStatement("SELECT * FROM test WHERE k=1", consistency_level=ConsistencyLevel.ALL)
        results = execute_until_pass(self.session, query)
        self.assertTrue(results)

        # Pause node so it shows as unreachable to coordinator
        get_node(1).pause()

        try:
            # Test write
            query = SimpleStatement("INSERT INTO test (k, v) VALUES (2, 2)", consistency_level=ConsistencyLevel.ALL)
            with self.assertRaises(WriteTimeout):
                self.session.execute(query, timeout=None)
            self.assertEqual(1, self.cluster.metrics.stats.write_timeouts)

        finally:
            get_node(1).resume()

    def test_read_timeout(self):
        """
        Trigger and ensure read_timeouts are counted
        Write a key, value pair. Pause a node without the coordinator node knowing about the "DOWN" state.
        Attempt a read at cl.ALL and receive a ReadTimeout.
        """


        # Test write
        self.session.execute("INSERT INTO test (k, v) VALUES (1, 1)")

        # Assert read
        query = SimpleStatement("SELECT * FROM test WHERE k=1", consistency_level=ConsistencyLevel.ALL)
        results = execute_until_pass(self.session, query)
        self.assertTrue(results)

        # Pause node so it shows as unreachable to coordinator
        get_node(1).pause()

        try:
            # Test read
            query = SimpleStatement("SELECT * FROM test", consistency_level=ConsistencyLevel.ALL)
            with self.assertRaises(ReadTimeout):
                self.session.execute(query, timeout=None)
            self.assertEqual(1, self.cluster.metrics.stats.read_timeouts)

        finally:
            get_node(1).resume()

    def test_unavailable(self):
        """
        Trigger and ensure unavailables are counted
        Write a key, value pair. Stop a node with the coordinator node knowing about the "DOWN" state.
        Attempt an insert/read at cl.ALL and receive a Unavailable Exception.
        """

        # Test write
        self.session.execute("INSERT INTO test (k, v) VALUES (1, 1)")

        # Assert read
        query = SimpleStatement("SELECT * FROM test WHERE k=1", consistency_level=ConsistencyLevel.ALL)
        results = execute_until_pass(self.session, query)
        self.assertTrue(results)

        # Stop node gracefully
        # Sometimes this commands continues with the other nodes having not noticed
        # 1 is down, and a Timeout error is returned instead of an Unavailable
        get_node(1).stop(wait=True, wait_other_notice=True)
        time.sleep(5)
        try:
            # Test write
            query = SimpleStatement("INSERT INTO test (k, v) VALUES (2, 2)", consistency_level=ConsistencyLevel.ALL)
            with self.assertRaises(Unavailable):
                self.session.execute(query)
            self.assertEqual(self.cluster.metrics.stats.unavailables, 1)

            # Test write
            query = SimpleStatement("SELECT * FROM test", consistency_level=ConsistencyLevel.ALL)
            with self.assertRaises(Unavailable):
                self.session.execute(query, timeout=None)
            self.assertEqual(self.cluster.metrics.stats.unavailables, 2)
        finally:
            get_node(1).start(wait_other_notice=True, wait_for_binary_proto=True)
            # Give some time for the cluster to come back up, for the next test
            time.sleep(5)

        self.cluster.shutdown()

    # def test_other_error(self):
    #     # TODO: Bootstrapping or Overloaded cases
    #     pass
    #
    # def test_ignore(self):
    #     # TODO: Look for ways to generate ignores
    #     pass
    #
    # def test_retry(self):
    #     # TODO: Look for ways to generate retries
    #     pass


class MetricsNamespaceTest(BasicSharedKeyspaceUnitTestCaseRF3WM):
    @local
    def test_metrics_per_cluster(self):
        """
        Test to validate that metrics can be scopped to invdividual clusters
        @since 3.6.0
        @jira_ticket PYTHON-561
        @expected_result metrics should be scopped to a cluster level

        @test_category metrics
        """

        cluster2 = TestCluster(
            metrics_enabled=True,
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(retry_policy=FallthroughRetryPolicy())}
        )
        cluster2.connect(self.ks_name, wait_for_all_pools=True)

        self.assertEqual(len(cluster2.metadata.all_hosts()), 3)

        query = SimpleStatement("SELECT * FROM {0}.{0}".format(self.ks_name), consistency_level=ConsistencyLevel.ALL)
        self.session.execute(query)

        # Pause node so it shows as unreachable to coordinator
        get_node(1).pause()

        try:
            # Test write
            query = SimpleStatement("INSERT INTO {0}.{0} (k, v) VALUES (2, 2)".format(self.ks_name), consistency_level=ConsistencyLevel.ALL)
            with self.assertRaises(WriteTimeout):
                self.session.execute(query, timeout=None)
        finally:
            get_node(1).resume()

        # Change the scales stats_name of the cluster2
        cluster2.metrics.set_stats_name('cluster2-metrics')

        stats_cluster1 = self.cluster.metrics.get_stats()
        stats_cluster2 = cluster2.metrics.get_stats()

        # Test direct access to stats
        self.assertEqual(1, self.cluster.metrics.stats.write_timeouts)
        self.assertEqual(0, cluster2.metrics.stats.write_timeouts)

        # Test direct access to a child stats
        self.assertNotEqual(0.0, self.cluster.metrics.request_timer['mean'])
        self.assertEqual(0.0, cluster2.metrics.request_timer['mean'])

        # Test access via metrics.get_stats()
        self.assertNotEqual(0.0, stats_cluster1['request_timer']['mean'])
        self.assertEqual(0.0, stats_cluster2['request_timer']['mean'])

        # Test access by stats_name
        self.assertEqual(0.0, scales.getStats()['cluster2-metrics']['request_timer']['mean'])

        cluster2.shutdown()

    def test_duplicate_metrics_per_cluster(self):
        """
        Test to validate that cluster metrics names can't overlap.
        @since 3.6.0
        @jira_ticket PYTHON-561
        @expected_result metric names should not be allowed to be same.

        @test_category metrics
        """
        cluster2 = TestCluster(
            metrics_enabled=True,
            monitor_reporting_enabled=False,
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(retry_policy=FallthroughRetryPolicy())}
        )

        cluster3 = TestCluster(
            metrics_enabled=True,
            monitor_reporting_enabled=False,
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(retry_policy=FallthroughRetryPolicy())}
        )

        # Ensure duplicate metric names are not allowed
        cluster2.metrics.set_stats_name("appcluster")
        cluster2.metrics.set_stats_name("appcluster")
        with self.assertRaises(ValueError):
            cluster3.metrics.set_stats_name("appcluster")
        cluster3.metrics.set_stats_name("devops")

        session2 = cluster2.connect(self.ks_name, wait_for_all_pools=True)
        session3 = cluster3.connect(self.ks_name, wait_for_all_pools=True)

        # Basic validation that naming metrics doesn't impact their segregation or accuracy
        for i in range(10):
            query = SimpleStatement("SELECT * FROM {0}.{0}".format(self.ks_name), consistency_level=ConsistencyLevel.ALL)
            session2.execute(query)

        for i in range(5):
            query = SimpleStatement("SELECT * FROM {0}.{0}".format(self.ks_name), consistency_level=ConsistencyLevel.ALL)
            session3.execute(query)

        self.assertEqual(cluster2.metrics.get_stats()['request_timer']['count'], 10)
        self.assertEqual(cluster3.metrics.get_stats()['request_timer']['count'], 5)

        # Check scales to ensure they are appropriately named
        self.assertTrue("appcluster" in scales._Stats.stats.keys())
        self.assertTrue("devops" in scales._Stats.stats.keys())

        cluster2.shutdown()
        cluster3.shutdown()


class RequestAnalyzer(object):
    """
    Class used to track request and error counts for a Session.
    Also computes statistics on encoded request size.
    """

    requests = scales.PmfStat('request size')
    errors = scales.IntStat('errors')
    successful = scales.IntStat("success")
    # Throw exceptions when invoked.
    throw_on_success = False
    throw_on_fail = False

    def __init__(self, session, throw_on_success=False, throw_on_fail=False):
        scales.init(self, '/request')
        # each instance will be registered with a session, and receive a callback for each request generated
        session.add_request_init_listener(self.on_request)
        self.throw_on_fail = throw_on_fail
        self.throw_on_success = throw_on_success

    def on_request(self, rf):
        # This callback is invoked each time a request is created, on the thread creating the request.
        # We can use this to count events, or add callbacks
        rf.add_callbacks(self.on_success, self.on_error, callback_args=(rf,), errback_args=(rf,))

    def on_success(self, _, response_future):
        # future callback on a successful request; just record the size
        self.requests.addValue(response_future.request_encoded_size)
        self.successful += 1
        if self.throw_on_success:
            raise AttributeError

    def on_error(self, _, response_future):
        # future callback for failed; record size and increment errors
        self.requests.addValue(response_future.request_encoded_size)
        self.errors += 1
        if self.throw_on_fail:
            raise AttributeError

    def remove_ra(self, session):
        session.remove_request_init_listener(self.on_request)

    def __str__(self):
        # just extracting request count from the size stats (which are recorded on all requests)
        request_sizes = dict(self.requests)
        count = request_sizes.pop('count')
        return "%d requests (%d errors)\nRequest size statistics:\n%s" % (count, self.errors, pp.pformat(request_sizes))


class MetricsRequestSize(BasicExistingKeyspaceUnitTestCase):

    @classmethod
    def setUpClass(cls):
        cls.common_setup(1, keyspace_creation=False, monitor_reporting_enabled=False)

    def wait_for_count(self, ra, expected_count, error=False):
        for _ in range(10):
            if not error:
                if ra.successful is expected_count:
                    return True
            else:
                if ra.errors is expected_count:
                    return True
            time.sleep(.01)
        return False

    def test_metrics_per_cluster(self):
        """
        Test to validate that requests listeners.

        This test creates a simple metrics based request listener to track request size, it then
        check to ensure that on_success and on_error methods are invoked appropriately.
        @since 3.7.0
        @jira_ticket PYTHON-284
        @expected_result in_error, and on_success should be invoked appropriately

        @test_category metrics
        """

        ra = RequestAnalyzer(self.session)
        for _ in range(10):
            self.session.execute("SELECT release_version FROM system.local")

        for _ in range(3):
            try:
                self.session.execute("nonsense")
            except SyntaxException:
                continue

        self.assertTrue(self.wait_for_count(ra, 10))
        self.assertTrue(self.wait_for_count(ra, 3, error=True))

        ra.remove_ra(self.session)

        # Make sure a poorly coded RA doesn't cause issues
        ra = RequestAnalyzer(self.session, throw_on_success=False, throw_on_fail=True)
        self.session.execute("SELECT release_version FROM system.local")
        
        ra.remove_ra(self.session)

        RequestAnalyzer(self.session, throw_on_success=True)
        try:
            self.session.execute("nonsense")
        except SyntaxException:
            pass
