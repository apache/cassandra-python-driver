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

import logging
import struct
import sys
import traceback
from cassandra import cqltypes

from cassandra import ConsistencyLevel, Unavailable, OperationTimedOut, ReadTimeout, ReadFailure, \
    WriteTimeout, WriteFailure
from cassandra.cluster import NoHostAvailable, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.metadata import murmur3
from cassandra.policies import (
    RoundRobinPolicy, DCAwareRoundRobinPolicy,
    TokenAwarePolicy, WhiteListRoundRobinPolicy,
    HostFilterPolicy
)
from cassandra.query import SimpleStatement

from tests.integration import use_singledc, use_multidc, remove_cluster, TestCluster, greaterthanorequalcass40, notdse
from tests.integration.long.utils import (wait_for_up, create_schema,
                                          CoordinatorStats, force_stop,
                                          wait_for_down, decommission, start,
                                          bootstrap, stop, IP_FORMAT)

import unittest

log = logging.getLogger(__name__)


class LoadBalancingPolicyTests(unittest.TestCase):

    def setUp(self):
        remove_cluster()  # clear ahead of test so it doesn't use one left in unknown state
        self.coordinator_stats = CoordinatorStats()
        self.prepared = None
        self.probe_cluster = None

    def tearDown(self):
        if self.probe_cluster:
            self.probe_cluster.shutdown()

    @classmethod
    def teardown_class(cls):
        remove_cluster()

    def _connect_probe_cluster(self):
        if not self.probe_cluster:
            # distinct cluster so we can see the status of nodes ignored by the LBP being tested
            self.probe_cluster = TestCluster(
                schema_metadata_enabled=False,
                token_metadata_enabled=False,
                execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=RoundRobinPolicy())}
            )
            self.probe_session = self.probe_cluster.connect()

    def _wait_for_nodes_up(self, nodes, cluster=None):
        log.debug('entered: _wait_for_nodes_up(nodes={ns}, '
                  'cluster={cs})'.format(ns=nodes,
                                         cs=cluster))
        if not cluster:
            log.debug('connecting to cluster')
            self._connect_probe_cluster()
            cluster = self.probe_cluster
        for n in nodes:
            wait_for_up(cluster, n)

    def _wait_for_nodes_down(self, nodes, cluster=None):
        log.debug('entered: _wait_for_nodes_down(nodes={ns}, '
                  'cluster={cs})'.format(ns=nodes,
                                         cs=cluster))
        if not cluster:
            self._connect_probe_cluster()
            cluster = self.probe_cluster
        for n in nodes:
            wait_for_down(cluster, n)

    def _cluster_session_with_lbp(self, lbp):
        # create a cluster with no delay on events

        cluster = TestCluster(topology_event_refresh_window=0, status_event_refresh_window=0,
                              execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=lbp)})
        session = cluster.connect()
        return cluster, session

    def _insert(self, session, keyspace, count=12,
                consistency_level=ConsistencyLevel.ONE):
        log.debug('entered _insert('
                  'session={session}, keyspace={keyspace}, '
                  'count={count}, consistency_level={consistency_level}'
                  ')'.format(session=session, keyspace=keyspace, count=count,
                             consistency_level=consistency_level))
        session.execute('USE %s' % keyspace)
        ss = SimpleStatement('INSERT INTO cf(k, i) VALUES (0, 0)', consistency_level=consistency_level)

        tries = 0
        while tries < 100:
            try:
                execute_concurrent_with_args(session, ss, [None] * count)
                log.debug('Completed _insert on try #{}'.format(tries + 1))
                return
            except (OperationTimedOut, WriteTimeout, WriteFailure):
                ex_type, ex, tb = sys.exc_info()
                log.warning("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                del tb
                tries += 1

        raise RuntimeError("Failed to execute query after 100 attempts: {0}".format(ss))

    def _query(self, session, keyspace, count=12,
               consistency_level=ConsistencyLevel.ONE, use_prepared=False):
        log.debug('entered _query('
                  'session={session}, keyspace={keyspace}, '
                  'count={count}, consistency_level={consistency_level}, '
                  'use_prepared={use_prepared}'
                  ')'.format(session=session, keyspace=keyspace, count=count,
                             consistency_level=consistency_level,
                             use_prepared=use_prepared))
        if use_prepared:
            query_string = 'SELECT * FROM %s.cf WHERE k = ?' % keyspace
            if not self.prepared or self.prepared.query_string != query_string:
                self.prepared = session.prepare(query_string)
                self.prepared.consistency_level = consistency_level
            for i in range(count):
                tries = 0
                while True:
                    if tries > 100:
                        raise RuntimeError("Failed to execute query after 100 attempts: {0}".format(self.prepared))
                    try:
                        self.coordinator_stats.add_coordinator(session.execute_async(self.prepared.bind((0,))))
                        break
                    except (OperationTimedOut, ReadTimeout, ReadFailure):
                        ex_type, ex, tb = sys.exc_info()
                        log.warning("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                        del tb
                        tries += 1
        else:
            routing_key = struct.pack('>i', 0)
            for i in range(count):
                ss = SimpleStatement('SELECT * FROM %s.cf WHERE k = 0' % keyspace,
                                     consistency_level=consistency_level,
                                     routing_key=routing_key)
                tries = 0
                while True:
                    if tries > 100:
                        raise RuntimeError("Failed to execute query after 100 attempts: {0}".format(ss))
                    try:
                        self.coordinator_stats.add_coordinator(session.execute_async(ss))
                        break
                    except (OperationTimedOut, ReadTimeout, ReadFailure):
                        ex_type, ex, tb = sys.exc_info()
                        log.warning("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                        del tb
                        tries += 1

    def test_token_aware_is_used_by_default(self):
        """
        Test for default load balancing policy

        test_token_aware_is_used_by_default tests that the default loadbalancing policy is policies.TokenAwarePolicy.
        It creates a simple Cluster and verifies that the default loadbalancing policy is TokenAwarePolicy if the
        murmur3 C extension is found. Otherwise, the default loadbalancing policy is DCAwareRoundRobinPolicy.

        @since 2.6.0
        @jira_ticket PYTHON-160
        @expected_result TokenAwarePolicy should be the default loadbalancing policy.

        @test_category load_balancing:token_aware
        """

        cluster = TestCluster()
        self.addCleanup(cluster.shutdown)

        if murmur3 is not None:
            self.assertTrue(isinstance(cluster.profile_manager.default.load_balancing_policy, TokenAwarePolicy))
        else:
            self.assertTrue(isinstance(cluster.profile_manager.default.load_balancing_policy, DCAwareRoundRobinPolicy))

    def test_roundrobin(self):
        use_singledc()
        keyspace = 'test_roundrobin'
        cluster, session = self._cluster_session_with_lbp(RoundRobinPolicy())
        self.addCleanup(cluster.shutdown)

        self._wait_for_nodes_up(range(1, 4), cluster)
        create_schema(cluster, session, keyspace, replication_factor=3)
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 4)
        self.coordinator_stats.assert_query_count_equals(self, 2, 4)
        self.coordinator_stats.assert_query_count_equals(self, 3, 4)

        force_stop(3)
        self._wait_for_nodes_down([3], cluster)

        self.coordinator_stats.reset_counts()
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 6)
        self.coordinator_stats.assert_query_count_equals(self, 2, 6)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        decommission(1)
        start(3)
        self._wait_for_nodes_down([1], cluster)
        self._wait_for_nodes_up([3], cluster)

        self.coordinator_stats.reset_counts()
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 6)
        self.coordinator_stats.assert_query_count_equals(self, 3, 6)

    def test_roundrobin_two_dcs(self):
        use_multidc([2, 2])
        keyspace = 'test_roundrobin_two_dcs'
        cluster, session = self._cluster_session_with_lbp(RoundRobinPolicy())
        self.addCleanup(cluster.shutdown)
        self._wait_for_nodes_up(range(1, 5), cluster)

        create_schema(cluster, session, keyspace, replication_strategy=[2, 2])
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 3)
        self.coordinator_stats.assert_query_count_equals(self, 2, 3)
        self.coordinator_stats.assert_query_count_equals(self, 3, 3)
        self.coordinator_stats.assert_query_count_equals(self, 4, 3)

        force_stop(1)
        bootstrap(5, 'dc3')

        # reset control connection
        self._insert(session, keyspace, count=1000)

        self._wait_for_nodes_up([5], cluster)

        self.coordinator_stats.reset_counts()
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 3)
        self.coordinator_stats.assert_query_count_equals(self, 3, 3)
        self.coordinator_stats.assert_query_count_equals(self, 4, 3)
        self.coordinator_stats.assert_query_count_equals(self, 5, 3)

    def test_roundrobin_two_dcs_2(self):
        use_multidc([2, 2])
        keyspace = 'test_roundrobin_two_dcs_2'
        cluster, session = self._cluster_session_with_lbp(RoundRobinPolicy())
        self.addCleanup(cluster.shutdown)
        self._wait_for_nodes_up(range(1, 5), cluster)

        create_schema(cluster, session, keyspace, replication_strategy=[2, 2])
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 3)
        self.coordinator_stats.assert_query_count_equals(self, 2, 3)
        self.coordinator_stats.assert_query_count_equals(self, 3, 3)
        self.coordinator_stats.assert_query_count_equals(self, 4, 3)

        force_stop(1)
        bootstrap(5, 'dc1')

        # reset control connection
        self._insert(session, keyspace, count=1000)

        self._wait_for_nodes_up([5], cluster)

        self.coordinator_stats.reset_counts()
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 3)
        self.coordinator_stats.assert_query_count_equals(self, 3, 3)
        self.coordinator_stats.assert_query_count_equals(self, 4, 3)
        self.coordinator_stats.assert_query_count_equals(self, 5, 3)

    def test_dc_aware_roundrobin_two_dcs(self):
        use_multidc([3, 2])
        keyspace = 'test_dc_aware_roundrobin_two_dcs'
        cluster, session = self._cluster_session_with_lbp(DCAwareRoundRobinPolicy('dc1'))
        self.addCleanup(cluster.shutdown)
        self._wait_for_nodes_up(range(1, 6))

        create_schema(cluster, session, keyspace, replication_strategy=[2, 2])
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 4)
        self.coordinator_stats.assert_query_count_equals(self, 2, 4)
        self.coordinator_stats.assert_query_count_equals(self, 3, 4)
        self.coordinator_stats.assert_query_count_equals(self, 4, 0)
        self.coordinator_stats.assert_query_count_equals(self, 5, 0)

    def test_dc_aware_roundrobin_two_dcs_2(self):
        use_multidc([3, 2])
        keyspace = 'test_dc_aware_roundrobin_two_dcs_2'
        cluster, session = self._cluster_session_with_lbp(DCAwareRoundRobinPolicy('dc2'))
        self.addCleanup(cluster.shutdown)
        self._wait_for_nodes_up(range(1, 6))

        create_schema(cluster, session, keyspace, replication_strategy=[2, 2])
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 0)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)
        self.coordinator_stats.assert_query_count_equals(self, 4, 6)
        self.coordinator_stats.assert_query_count_equals(self, 5, 6)

    def test_dc_aware_roundrobin_one_remote_host(self):
        use_multidc([2, 2])
        keyspace = 'test_dc_aware_roundrobin_one_remote_host'
        cluster, session = self._cluster_session_with_lbp(DCAwareRoundRobinPolicy('dc2', used_hosts_per_remote_dc=1))
        self.addCleanup(cluster.shutdown)
        self._wait_for_nodes_up(range(1, 5))

        create_schema(cluster, session, keyspace, replication_strategy=[2, 2])
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 0)
        self.coordinator_stats.assert_query_count_equals(self, 3, 6)
        self.coordinator_stats.assert_query_count_equals(self, 4, 6)

        self.coordinator_stats.reset_counts()
        bootstrap(5, 'dc1')
        self._wait_for_nodes_up([5])

        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 0)
        self.coordinator_stats.assert_query_count_equals(self, 3, 6)
        self.coordinator_stats.assert_query_count_equals(self, 4, 6)
        self.coordinator_stats.assert_query_count_equals(self, 5, 0)

        self.coordinator_stats.reset_counts()
        decommission(3)
        decommission(4)
        self._wait_for_nodes_down([3, 4])

        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 3, 0)
        self.coordinator_stats.assert_query_count_equals(self, 4, 0)
        responses = set()
        for node in [1, 2, 5]:
            responses.add(self.coordinator_stats.get_query_count(node))
        self.assertEqual(set([0, 0, 12]), responses)

        self.coordinator_stats.reset_counts()
        decommission(5)
        self._wait_for_nodes_down([5])

        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 3, 0)
        self.coordinator_stats.assert_query_count_equals(self, 4, 0)
        self.coordinator_stats.assert_query_count_equals(self, 5, 0)
        responses = set()
        for node in [1, 2]:
            responses.add(self.coordinator_stats.get_query_count(node))
        self.assertEqual(set([0, 12]), responses)

        self.coordinator_stats.reset_counts()
        decommission(1)
        self._wait_for_nodes_down([1])

        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 12)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)
        self.coordinator_stats.assert_query_count_equals(self, 4, 0)
        self.coordinator_stats.assert_query_count_equals(self, 5, 0)

        self.coordinator_stats.reset_counts()
        force_stop(2)

        try:
            self._query(session, keyspace)
            self.fail()
        except NoHostAvailable:
            pass

    def test_token_aware(self):
        keyspace = 'test_token_aware'
        self.token_aware(keyspace)

    def test_token_aware_prepared(self):
        keyspace = 'test_token_aware_prepared'
        self.token_aware(keyspace, True)

    def token_aware(self, keyspace, use_prepared=False):
        use_singledc()
        cluster, session = self._cluster_session_with_lbp(TokenAwarePolicy(RoundRobinPolicy()))
        self.addCleanup(cluster.shutdown)
        self._wait_for_nodes_up(range(1, 4), cluster)

        create_schema(cluster, session, keyspace, replication_factor=1)
        self._insert(session, keyspace)
        self._query(session, keyspace, use_prepared=use_prepared)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 12)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        self.coordinator_stats.reset_counts()
        self._query(session, keyspace, use_prepared=use_prepared)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 12)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        self.coordinator_stats.reset_counts()
        force_stop(2)
        self._wait_for_nodes_down([2], cluster)

        try:
            self._query(session, keyspace, use_prepared=use_prepared)
            self.fail()
        except Unavailable as e:
            self.assertEqual(e.consistency, 1)
            self.assertEqual(e.required_replicas, 1)
            self.assertEqual(e.alive_replicas, 0)

        self.coordinator_stats.reset_counts()
        start(2)
        self._wait_for_nodes_up([2], cluster)

        self._query(session, keyspace, use_prepared=use_prepared)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 12)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        self.coordinator_stats.reset_counts()
        stop(2)
        self._wait_for_nodes_down([2], cluster)

        try:
            self._query(session, keyspace, use_prepared=use_prepared)
            self.fail()
        except Unavailable:
            pass

        self.coordinator_stats.reset_counts()
        start(2)
        self._wait_for_nodes_up([2], cluster)
        decommission(2)
        self._wait_for_nodes_down([2], cluster)

        self._query(session, keyspace, use_prepared=use_prepared)

        results = set([
            self.coordinator_stats.get_query_count(1),
            self.coordinator_stats.get_query_count(3)
        ])
        self.assertEqual(results, set([0, 12]))
        self.coordinator_stats.assert_query_count_equals(self, 2, 0)

    def test_token_aware_composite_key(self):
        use_singledc()
        keyspace = 'test_token_aware_composite_key'
        table = 'composite'
        cluster, session = self._cluster_session_with_lbp(TokenAwarePolicy(RoundRobinPolicy()))
        self.addCleanup(cluster.shutdown)
        self._wait_for_nodes_up(range(1, 4), cluster)

        create_schema(cluster, session, keyspace, replication_factor=2)
        session.execute('CREATE TABLE %s ('
                        'k1 int, '
                        'k2 int, '
                        'i int, '
                        'PRIMARY KEY ((k1, k2)))' % table)

        prepared = session.prepare('INSERT INTO %s '
                                   '(k1, k2, i) '
                                   'VALUES '
                                   '(?, ?, ?)' % table)
        bound = prepared.bind((1, 2, 3))
        result = session.execute(bound)
        self.assertIn(result.response_future.attempted_hosts[0],
                      cluster.metadata.get_replicas(keyspace, bound.routing_key))

        # There could be race condition with querying a node
        # which doesn't yet have the data so we query one of
        # the replicas
        results = session.execute(SimpleStatement('SELECT * FROM %s WHERE k1 = 1 AND k2 = 2' % table,
                                                  routing_key=bound.routing_key))
        self.assertIn(results.response_future.attempted_hosts[0],
                      cluster.metadata.get_replicas(keyspace, bound.routing_key))

        self.assertTrue(results[0].i)

    def test_token_aware_with_rf_2(self, use_prepared=False):
        use_singledc()
        keyspace = 'test_token_aware_with_rf_2'
        cluster, session = self._cluster_session_with_lbp(TokenAwarePolicy(RoundRobinPolicy()))
        self.addCleanup(cluster.shutdown)
        self._wait_for_nodes_up(range(1, 4), cluster)

        create_schema(cluster, session, keyspace, replication_factor=2)
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 12)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        self.coordinator_stats.reset_counts()
        stop(2)
        self._wait_for_nodes_down([2], cluster)

        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 0)
        self.coordinator_stats.assert_query_count_equals(self, 3, 12)

    def test_token_aware_with_local_table(self):
        use_singledc()
        cluster, session = self._cluster_session_with_lbp(TokenAwarePolicy(RoundRobinPolicy()))
        self.addCleanup(cluster.shutdown)
        self._wait_for_nodes_up(range(1, 4), cluster)

        p = session.prepare("SELECT * FROM system.local WHERE key=?")
        # this would blow up prior to 61b4fad
        r = session.execute(p, ('local',))
        self.assertEqual(r[0].key, 'local')

    def test_token_aware_with_shuffle_rf2(self):
        """
        Test to validate the hosts are shuffled when the `shuffle_replicas` is truthy
        @since 3.8
        @jira_ticket PYTHON-676
        @expected_result the request are spread across the replicas,
        when one of them is down, the requests target the available one

        @test_category policy
        """
        keyspace = 'test_token_aware_with_rf_2'
        cluster, session = self._set_up_shuffle_test(keyspace, replication_factor=2)
        self.addCleanup(cluster.shutdown)

        self._check_query_order_changes(session=session, keyspace=keyspace)

        # check TokenAwarePolicy still return the remaining replicas when one goes down
        self.coordinator_stats.reset_counts()
        stop(2)
        self._wait_for_nodes_down([2], cluster)

        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 0)
        self.coordinator_stats.assert_query_count_equals(self, 3, 12)

    def test_token_aware_with_shuffle_rf3(self):
        """
        Test to validate the hosts are shuffled when the `shuffle_replicas` is truthy
        @since 3.8
        @jira_ticket PYTHON-676
        @expected_result the request are spread across the replicas,
        when one of them is down, the requests target the other available ones

        @test_category policy
        """
        keyspace = 'test_token_aware_with_rf_3'
        cluster, session = self._set_up_shuffle_test(keyspace, replication_factor=3)
        self.addCleanup(cluster.shutdown)

        self._check_query_order_changes(session=session, keyspace=keyspace)

        # check TokenAwarePolicy still return the remaining replicas when one goes down
        self.coordinator_stats.reset_counts()
        stop(1)
        self._wait_for_nodes_down([1], cluster)

        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        query_count_two = self.coordinator_stats.get_query_count(2)
        query_count_three = self.coordinator_stats.get_query_count(3)
        self.assertEqual(query_count_two + query_count_three, 12)

        self.coordinator_stats.reset_counts()
        stop(2)
        self._wait_for_nodes_down([2], cluster)

        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 0)
        self.coordinator_stats.assert_query_count_equals(self, 3, 12)

    @notdse
    @greaterthanorequalcass40
    def test_token_aware_with_transient_replication(self):
        """
        Test to validate that the token aware policy doesn't route any request to a transient node.

        @since 3.23
        @jira_ticket PYTHON-1207
        @expected_result the requests are spread across the 2 full replicas and
        no other nodes are queried by the coordinator.

        @test_category policy
        """
        # We can test this with a single dc when CASSANDRA-15670 is fixed
        use_multidc([3, 3])

        cluster, session = self._cluster_session_with_lbp(
            TokenAwarePolicy(DCAwareRoundRobinPolicy(), shuffle_replicas=True)
        )
        self.addCleanup(cluster.shutdown)

        session.execute("CREATE KEYSPACE test_tr WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3/1', 'dc2': '3/1'};")
        session.execute("CREATE TABLE test_tr.users (id int PRIMARY KEY, username text) WITH read_repair ='NONE';")
        for i in range(100):
            session.execute("INSERT INTO test_tr.users (id, username) VALUES (%d, 'user');" % (i,))

        query = session.prepare("SELECT * FROM test_tr.users WHERE id = ?")
        for i in range(100):
            f = session.execute_async(query, (i,), trace=True)
            full_dc1_replicas = [h for h in cluster.metadata.get_replicas('test_tr', cqltypes.Int32Type.serialize(i, cluster.protocol_version))
                                 if h.datacenter == 'dc1']
            self.assertEqual(len(full_dc1_replicas), 2)

            f.result()
            trace_hosts = [cluster.metadata.get_host(e.source) for e in f.get_query_trace().events]

            for h in f.attempted_hosts:
                self.assertIn(h, full_dc1_replicas)
            for h in trace_hosts:
                self.assertIn(h, full_dc1_replicas)


    def _set_up_shuffle_test(self, keyspace, replication_factor):
        use_singledc()
        cluster, session = self._cluster_session_with_lbp(
            TokenAwarePolicy(RoundRobinPolicy(), shuffle_replicas=True)
        )
        self._wait_for_nodes_up(range(1, 4), cluster)

        create_schema(cluster, session, keyspace, replication_factor=replication_factor)
        return cluster, session

    def _check_query_order_changes(self, session, keyspace):
        LIMIT_TRIES, tried, query_counts = 20, 0, set()

        while len(query_counts) <= 1:
            tried += 1
            if tried >= LIMIT_TRIES:
                raise Exception("After {0} tries shuffle returned the same output".format(LIMIT_TRIES))

            self._insert(session, keyspace)
            self._query(session, keyspace)

            loop_qcs = (self.coordinator_stats.get_query_count(1),
                        self.coordinator_stats.get_query_count(2),
                        self.coordinator_stats.get_query_count(3))

            query_counts.add(loop_qcs)
            self.assertEqual(sum(loop_qcs), 12)

            # end the loop if we get more than one query ordering
            self.coordinator_stats.reset_counts()

    def test_white_list(self):
        use_singledc()
        keyspace = 'test_white_list'

        cluster = TestCluster(
            contact_points=('127.0.0.2',), topology_event_refresh_window=0, status_event_refresh_window=0,
            execution_profiles={
                EXEC_PROFILE_DEFAULT: ExecutionProfile(
                    load_balancing_policy=WhiteListRoundRobinPolicy((IP_FORMAT % 2,))
                )
            }
        )
        self.addCleanup(cluster.shutdown)
        session = cluster.connect()
        self._wait_for_nodes_up([1, 2, 3])

        create_schema(cluster, session, keyspace)
        self._insert(session, keyspace)
        self._query(session, keyspace)

        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 12)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        # white list policy should not allow reconnecting to ignored hosts
        force_stop(3)
        self._wait_for_nodes_down([3])
        self.assertFalse(cluster.metadata.get_host(IP_FORMAT % 3).is_currently_reconnecting())

        self.coordinator_stats.reset_counts()
        force_stop(2)
        self._wait_for_nodes_down([2])

        try:
            self._query(session, keyspace)
            self.fail()
        except NoHostAvailable:
            pass

    def test_black_list_with_host_filter_policy(self):
        """
        Test to validate removing certain hosts from the query plan with
        HostFilterPolicy
        @since 3.8
        @jira_ticket PYTHON-961
        @expected_result the excluded hosts are ignored

        @test_category policy
        """
        use_singledc()
        keyspace = 'test_black_list_with_hfp'
        ignored_address = (IP_FORMAT % 2)
        hfp = HostFilterPolicy(
            child_policy=RoundRobinPolicy(),
            predicate=lambda host: host.address != ignored_address
        )
        cluster = TestCluster(
            contact_points=(IP_FORMAT % 1,),
            topology_event_refresh_window=0,
            status_event_refresh_window=0,
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=hfp)}
        )
        self.addCleanup(cluster.shutdown)
        session = cluster.connect()
        self._wait_for_nodes_up([1, 2, 3])

        self.assertNotIn(ignored_address, [h.address for h in hfp.make_query_plan()])

        create_schema(cluster, session, keyspace)
        self._insert(session, keyspace)
        self._query(session, keyspace)

        # RoundRobin doesn't provide a gurantee on the order of the hosts
        # so we will have that for 127.0.0.1 and 127.0.0.3 the count for one
        # will be 4 and for the other 8
        first_node_count = self.coordinator_stats.get_query_count(1)
        third_node_count = self.coordinator_stats.get_query_count(3)
        self.assertEqual(first_node_count + third_node_count, 12)
        self.assertTrue(first_node_count == 8 or first_node_count == 4)

        self.coordinator_stats.assert_query_count_equals(self, 2, 0)

        # policy should not allow reconnecting to ignored host
        force_stop(2)
        self._wait_for_nodes_down([2])
        self.assertFalse(cluster.metadata.get_host(ignored_address).is_currently_reconnecting())
