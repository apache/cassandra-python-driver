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
import time
import traceback

from cassandra import ConsistencyLevel, OperationTimedOut, ReadTimeout, WriteTimeout, Unavailable
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import SimpleStatement
from tests.integration import use_singledc, execute_until_pass, TestCluster

from tests.integration.long.utils import (
    force_stop, create_schema, wait_for_down, wait_for_up, start, CoordinatorStats
)

import unittest

ALL_CONSISTENCY_LEVELS = {
    ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.TWO, ConsistencyLevel.QUORUM,
    ConsistencyLevel.THREE, ConsistencyLevel.ALL, ConsistencyLevel.LOCAL_QUORUM,
    ConsistencyLevel.EACH_QUORUM
}
MULTI_DC_CONSISTENCY_LEVELS = {ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM}
SINGLE_DC_CONSISTENCY_LEVELS = ALL_CONSISTENCY_LEVELS - MULTI_DC_CONSISTENCY_LEVELS

log = logging.getLogger(__name__)


def setup_module():
    use_singledc()


class ConsistencyTests(unittest.TestCase):

    def setUp(self):
        self.coordinator_stats = CoordinatorStats()

    def _cl_failure(self, consistency_level, e):
        self.fail('Instead of success, saw %s for CL.%s:\n\n%s' % (
            e, ConsistencyLevel.value_to_name[consistency_level],
            traceback.format_exc()))

    def _cl_expected_failure(self, cl):
        self.fail('Test passed at ConsistencyLevel.%s:\n\n%s' % (
                  ConsistencyLevel.value_to_name[cl], traceback.format_exc()))

    def _insert(self, session, keyspace, count, consistency_level=ConsistencyLevel.ONE):
        session.execute('USE %s' % keyspace)
        for i in range(count):
            ss = SimpleStatement('INSERT INTO cf(k, i) VALUES (0, 0)',
                                 consistency_level=consistency_level)
            execute_until_pass(session, ss)

    def _query(self, session, keyspace, count, consistency_level=ConsistencyLevel.ONE):
        routing_key = struct.pack('>i', 0)
        for i in range(count):
            ss = SimpleStatement('SELECT * FROM cf WHERE k = 0',
                                 consistency_level=consistency_level,
                                 routing_key=routing_key)
            tries = 0
            while True:
                if tries > 100:
                    raise RuntimeError("Failed to execute query after 100 attempts: {0}".format(ss))
                try:
                    self.coordinator_stats.add_coordinator(session.execute_async(ss))
                    break
                except (OperationTimedOut, ReadTimeout):
                    ex_type, ex, tb = sys.exc_info()
                    log.warning("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                    del tb
                    tries += 1
                    time.sleep(1)

    def _assert_writes_succeed(self, session, keyspace, consistency_levels):
        for cl in consistency_levels:
            self.coordinator_stats.reset_counts()
            try:
                self._insert(session, keyspace, 1, cl)
            except Exception as e:
                self._cl_failure(cl, e)

    def _assert_reads_succeed(self, session, keyspace, consistency_levels, expected_reader=3):
        for cl in consistency_levels:
            self.coordinator_stats.reset_counts()
            try:
                self._query(session, keyspace, 1, cl)
                for i in range(3):
                    if i == expected_reader:
                        self.coordinator_stats.assert_query_count_equals(self, i, 1)
                    else:
                        self.coordinator_stats.assert_query_count_equals(self, i, 0)
            except Exception as e:
                self._cl_failure(cl, e)

    def _assert_writes_fail(self, session, keyspace, consistency_levels):
        for cl in consistency_levels:
            self.coordinator_stats.reset_counts()
            try:
                self._insert(session, keyspace, 1, cl)
                self._cl_expected_failure(cl)
            except (Unavailable, WriteTimeout):
                pass

    def _assert_reads_fail(self, session, keyspace, consistency_levels):
        for cl in consistency_levels:
            self.coordinator_stats.reset_counts()
            try:
                self._query(session, keyspace, 1, cl)
                self._cl_expected_failure(cl)
            except (Unavailable, ReadTimeout):
                pass

    def _test_tokenaware_one_node_down(self, keyspace, rf, accepted):
        cluster = TestCluster(
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(TokenAwarePolicy(RoundRobinPolicy()))}
        )
        session = cluster.connect(wait_for_all_pools=True)
        wait_for_up(cluster, 1)
        wait_for_up(cluster, 2)

        create_schema(cluster, session, keyspace, replication_factor=rf)
        self._insert(session, keyspace, count=1)
        self._query(session, keyspace, count=1)
        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 1)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        try:
            force_stop(2)
            wait_for_down(cluster, 2)

            self._assert_writes_succeed(session, keyspace, accepted)
            self._assert_reads_succeed(session, keyspace,
                                       accepted - set([ConsistencyLevel.ANY]))
            self._assert_writes_fail(session, keyspace,
                                     SINGLE_DC_CONSISTENCY_LEVELS - accepted)
            self._assert_reads_fail(session, keyspace,
                                    SINGLE_DC_CONSISTENCY_LEVELS - accepted)
        finally:
            start(2)
            wait_for_up(cluster, 2)

        cluster.shutdown()

    def test_rfone_tokenaware_one_node_down(self):
        self._test_tokenaware_one_node_down(
            keyspace='test_rfone_tokenaware',
            rf=1,
            accepted=set([ConsistencyLevel.ANY]))

    def test_rftwo_tokenaware_one_node_down(self):
        self._test_tokenaware_one_node_down(
            keyspace='test_rftwo_tokenaware',
            rf=2,
            accepted=set([ConsistencyLevel.ANY, ConsistencyLevel.ONE]))

    def test_rfthree_tokenaware_one_node_down(self):
        self._test_tokenaware_one_node_down(
            keyspace='test_rfthree_tokenaware',
            rf=3,
            accepted=set([ConsistencyLevel.ANY, ConsistencyLevel.ONE,
                          ConsistencyLevel.TWO, ConsistencyLevel.QUORUM]))

    def test_rfthree_tokenaware_none_down(self):
        keyspace = 'test_rfthree_tokenaware_none_down'
        cluster = TestCluster(
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(TokenAwarePolicy(RoundRobinPolicy()))}
        )
        session = cluster.connect(wait_for_all_pools=True)
        wait_for_up(cluster, 1)
        wait_for_up(cluster, 2)

        create_schema(cluster, session, keyspace, replication_factor=3)
        self._insert(session, keyspace, count=1)
        self._query(session, keyspace, count=1)
        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 1)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        self.coordinator_stats.reset_counts()

        self._assert_writes_succeed(session, keyspace, SINGLE_DC_CONSISTENCY_LEVELS)
        self._assert_reads_succeed(session, keyspace,
                                   SINGLE_DC_CONSISTENCY_LEVELS - set([ConsistencyLevel.ANY]),
                                   expected_reader=2)

        cluster.shutdown()

    def _test_downgrading_cl(self, keyspace, rf, accepted):
        cluster = TestCluster(execution_profiles={
            EXEC_PROFILE_DEFAULT: ExecutionProfile(TokenAwarePolicy(RoundRobinPolicy()),
                                                   DowngradingConsistencyRetryPolicy())
        })
        session = cluster.connect(wait_for_all_pools=True)

        create_schema(cluster, session, keyspace, replication_factor=rf)
        self._insert(session, keyspace, 1)
        self._query(session, keyspace, 1)
        self.coordinator_stats.assert_query_count_equals(self, 1, 0)
        self.coordinator_stats.assert_query_count_equals(self, 2, 1)
        self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        try:
            force_stop(2)
            wait_for_down(cluster, 2)

            self._assert_writes_succeed(session, keyspace, accepted)
            self._assert_reads_succeed(session, keyspace,
                                       accepted - set([ConsistencyLevel.ANY]))
            self._assert_writes_fail(session, keyspace,
                                     SINGLE_DC_CONSISTENCY_LEVELS - accepted)
            self._assert_reads_fail(session, keyspace,
                                    SINGLE_DC_CONSISTENCY_LEVELS - accepted)
        finally:
            start(2)
            wait_for_up(cluster, 2)

        cluster.shutdown()

    def test_rfone_downgradingcl(self):
        self._test_downgrading_cl(
            keyspace='test_rfone_downgradingcl',
            rf=1,
            accepted=set([ConsistencyLevel.ANY]))

    def test_rftwo_downgradingcl(self):
        self._test_downgrading_cl(
            keyspace='test_rftwo_downgradingcl',
            rf=2,
            accepted=SINGLE_DC_CONSISTENCY_LEVELS)

    def test_rfthree_roundrobin_downgradingcl(self):
        keyspace = 'test_rfthree_roundrobin_downgradingcl'
        with TestCluster(execution_profiles={
            EXEC_PROFILE_DEFAULT: ExecutionProfile(RoundRobinPolicy(),
                                                   DowngradingConsistencyRetryPolicy())
        }) as cluster:
            self.rfthree_downgradingcl(cluster, keyspace, True)

    def test_rfthree_tokenaware_downgradingcl(self):
        keyspace = 'test_rfthree_tokenaware_downgradingcl'
        with TestCluster(execution_profiles={
            EXEC_PROFILE_DEFAULT: ExecutionProfile(TokenAwarePolicy(RoundRobinPolicy()),
                                                   DowngradingConsistencyRetryPolicy())
        }) as cluster:
            self.rfthree_downgradingcl(cluster, keyspace, False)

    def rfthree_downgradingcl(self, cluster, keyspace, roundrobin):
        session = cluster.connect(wait_for_all_pools=True)

        create_schema(cluster, session, keyspace, replication_factor=2)
        self._insert(session, keyspace, count=12)
        self._query(session, keyspace, count=12)

        if roundrobin:
            self.coordinator_stats.assert_query_count_equals(self, 1, 4)
            self.coordinator_stats.assert_query_count_equals(self, 2, 4)
            self.coordinator_stats.assert_query_count_equals(self, 3, 4)
        else:
            self.coordinator_stats.assert_query_count_equals(self, 1, 0)
            self.coordinator_stats.assert_query_count_equals(self, 2, 12)
            self.coordinator_stats.assert_query_count_equals(self, 3, 0)

        try:
            self.coordinator_stats.reset_counts()
            force_stop(2)
            wait_for_down(cluster, 2)

            self._assert_writes_succeed(session, keyspace, SINGLE_DC_CONSISTENCY_LEVELS)

            # Test reads that expected to complete successfully
            for cl in SINGLE_DC_CONSISTENCY_LEVELS - set([ConsistencyLevel.ANY]):
                self.coordinator_stats.reset_counts()
                self._query(session, keyspace, 12, consistency_level=cl)
                if roundrobin:
                    self.coordinator_stats.assert_query_count_equals(self, 1, 6)
                    self.coordinator_stats.assert_query_count_equals(self, 2, 0)
                    self.coordinator_stats.assert_query_count_equals(self, 3, 6)
                else:
                    self.coordinator_stats.assert_query_count_equals(self, 1, 0)
                    self.coordinator_stats.assert_query_count_equals(self, 2, 0)
                    self.coordinator_stats.assert_query_count_equals(self, 3, 12)
        finally:
            start(2)
            wait_for_up(cluster, 2)

        session.cluster.shutdown()

    # TODO: can't be done in this class since we reuse the ccm cluster
    #       instead we should create these elsewhere
    # def test_rfthree_downgradingcl_twodcs(self):
    # def test_rfthree_downgradingcl_twodcs_dcaware(self):


class ConnectivityTest(unittest.TestCase):

    def get_node_not_x(self, node_to_stop):
        nodes = [1, 2, 3]
        for num in nodes:
            if num is not node_to_stop:
                return num

    def test_pool_with_host_down(self):
        """
        Test to ensure that cluster.connect() doesn't return prior to pools being initialized.

        This test will figure out which host our pool logic will connect to first. It then shuts that server down.
        Previously the cluster.connect() would return prior to the pools being initialized, and the first queries would
        return a no host exception

        @since 3.7.0
        @jira_ticket PYTHON-617
        @expected_result query should complete successfully

        @test_category connection
        """

        # find the first node, we will try create connections to, shut it down.

        # We will be shuting down a random house, so we need a complete contact list
        all_contact_points = ["127.0.0.1", "127.0.0.2", "127.0.0.3"]

        # Connect up and find out which host will bet queries routed to to first
        cluster = TestCluster()
        cluster.connect(wait_for_all_pools=True)
        hosts = cluster.metadata.all_hosts()
        address = hosts[0].address
        node_to_stop = int(address.split('.')[-1:][0])
        cluster.shutdown()

        # We now register a cluster that has it's Control Connection NOT on the node that we are shutting down.
        # We do this so we don't miss the event
        contact_point = '127.0.0.{0}'.format(self.get_node_not_x(node_to_stop))
        cluster = TestCluster(contact_points=[contact_point])
        cluster.connect(wait_for_all_pools=True)
        try:
            force_stop(node_to_stop)
            wait_for_down(cluster, node_to_stop)
            # Attempt a query against that node. It should complete
            cluster2 = TestCluster(contact_points=all_contact_points)
            session2 = cluster2.connect()
            session2.execute("SELECT * FROM system.local")
        finally:
            cluster2.shutdown()
            start(node_to_stop)
            wait_for_up(cluster, node_to_stop)
            cluster.shutdown()



