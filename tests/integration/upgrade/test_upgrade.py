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
from itertools import count

from cassandra.auth import PlainTextAuthProvider, SaslAuthProvider
from cassandra.cluster import ConsistencyLevel, Cluster, DriverException, ExecutionProfile
from cassandra.policies import ConstantSpeculativeExecutionPolicy
from tests.integration.upgrade import UpgradeBase, UpgradeBaseAuth, UpgradePath, upgrade_paths

import unittest


# Previous Cassandra upgrade
two_to_three_path = upgrade_paths([
    UpgradePath("2.2.9-3.11", {"version": "2.2.9"}, {"version": "3.11.4"}, {}),
])

# Previous DSE upgrade
five_upgrade_path = upgrade_paths([
    UpgradePath("5.0.11-5.1.4", {"version": "5.0.11"}, {"version": "5.1.4"}, {}),
])


class UpgradeTests(UpgradeBase):
    @two_to_three_path
    def test_can_write(self):
        """
        Verify that the driver will keep querying C* even if there is a host down while being
        upgraded and that all the writes will eventually succeed
        @since 3.12
        @jira_ticket PYTHON-546
        @expected_result all the writes succeed

        @test_category upgrade
        """
        self.start_upgrade(0)

        self.cluster_driver.add_execution_profile("all", ExecutionProfile(consistency_level=ConsistencyLevel.ALL))
        self.cluster_driver.add_execution_profile("one", ExecutionProfile(consistency_level=ConsistencyLevel.LOCAL_ONE))

        c = count()
        while not self.is_upgraded():
            self.session.execute("INSERT INTO test3rf.test(k, v) VALUES (%s, 0)", (next(c), ), execution_profile="one")
            time.sleep(0.0001)

        total_number_of_inserted = self.session.execute("SELECT COUNT(*) from test3rf.test", execution_profile="all")[0][0]
        self.assertEqual(total_number_of_inserted, next(c))

        self.assertEqual(self.logger_handler.get_message_count("error", ""), 0)

    @two_to_three_path
    def test_can_connect(self):
        """
        Verify that the driver can connect to all the nodes
        despite some nodes being in different versions
        @since 3.12
        @jira_ticket PYTHON-546
        @expected_result the driver connects successfully and can execute queries against
        all the hosts

        @test_category upgrade
        """
        def connect_and_shutdown():
            cluster = Cluster()
            session = cluster.connect(wait_for_all_pools=True)
            queried_hosts = set()
            for _ in range(10):
                results = session.execute("SELECT * from system.local")
                self.assertGreater(len(results.current_rows), 0)
                self.assertEqual(len(results.response_future.attempted_hosts), 1)
                queried_hosts.add(results.response_future.attempted_hosts[0])
            self.assertEqual(len(queried_hosts), 3)
            cluster.shutdown()

        connect_and_shutdown()
        for node in self.nodes:
            self.upgrade_node(node)
            connect_and_shutdown()

        connect_and_shutdown()


class UpgradeTestsMetadata(UpgradeBase):
    @two_to_three_path
    def test_can_write(self):
        """
        Verify that the driver will keep querying C* even if there is a host down while being
        upgraded and that all the writes will eventually succeed
        @since 3.12
        @jira_ticket PYTHON-546
        @expected_result all the writes succeed

        @test_category upgrade
        """
        self.start_upgrade(0)

        self.cluster_driver.add_execution_profile("all", ExecutionProfile(consistency_level=ConsistencyLevel.ALL))
        self.cluster_driver.add_execution_profile("one", ExecutionProfile(consistency_level=ConsistencyLevel.LOCAL_ONE))

        c = count()
        while not self.is_upgraded():
            self.session.execute("INSERT INTO test3rf.test(k, v) VALUES (%s, 0)", (next(c),), execution_profile="one")
            time.sleep(0.0001)

        total_number_of_inserted = self.session.execute("SELECT COUNT(*) from test3rf.test", execution_profile="all")[0][0]
        self.assertEqual(total_number_of_inserted, next(c))

        self.assertEqual(self.logger_handler.get_message_count("error", ""), 0)

    @two_to_three_path
    def test_schema_metadata_gets_refreshed(self):
        """
        Verify that the driver fails to update the metadata while connected against
        different versions of nodes. This won't succeed because each node will report a
        different schema version

        @since 3.12
        @jira_ticket PYTHON-546
        @expected_result the driver raises DriverException when updating the schema
        metadata while upgrading
        all the hosts

        @test_category metadata
        """
        original_meta = self.cluster_driver.metadata.keyspaces
        number_of_nodes = len(self.cluster.nodelist())
        nodes = self.nodes
        for node in nodes[1:]:
            self.upgrade_node(node)
            # Wait for the control connection to reconnect
            time.sleep(20)

            with self.assertRaises(DriverException):
                self.cluster_driver.refresh_schema_metadata(max_schema_agreement_wait=10)

        self.upgrade_node(nodes[0])
        # Wait for the control connection to reconnect
        time.sleep(20)
        self.cluster_driver.refresh_schema_metadata(max_schema_agreement_wait=40)
        self.assertNotEqual(original_meta, self.cluster_driver.metadata.keyspaces)

    @two_to_three_path
    def test_schema_nodes_gets_refreshed(self):
        """
        Verify that the driver token map and node list gets rebuild correctly while upgrading.
        The token map and the node list should be the same after each node upgrade

        @since 3.12
        @jira_ticket PYTHON-546
        @expected_result the token map and the node list stays consistent with each node upgrade
        metadata while upgrading
        all the hosts

        @test_category metadata
        """
        for node in self.nodes:
            token_map = self.cluster_driver.metadata.token_map
            self.upgrade_node(node)
            # Wait for the control connection to reconnect
            time.sleep(20)

            self.cluster_driver.refresh_nodes(force_token_rebuild=True)
            self._assert_same_token_map(token_map, self.cluster_driver.metadata.token_map)

    def _assert_same_token_map(self, original, new):
        self.assertIsNot(original, new)
        self.assertEqual(original.tokens_to_hosts_by_ks, new.tokens_to_hosts_by_ks)
        self.assertEqual(original.token_to_host_owner, new.token_to_host_owner)
        self.assertEqual(original.ring, new.ring)


two_to_three_with_auth_path = upgrade_paths([
    UpgradePath("2.2.9-3.11-auth", {"version": "2.2.9"}, {"version": "3.11.4"},
                {'authenticator': 'PasswordAuthenticator',
                 'authorizer': 'CassandraAuthorizer'}),
])
class UpgradeTestsAuthentication(UpgradeBaseAuth):
    @two_to_three_with_auth_path
    def test_can_connect_auth_plain(self):
        """
        Verify that the driver can connect despite some nodes being in different versions
        with plain authentication
        @since 3.12
        @jira_ticket PYTHON-546
        @expected_result the driver connects successfully and can execute queries against
        all the hosts

        @test_category upgrade
        """
        auth_provider = PlainTextAuthProvider(
            username="cassandra",
            password="cassandra"
        )
        self.connect_and_shutdown(auth_provider)
        for node in self.nodes:
            self.upgrade_node(node)
            self.connect_and_shutdown(auth_provider)

        self.connect_and_shutdown(auth_provider)

    @two_to_three_with_auth_path
    def test_can_connect_auth_sasl(self):
        """
        Verify that the driver can connect despite some nodes being in different versions
        with ssl authentication
        @since 3.12
        @jira_ticket PYTHON-546
        @expected_result the driver connects successfully and can execute queries against
        all the hosts

        @test_category upgrade
        """
        sasl_kwargs = {'service': 'cassandra',
                       'mechanism': 'PLAIN',
                       'qops': ['auth'],
                       'username': 'cassandra',
                       'password': 'cassandra'}
        auth_provider = SaslAuthProvider(**sasl_kwargs)
        self.connect_and_shutdown(auth_provider)
        for node in self.nodes:
            self.upgrade_node(node)
            self.connect_and_shutdown(auth_provider)

        self.connect_and_shutdown(auth_provider)

    def connect_and_shutdown(self, auth_provider):
        cluster = Cluster(idle_heartbeat_interval=0,
                          auth_provider=auth_provider)
        session = cluster.connect(wait_for_all_pools=True)
        queried_hosts = set()
        for _ in range(10):
            results = session.execute("SELECT * from system.local")
            self.assertGreater(len(results.current_rows), 0)
            self.assertEqual(len(results.response_future.attempted_hosts), 1)
            queried_hosts.add(results.response_future.attempted_hosts[0])
        self.assertEqual(len(queried_hosts), 3)
        cluster.shutdown()


class UpgradeTestsPolicies(UpgradeBase):
    @two_to_three_path
    def test_can_write_speculative(self):
        """
        Verify that the driver will keep querying C* even if there is a host down while being
        upgraded and that all the writes will eventually succeed using the ConstantSpeculativeExecutionPolicy
        policy
        @since 3.12
        @jira_ticket PYTHON-546
        @expected_result all the writes succeed

        @test_category upgrade
        """
        spec_ep_rr = ExecutionProfile(speculative_execution_policy=ConstantSpeculativeExecutionPolicy(.5, 10),
                                      request_timeout=12)
        cluster = Cluster()
        self.addCleanup(cluster.shutdown)
        cluster.add_execution_profile("spec_ep_rr", spec_ep_rr)
        cluster.add_execution_profile("all", ExecutionProfile(consistency_level=ConsistencyLevel.ALL))
        session = cluster.connect()

        self.start_upgrade(0)

        c = count()
        while not self.is_upgraded():
            session.execute("INSERT INTO test3rf.test(k, v) VALUES (%s, 0)", (next(c),),
                                 execution_profile='spec_ep_rr')
            time.sleep(0.0001)

        total_number_of_inserted = session.execute("SELECT COUNT(*) from test3rf.test", execution_profile="all")[0][0]
        self.assertEqual(total_number_of_inserted, next(c))

        self.assertEqual(self.logger_handler.get_message_count("error", ""), 0)
