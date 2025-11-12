# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License
from cassandra.datastax.cloud import parse_metadata_info
from cassandra.query import SimpleStatement
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table, create_keyspace_simple
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns

import unittest

from ssl import SSLContext, PROTOCOL_TLS

from cassandra import DriverException, ConsistencyLevel, InvalidRequest
from cassandra.cluster import NoHostAvailable, ExecutionProfile, Cluster, _execution_profile_to_string
from cassandra.endpoint import SniEndPoint
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy, ConstantReconnectionPolicy

from unittest.mock import patch

from tests.integration import requirescloudproxy
from tests.util import wait_until_not_raised
from tests.integration.cloud import CloudProxyCluster, CLOUD_PROXY_SERVER

DISALLOWED_CONSISTENCIES = [
    ConsistencyLevel.ANY,
    ConsistencyLevel.ONE,
    ConsistencyLevel.LOCAL_ONE
]


@requirescloudproxy
class CloudTests(CloudProxyCluster):
    def hosts_up(self):
        return [h for h in self.cluster.metadata.all_hosts() if h.is_up]

    def test_resolve_and_connect(self):
        self.connect(self.creds)

        self.assertEqual(len(self.hosts_up()), 3)
        for host in self.cluster.metadata.all_hosts():
            self.assertTrue(host.is_up)
            self.assertIsInstance(host.endpoint, SniEndPoint)
            self.assertEqual(str(host.endpoint), "{}:{}:{}".format(
                host.endpoint.address, host.endpoint.port, host.host_id))
            self.assertIn(host.endpoint._resolved_address, ("127.0.0.1", '::1'))

    def test_match_system_local(self):
        self.connect(self.creds)

        self.assertEqual(len(self.hosts_up()), 3)
        for host in self.cluster.metadata.all_hosts():
            row = self.session.execute('SELECT * FROM system.local', host=host).one()
            self.assertEqual(row.host_id, host.host_id)
            self.assertEqual(row.rpc_address, host.broadcast_rpc_address)

    def test_set_auth_provider(self):
        self.connect(self.creds)
        self.assertIsInstance(self.cluster.auth_provider, PlainTextAuthProvider)
        self.assertEqual(self.cluster.auth_provider.username, 'user1')
        self.assertEqual(self.cluster.auth_provider.password, 'user1')

    def test_support_leaving_the_auth_unset(self):
        with self.assertRaises(NoHostAvailable):
            self.connect(self.creds_no_auth)
        self.assertIsNone(self.cluster.auth_provider)

    def test_support_overriding_auth_provider(self):
        try:
            self.connect(self.creds, auth_provider=PlainTextAuthProvider('invalid', 'invalid'))
        except:
            pass  # this will fail soon when sni_single_endpoint is updated
        self.assertIsInstance(self.cluster.auth_provider, PlainTextAuthProvider)
        self.assertEqual(self.cluster.auth_provider.username, 'invalid')
        self.assertEqual(self.cluster.auth_provider.password, 'invalid')

    def test_error_overriding_ssl_context(self):
        with self.assertRaises(ValueError) as cm:
            self.connect(self.creds, ssl_context=SSLContext(PROTOCOL_TLS))

        self.assertIn('cannot be specified with a cloud configuration', str(cm.exception))

    def test_error_overriding_ssl_options(self):
        with self.assertRaises(ValueError) as cm:
            self.connect(self.creds, ssl_options={'check_hostname': True})

        self.assertIn('cannot be specified with a cloud configuration', str(cm.exception))

    def _bad_hostname_metadata(self, config, http_data):
        config = parse_metadata_info(config, http_data)
        config.sni_host = "127.0.0.1"
        return config

    def test_verify_hostname(self):
        with patch('cassandra.datastax.cloud.parse_metadata_info', wraps=self._bad_hostname_metadata):
            with self.assertRaises(NoHostAvailable) as e:
                self.connect(self.creds)
            self.assertIn("hostname", str(e.exception).lower())

    def test_error_when_bundle_doesnt_exist(self):
        try:
            self.connect('/invalid/path/file.zip')
        except Exception as e:
            self.assertIsInstance(e, FileNotFoundError)

    def test_load_balancing_policy_is_dcawaretokenlbp(self):
        self.connect(self.creds)
        self.assertIsInstance(self.cluster.profile_manager.default.load_balancing_policy,
                              TokenAwarePolicy)
        self.assertIsInstance(self.cluster.profile_manager.default.load_balancing_policy._child_policy,
                              DCAwareRoundRobinPolicy)

    def test_resolve_and_reconnect_on_node_down(self):

        self.connect(self.creds,
                     idle_heartbeat_interval=1, idle_heartbeat_timeout=1,
                     reconnection_policy=ConstantReconnectionPolicy(120))

        self.assertEqual(len(self.hosts_up()), 3)
        CLOUD_PROXY_SERVER.stop_node(1)
        wait_until_not_raised(
            lambda: self.assertEqual(len(self.hosts_up()), 2),
            0.02, 250)

        host = [h for h in self.cluster.metadata.all_hosts() if not h.is_up][0]
        with patch.object(SniEndPoint, "resolve", wraps=host.endpoint.resolve) as mocked_resolve:
            CLOUD_PROXY_SERVER.start_node(1)
            wait_until_not_raised(
                lambda: self.assertEqual(len(self.hosts_up()), 3),
                0.02, 250)
            mocked_resolve.assert_called()

    def test_metadata_unreachable(self):
        with self.assertRaises(DriverException) as cm:
            self.connect(self.creds_unreachable, connect_timeout=1)

        self.assertIn('Unable to connect to the metadata service', str(cm.exception))

    def test_metadata_ssl_error(self):
        with self.assertRaises(DriverException) as cm:
            self.connect(self.creds_invalid_ca)

        self.assertIn('Unable to connect to the metadata', str(cm.exception))

    def test_default_consistency(self):
        self.connect(self.creds)
        self.assertEqual(self.session.default_consistency_level, ConsistencyLevel.LOCAL_QUORUM)
        # Verify EXEC_PROFILE_DEFAULT, EXEC_PROFILE_GRAPH_DEFAULT,
        # EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT, EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT
        for ep_key in self.cluster.profile_manager.profiles.keys():
            ep = self.cluster.profile_manager.profiles[ep_key]
            self.assertEqual(
                ep.consistency_level,
                ConsistencyLevel.LOCAL_QUORUM,
                "Expecting LOCAL QUORUM for profile {}, but got {} instead".format(
                _execution_profile_to_string(ep_key), ConsistencyLevel.value_to_name[ep.consistency_level]
                ))

    def test_default_consistency_of_execution_profiles(self):
        cloud_config = {'secure_connect_bundle': self.creds}
        self.cluster = Cluster(cloud=cloud_config, protocol_version=4, execution_profiles={
            'pre_create_default_ep': ExecutionProfile(),
            'pre_create_changed_ep': ExecutionProfile(
                consistency_level=ConsistencyLevel.LOCAL_ONE,
            ),
        })
        self.cluster.add_execution_profile('pre_connect_default_ep', ExecutionProfile())
        self.cluster.add_execution_profile(
            'pre_connect_changed_ep',
            ExecutionProfile(
                consistency_level=ConsistencyLevel.LOCAL_ONE,
            )
        )
        session = self.cluster.connect(wait_for_all_pools=True)

        self.cluster.add_execution_profile('post_connect_default_ep', ExecutionProfile())
        self.cluster.add_execution_profile(
            'post_connect_changed_ep',
            ExecutionProfile(
                consistency_level=ConsistencyLevel.LOCAL_ONE,
            )
        )

        for default in ['pre_create_default_ep', 'pre_connect_default_ep', 'post_connect_default_ep']:
            cl = self.cluster.profile_manager.profiles[default].consistency_level
            self.assertEqual(
                cl, ConsistencyLevel.LOCAL_QUORUM,
                "Expecting LOCAL QUORUM for profile {}, but got {} instead".format(default, cl)
            )
        for changed in ['pre_create_changed_ep', 'pre_connect_changed_ep', 'post_connect_changed_ep']:
            cl = self.cluster.profile_manager.profiles[changed].consistency_level
            self.assertEqual(
                cl, ConsistencyLevel.LOCAL_ONE,
                "Expecting LOCAL ONE for profile {}, but got {} instead".format(default, cl)
            )

    def test_consistency_guardrails(self):
        self.connect(self.creds)
        self.session.execute(
            "CREATE KEYSPACE IF NOT EXISTS test_consistency_guardrails "
            "with replication={'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        self.session.execute("CREATE TABLE IF NOT EXISTS test_consistency_guardrails.guardrails (id int primary key)")
        for consistency in DISALLOWED_CONSISTENCIES:
            statement = SimpleStatement(
                "INSERT INTO test_consistency_guardrails.guardrails (id) values (1)",
                consistency_level=consistency
            )
            with self.assertRaises(InvalidRequest) as e:
                self.session.execute(statement)
            self.assertIn('not allowed for Write Consistency Level', str(e.exception))

        # Sanity check to make sure we can do a normal insert
        statement = SimpleStatement(
            "INSERT INTO test_consistency_guardrails.guardrails (id) values (1)",
            consistency_level=ConsistencyLevel.LOCAL_QUORUM
        )
        try:
            self.session.execute(statement)
        except InvalidRequest:
            self.fail("InvalidRequest was incorrectly raised for write query at LOCAL QUORUM!")

    def test_cqlengine_can_connect(self):
        class TestModel(Model):
            id = columns.Integer(primary_key=True)
            val = columns.Text()

        connection.setup(None, "test", cloud={'secure_connect_bundle': self.creds})
        create_keyspace_simple('test', 1)
        sync_table(TestModel)
        TestModel.objects.create(id=42, value='test')
        self.assertEqual(len(TestModel.objects.all()), 1)
