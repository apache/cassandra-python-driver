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


from cassandra import ConsistencyLevel
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns, connection, models
from cassandra.cqlengine.management import sync_table
from cassandra.cluster import ExecutionProfile, _clusters_for_shutdown, _ConfigMode, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy
from cassandra.query import dict_factory

from tests.integration import CASSANDRA_IP, PROTOCOL_VERSION, execute_with_long_wait_retry, local, TestCluster
from tests.integration.cqlengine.base import BaseCassEngTestCase
from tests.integration.cqlengine import DEFAULT_KEYSPACE, setup_connection


class TestConnectModel(Model):

    id = columns.Integer(primary_key=True)
    keyspace = columns.Text()


class ConnectionTest(unittest.TestCase):
    def tearDown(self):
        connection.unregister_connection("default")

    @local
    def test_connection_setup_with_setup(self):
        connection.setup(hosts=None, default_keyspace=None)
        self.assertIsNotNone(connection.get_connection("default").cluster.metadata.get_host("127.0.0.1"))

    @local
    def test_connection_setup_with_default(self):
        connection.default()
        self.assertIsNotNone(connection.get_connection("default").cluster.metadata.get_host("127.0.0.1"))

    def test_only_one_connection_is_created(self):
        """
        Test to ensure that only one new connection is created by
        connection.register_connection

        @since 3.12
        @jira_ticket PYTHON-814
        @expected_result Only one connection is created

        @test_category object_mapper
        """
        number_of_clusters_before = len(_clusters_for_shutdown)
        connection.default()
        number_of_clusters_after = len(_clusters_for_shutdown)
        self.assertEqual(number_of_clusters_after - number_of_clusters_before, 1)


class SeveralConnectionsTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        connection.unregister_connection('default')
        cls.keyspace1 = 'ctest1'
        cls.keyspace2 = 'ctest2'
        super(SeveralConnectionsTest, cls).setUpClass()
        cls.setup_cluster = TestCluster()
        cls.setup_session = cls.setup_cluster.connect()
        ddl = "CREATE KEYSPACE {0} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '{1}'}}".format(cls.keyspace1, 1)
        execute_with_long_wait_retry(cls.setup_session, ddl)
        ddl = "CREATE KEYSPACE {0} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '{1}'}}".format(cls.keyspace2, 1)
        execute_with_long_wait_retry(cls.setup_session, ddl)

    @classmethod
    def tearDownClass(cls):
        execute_with_long_wait_retry(cls.setup_session, "DROP KEYSPACE {0}".format(cls.keyspace1))
        execute_with_long_wait_retry(cls.setup_session, "DROP KEYSPACE {0}".format(cls.keyspace2))
        models.DEFAULT_KEYSPACE = DEFAULT_KEYSPACE
        cls.setup_cluster.shutdown()
        setup_connection(DEFAULT_KEYSPACE)
        models.DEFAULT_KEYSPACE

    def setUp(self):
        self.c = TestCluster()
        self.session1 = self.c.connect(keyspace=self.keyspace1)
        self.session1.row_factory = dict_factory
        self.session2 = self.c.connect(keyspace=self.keyspace2)
        self.session2.row_factory = dict_factory

    def tearDown(self):
        self.c.shutdown()

    def test_connection_session_switch(self):
        """
        Test to ensure that when the default keyspace is changed in a session and that session,
        is set in the connection class, that the new default keyspace is honored.

        @since 3.1
        @jira_ticket PYTHON-486
        @expected_result CQLENGINE adopts whatever keyspace is passed in vai the set_session method as default

        @test_category object_mapper
        """

        connection.set_session(self.session1)
        sync_table(TestConnectModel)
        TCM1 = TestConnectModel.create(id=1, keyspace=self.keyspace1)
        connection.set_session(self.session2)
        sync_table(TestConnectModel)
        TCM2 = TestConnectModel.create(id=1, keyspace=self.keyspace2)
        connection.set_session(self.session1)
        self.assertEqual(1, TestConnectModel.objects.count())
        self.assertEqual(TestConnectModel.objects.first(), TCM1)
        connection.set_session(self.session2)
        self.assertEqual(1, TestConnectModel.objects.count())
        self.assertEqual(TestConnectModel.objects.first(), TCM2)


class ConnectionModel(Model):
    key = columns.Integer(primary_key=True)
    some_data = columns.Text()


class ConnectionInitTest(unittest.TestCase):
    def test_default_connection_uses_legacy(self):
        connection.default()
        conn = connection.get_connection()
        self.assertEqual(conn.cluster._config_mode, _ConfigMode.LEGACY)

    def test_connection_with_legacy_settings(self):
        connection.setup(
            hosts=[CASSANDRA_IP],
            default_keyspace=DEFAULT_KEYSPACE,
            consistency=ConsistencyLevel.LOCAL_ONE
        )
        conn = connection.get_connection()
        self.assertEqual(conn.cluster._config_mode, _ConfigMode.LEGACY)

    def test_connection_from_session_with_execution_profile(self):
        cluster = TestCluster(execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(row_factory=dict_factory)})
        session = cluster.connect()
        connection.default()
        connection.set_session(session)
        conn = connection.get_connection()
        self.assertEqual(conn.cluster._config_mode, _ConfigMode.PROFILES)

    def test_connection_from_session_with_legacy_settings(self):
        cluster = TestCluster(load_balancing_policy=RoundRobinPolicy())
        session = cluster.connect()
        session.row_factory = dict_factory
        connection.set_session(session)
        conn = connection.get_connection()
        self.assertEqual(conn.cluster._config_mode, _ConfigMode.LEGACY)

    def test_uncommitted_session_uses_legacy(self):
        cluster = TestCluster()
        session = cluster.connect()
        session.row_factory = dict_factory
        connection.set_session(session)
        conn = connection.get_connection()
        self.assertEqual(conn.cluster._config_mode, _ConfigMode.LEGACY)

    def test_legacy_insert_query(self):
        connection.setup(
            hosts=[CASSANDRA_IP],
            default_keyspace=DEFAULT_KEYSPACE,
            consistency=ConsistencyLevel.LOCAL_ONE
        )
        self.assertEqual(connection.get_connection().cluster._config_mode, _ConfigMode.LEGACY)

        sync_table(ConnectionModel)
        ConnectionModel.objects.create(key=0, some_data='text0')
        ConnectionModel.objects.create(key=1, some_data='text1')
        self.assertEqual(ConnectionModel.objects(key=0)[0].some_data, 'text0')

    def test_execution_profile_insert_query(self):
        cluster = TestCluster(execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(row_factory=dict_factory)})
        session = cluster.connect()
        connection.default()
        connection.set_session(session)
        self.assertEqual(connection.get_connection().cluster._config_mode, _ConfigMode.PROFILES)

        sync_table(ConnectionModel)
        ConnectionModel.objects.create(key=0, some_data='text0')
        ConnectionModel.objects.create(key=1, some_data='text1')
        self.assertEqual(ConnectionModel.objects(key=0)[0].some_data, 'text0')
