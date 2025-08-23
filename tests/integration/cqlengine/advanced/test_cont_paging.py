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

from packaging.version import Version

from cassandra.cluster import (EXEC_PROFILE_DEFAULT,
                         ContinuousPagingOptions, ExecutionProfile,
                         ProtocolVersion)
from cassandra.cqlengine import columns, connection, models
from cassandra.cqlengine.management import drop_table, sync_table
from tests.integration import (DSE_VERSION, greaterthanorequaldse51,
                               greaterthanorequaldse60, requiredse, TestCluster)


class TestMultiKeyModel(models.Model):
    partition = columns.Integer(primary_key=True)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer(required=False)
    text = columns.Text(required=False)


def setup_module():
    if DSE_VERSION:
        sync_table(TestMultiKeyModel)
        for i in range(1000):
            TestMultiKeyModel.create(partition=i, cluster=i, count=5, text="text to write")


def teardown_module():
    if DSE_VERSION:
        drop_table(TestMultiKeyModel)


@requiredse
class BasicConcurrentTests():
    required_dse_version = None
    protocol_version = None
    connections = set()
    sane_connections = {"CONTDEFAULT"}

    @classmethod
    def setUpClass(cls):
        if DSE_VERSION:
            cls._create_cluster_with_cp_options("CONTDEFAULT", ContinuousPagingOptions())
            cls._create_cluster_with_cp_options("ONEPAGE", ContinuousPagingOptions(max_pages=1))
            cls._create_cluster_with_cp_options("MANYPAGES", ContinuousPagingOptions(max_pages=10))
            cls._create_cluster_with_cp_options("SLOW", ContinuousPagingOptions(max_pages_per_second=1))

    @classmethod
    def tearDownClass(cls):
        if not DSE_VERSION or DSE_VERSION < cls.required_dse_version:
            return

        cls.cluster_default.shutdown()
        connection.set_default_connection("default")

    @classmethod
    def _create_cluster_with_cp_options(cls, name, cp_options):
        execution_profiles = {EXEC_PROFILE_DEFAULT:
                                  ExecutionProfile(continuous_paging_options=cp_options)}
        cls.cluster_default = TestCluster(protocol_version=cls.protocol_version,
                                          execution_profiles=execution_profiles)
        cls.session_default = cls.cluster_default.connect(wait_for_all_pools=True)
        connection.register_connection(name, default=True, session=cls.session_default)
        cls.connections.add(name)

    def test_continuous_paging_basic(self):
        """
        Test to ensure that various continuous paging works with cqlengine
        for session
        @since DSE 2.4
        @jira_ticket PYTHON-872
        @expected_result various continous paging options should fetch all the results

        @test_category queries
        """
        for connection_name in self.sane_connections:
            connection.set_default_connection(connection_name)
            row = TestMultiKeyModel.get(partition=0, cluster=0)
            self.assertEqual(row.partition, 0)
            self.assertEqual(row.cluster, 0)
            rows = TestMultiKeyModel.objects().allow_filtering()
            self.assertEqual(len(rows), 1000)

    def test_fetch_size(self):
        """
        Test to ensure that various continuous paging works with different fetch sizes
        for session
        @since DSE 2.4
        @jira_ticket PYTHON-872
        @expected_result various continous paging options should fetch all the results

        @test_category queries
        """
        for connection_name in self.connections:
            conn = connection._connections[connection_name]
            initial_default = conn.session.default_fetch_size
            self.addCleanup(
                setattr,
                conn.session,
                "default_fetch_size",
                initial_default
            )

        connection.set_default_connection("ONEPAGE")
        for fetch_size in (2, 3, 7, 10, 99, 100, 101, 150):
            connection._connections["ONEPAGE"].session.default_fetch_size = fetch_size
            rows = TestMultiKeyModel.objects().allow_filtering()
            self.assertEqual(fetch_size, len(rows))

        connection.set_default_connection("MANYPAGES")
        for fetch_size in (2, 3, 7, 10, 15):
            connection._connections["MANYPAGES"].session.default_fetch_size = fetch_size
            rows = TestMultiKeyModel.objects().allow_filtering()
            self.assertEqual(fetch_size * 10, len(rows))

        for connection_name in self.sane_connections:
            connection.set_default_connection(connection_name)
            for fetch_size in (2, 3, 7, 10, 99, 100, 101, 150):
                connection._connections[connection_name].session.default_fetch_size = fetch_size
                rows = TestMultiKeyModel.objects().allow_filtering()
                self.assertEqual(1000, len(rows))


@requiredse
@greaterthanorequaldse51
class ContPagingTestsDSEV1(BasicConcurrentTests, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        BasicConcurrentTests.required_dse_version = Version('5.1')
        if not DSE_VERSION or DSE_VERSION < BasicConcurrentTests.required_dse_version:
            return

        BasicConcurrentTests.protocol_version = ProtocolVersion.DSE_V1
        BasicConcurrentTests.setUpClass()

@requiredse
@greaterthanorequaldse60
class ContPagingTestsDSEV2(BasicConcurrentTests, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        BasicConcurrentTests.required_dse_version = Version('6.0')
        if not DSE_VERSION or DSE_VERSION < BasicConcurrentTests.required_dse_version:
            return
        BasicConcurrentTests.protocol_version = ProtocolVersion.DSE_V2
        BasicConcurrentTests.setUpClass()

        cls.connections = cls.connections.union({"SMALL_QUEUE", "BIG_QUEUE"})
        cls.sane_connections = cls.sane_connections.union({"SMALL_QUEUE", "BIG_QUEUE"})

        cls._create_cluster_with_cp_options("SMALL_QUEUE", ContinuousPagingOptions(max_queue_size=2))
        cls._create_cluster_with_cp_options("BIG_QUEUE", ContinuousPagingOptions(max_queue_size=400))
