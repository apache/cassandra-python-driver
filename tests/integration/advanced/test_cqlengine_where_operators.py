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

import os
import time

from cassandra.cqlengine import columns, connection, models
from cassandra.cqlengine.management import (CQLENG_ALLOW_SCHEMA_MANAGEMENT,
                                      create_keyspace_simple, drop_table,
                                      sync_table)
from cassandra.cqlengine.statements import IsNotNull
from tests.integration import DSE_VERSION, requiredse, CASSANDRA_IP, greaterthanorequaldse60, TestCluster
from tests.integration.advanced import use_single_node_with_graph_and_solr
from tests.integration.cqlengine import DEFAULT_KEYSPACE


class SimpleNullableModel(models.Model):
    __keyspace__ = DEFAULT_KEYSPACE
    partition = columns.Integer(primary_key=True)
    nullable = columns.Integer(required=False)
    # nullable = columns.Integer(required=False, custom_index=True)


def setup_module():
    if DSE_VERSION:
        os.environ[CQLENG_ALLOW_SCHEMA_MANAGEMENT] = '1'
        use_single_node_with_graph_and_solr()
        setup_connection(DEFAULT_KEYSPACE)
        create_keyspace_simple(DEFAULT_KEYSPACE, 1)
        sync_table(SimpleNullableModel)


def setup_connection(keyspace_name):
    connection.setup([CASSANDRA_IP],
                     # consistency=ConsistencyLevel.ONE,
                     # protocol_version=PROTOCOL_VERSION,
                     default_keyspace=keyspace_name)


def teardown_module():
    if DSE_VERSION:
        drop_table(SimpleNullableModel)


@requiredse
class IsNotNullTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if DSE_VERSION:
            cls.cluster = TestCluster()

    @greaterthanorequaldse60
    def test_is_not_null_execution(self):
        """
        Verify that CQL statements have correct syntax when executed
        If we wanted them to return something meaningful and not a InvalidRequest
        we'd have to create an index in search for the column we are using
        IsNotNull

        @since 3.20
        @jira_ticket PYTHON-968
        @expected_result InvalidRequest is arisen

        @test_category cqlengine
        """
        cluster = TestCluster()
        self.addCleanup(cluster.shutdown)
        session = cluster.connect()

        SimpleNullableModel.create(partition=1, nullable=2)
        SimpleNullableModel.create(partition=2, nullable=None)

        self.addCleanup(session.execute, "DROP SEARCH INDEX ON {}".format(
            SimpleNullableModel.column_family_name()))
        create_index_stmt = (
            "CREATE SEARCH INDEX ON {} WITH COLUMNS nullable "
            "".format(SimpleNullableModel.column_family_name()))
        session.execute(create_index_stmt)

        SimpleNullableModel.create(partition=1, nullable=1)
        SimpleNullableModel.create(partition=2, nullable=None)

        # TODO: block on indexing more precisely
        time.sleep(5)

        self.assertEqual(len(list(SimpleNullableModel.objects.all())), 2)
        self.assertEqual(
            len(list(
                SimpleNullableModel.filter(IsNotNull("nullable"), partition__eq=2)
            )),
            0)
        self.assertEqual(
            len(list(
                SimpleNullableModel.filter(IsNotNull("nullable"), partition__eq=1)
            )),
            1)
