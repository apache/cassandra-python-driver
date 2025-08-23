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

from collections import namedtuple, OrderedDict

from cassandra import ProtocolVersion
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT
from cassandra.query import (named_tuple_factory, tuple_factory,
                             dict_factory, ordered_dict_factory)

from cassandra.cqlengine import columns
from cassandra.cqlengine.connection import set_session
from cassandra.cqlengine.models import Model

from tests.integration import requiressimulacron
from tests.integration.simulacron import PROTOCOL_VERSION, SimulacronCluster
from tests.integration.simulacron.utils import PrimeQuery, prime_request


PROTOCOL_VERSION = 4 if PROTOCOL_VERSION in \
    (ProtocolVersion.DSE_V1, ProtocolVersion.DSE_V2) else PROTOCOL_VERSION


@requiressimulacron
class EmptyColumnTests(SimulacronCluster):
    """
    Test that legacy empty column names can be read by the driver.

    @since 3.18
    @jira_ticket PYTHON-1082
    @expected_result the driver supports those columns
    """
    connect = False

    def tearDown(self):
        if self.cluster:
            self.cluster.shutdown()

    @staticmethod
    def _prime_testtable_query():
        queries = [
            'SELECT "", " " FROM testks.testtable',
            'SELECT "", " " FROM testks.testtable LIMIT 10000'  # cqlengine
        ]
        then = {
            'result': 'success',
            'delay_in_ms': 0,
            'rows': [
                {
                    "": "testval",
                    " ": "testval1"
                }
            ],
            'column_types': {
                "": "ascii",
                " ": "ascii"
            },
            'ignore_on_prepare': False
        }
        for query in queries:
            prime_request(PrimeQuery(query, then=then))

    def test_empty_columns_with_all_row_factories(self):
        query = 'SELECT "", " " FROM testks.testtable'
        self._prime_testtable_query()

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False)
        self.session = self.cluster.connect(wait_for_all_pools=True)

        # Test all row factories
        self.cluster.profile_manager.profiles[EXEC_PROFILE_DEFAULT].row_factory = named_tuple_factory
        self.assertEqual(
            list(self.session.execute(query)),
            [namedtuple('Row', ['field_0_', 'field_1_'])('testval', 'testval1')]
        )

        self.cluster.profile_manager.profiles[EXEC_PROFILE_DEFAULT].row_factory = tuple_factory
        self.assertEqual(
            list(self.session.execute(query)),
            [('testval', 'testval1')]
        )

        self.cluster.profile_manager.profiles[EXEC_PROFILE_DEFAULT].row_factory = dict_factory
        self.assertEqual(
            list(self.session.execute(query)),
            [{'': 'testval', ' ': 'testval1'}]
        )

        self.cluster.profile_manager.profiles[EXEC_PROFILE_DEFAULT].row_factory = ordered_dict_factory
        self.assertEqual(
            list(self.session.execute(query)),
            [OrderedDict((('', 'testval'), (' ', 'testval1')))]
        )

    def test_empty_columns_in_system_schema(self):
        queries = [
            "SELECT * FROM system_schema.tables",
            "SELECT * FROM system.schema.tables",
            "SELECT * FROM system.schema_columnfamilies"
        ]
        then = {
            'result': 'success',
            'delay_in_ms': 0,
            'rows': [
                {
                    "compression": dict(),
                    "compaction": dict(),
                    "bloom_filter_fp_chance": 0.1,
                    "caching": {"keys": "ALL", "rows_per_partition": "NONE"},
                    "comment": "comment",
                    "gc_grace_seconds": 60000,
                    "keyspace_name": "testks",
                    "table_name": "testtable",
                    "columnfamily_name": "testtable",  # C* 2.2
                    "flags": ["compound"],
                    "comparator": "none"  # C* 2.2
                }
            ],
            'column_types': {
                "compression": "map<ascii, ascii>",
                "compaction": "map<ascii, ascii>",
                "bloom_filter_fp_chance": "double",
                "caching": "map<ascii, ascii>",
                "comment": "ascii",
                "gc_grace_seconds": "int",
                "keyspace_name": "ascii",
                "table_name": "ascii",
                "columnfamily_name": "ascii",
                "flags": "set<ascii>",
                "comparator": "ascii"
            },
            'ignore_on_prepare': False
        }
        for query in queries:
            query = PrimeQuery(query, then=then)
            prime_request(query)

        queries = [
            "SELECT * FROM system_schema.keyspaces",
            "SELECT * FROM system.schema_keyspaces"
        ]
        then = {
            'result': 'success',
            'delay_in_ms': 0,
            'rows': [
                {
                    "strategy_class": "SimpleStrategy",  # C* 2.2
                    "strategy_options": '{}',  # C* 2.2
                    "replication": {'strategy': 'SimpleStrategy', 'replication_factor': 1},
                    "durable_writes": True,
                    "keyspace_name": "testks"
                }
            ],
            'column_types': {
                "strategy_class": "ascii",
                "strategy_options": "ascii",
                "replication": "map<ascii, ascii>",
                "keyspace_name": "ascii",
                "durable_writes": "boolean"
            },
            'ignore_on_prepare': False
        }
        for query in queries:
            query = PrimeQuery(query, then=then)
            prime_request(query)

        queries = [
            "SELECT * FROM system_schema.columns",
            "SELECT * FROM system.schema.columns",
            "SELECT * FROM system.schema_columns"
        ]
        then = {
            'result': 'success',
            'delay_in_ms': 0,
            'rows': [
                {
                    "table_name": 'testtable',
                    "columnfamily_name": 'testtable',  # C* 2.2
                    "column_name": "",
                    "keyspace_name": "testks",
                    "kind": "partition_key",
                    "clustering_order": "none",
                    "position": 0,
                    "type": "text",
                    "column_name_bytes": 0x12,
                    "validator": "none"  # C* 2.2
                },
                {
                    "table_name": 'testtable',
                    "columnfamily_name": 'testtable',  # C* 2.2
                    "column_name": " ",
                    "keyspace_name": "testks",
                    "kind": "regular",
                    "clustering_order": "none",
                    "position": -1,
                    "type": "text",
                    "column_name_bytes": 0x13,
                    "validator": "none"  # C* 2.2
                }
            ],
            'column_types': {
                "table_name": "ascii",
                "columnfamily_name": "ascii",
                "column_name": "ascii",
                "keyspace_name": "ascii",
                "clustering_order": "ascii",
                "column_name_bytes": "blob",
                "kind": "ascii",
                "position": "int",
                "type": "ascii",
                "validator": "ascii"  # C* 2.2
            },
            'ignore_on_prepare': False
        }
        for query in queries:
            query = PrimeQuery(query, then=then)
            prime_request(query)

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False)
        self.session = self.cluster.connect(wait_for_all_pools=True)

        table_metadata = self.cluster.metadata.keyspaces['testks'].tables['testtable']
        self.assertEqual(len(table_metadata.columns), 2)
        self.assertIn('', table_metadata.columns)
        self.assertIn(' ', table_metadata.columns)

    def test_empty_columns_with_cqlengine(self):
        self._prime_testtable_query()

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False)
        self.session = self.cluster.connect(wait_for_all_pools=True)
        set_session(self.session)

        class TestModel(Model):
            __keyspace__ = 'testks'
            __table_name__ = 'testtable'
            empty = columns.Text(db_field='', primary_key=True)
            space = columns.Text(db_field=' ')

        self.assertEqual(
            [TestModel(empty='testval', space='testval1')],
            list(TestModel.objects.only(['empty', 'space']).all())
        )
