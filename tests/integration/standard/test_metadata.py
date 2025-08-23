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

from collections import defaultdict
import difflib
import logging
import sys
import time
import os
from packaging.version import Version
from unittest.mock import Mock, patch

from cassandra import AlreadyExists, SignatureDescriptor, UserFunctionDescriptor, UserAggregateDescriptor

from cassandra.encoder import Encoder
from cassandra.metadata import (IndexMetadata, Token, murmur3, Function, Aggregate, protect_name, protect_names,
                                RegisteredTableExtension, _RegisteredExtensionType, get_schema_parser,
                                group_keys_by_replica, NO_VALID_REPLICA)
from cassandra.util import SortedSet

from tests.integration import (get_cluster, use_singledc, PROTOCOL_VERSION, execute_until_pass,
                               BasicSegregatedKeyspaceUnitTestCase, BasicSharedKeyspaceUnitTestCase,
                               BasicExistingKeyspaceUnitTestCase, drop_keyspace_shutdown_cluster, CASSANDRA_VERSION,
                               greaterthanorequaldse51, greaterthanorequalcass30, lessthancass30, local,
                               get_supported_protocol_versions, greaterthancass20,
                               greaterthancass21, assert_startswith, greaterthanorequalcass40,
                               greaterthanorequaldse67, lessthancass40,
                               TestCluster, DSE_VERSION, HCD_VERSION)


log = logging.getLogger(__name__)


def setup_module():
    use_singledc()


class HostMetaDataTests(BasicExistingKeyspaceUnitTestCase):
    @local
    def test_host_addresses(self):
        """
        Check to ensure that the broadcast_address, broadcast_rpc_address,
        listen adresss, ports and host are is populated correctly.

        @since 3.3
        @jira_ticket PYTHON-332
        @expected_result They are populated for C*> 2.1.6, 2.2.0

        @test_category metadata
        """
        # All nodes should have the broadcast_address, rpc_address and host_id set
        for host in self.cluster.metadata.all_hosts():
            self.assertIsNotNone(host.broadcast_address)
            self.assertIsNotNone(host.broadcast_rpc_address)
            self.assertIsNotNone(host.host_id)

            if not DSE_VERSION and CASSANDRA_VERSION >= Version('4-a'):
                self.assertIsNotNone(host.broadcast_port)
                self.assertIsNotNone(host.broadcast_rpc_port)

        con = self.cluster.control_connection.get_connections()[0]
        local_host = con.host

        # The control connection node should have the listen address set.
        listen_addrs = [host.listen_address for host in self.cluster.metadata.all_hosts()]
        self.assertTrue(local_host in listen_addrs)

        # The control connection node should have the broadcast_rpc_address set.
        rpc_addrs = [host.broadcast_rpc_address for host in self.cluster.metadata.all_hosts()]
        self.assertTrue(local_host in rpc_addrs)

    @unittest.skipUnless(
        os.getenv('MAPPED_CASSANDRA_VERSION', None) is not None,
        "Don't check the host version for test-dse")
    def test_host_release_version(self):
        """
        Checks the hosts release version and validates that it is equal to the
        Cassandra version we are using in our test harness.

        @since 3.3
        @jira_ticket PYTHON-301
        @expected_result host.release version should match our specified Cassandra version.

        @test_category metadata
        """
        for host in self.cluster.metadata.all_hosts():
            assert_startswith(host.release_version, CASSANDRA_VERSION.base_version)



@local
class MetaDataRemovalTest(unittest.TestCase):

    def setUp(self):
        self.cluster = TestCluster(contact_points=['127.0.0.1', '127.0.0.2', '127.0.0.3', '126.0.0.186'])
        self.cluster.connect()

    def tearDown(self):
        self.cluster.shutdown()

    def test_bad_contact_point(self):
        """
        Checks to ensure that hosts that are not resolvable are excluded from the contact point list.

        @since 3.6
        @jira_ticket PYTHON-549
        @expected_result Invalid hosts on the contact list should be excluded

        @test_category metadata
        """
        self.assertEqual(len(self.cluster.metadata.all_hosts()), 3)


class SchemaMetadataTests(BasicSegregatedKeyspaceUnitTestCase):

    def test_schema_metadata_disable(self):
        """
        Checks to ensure that schema metadata_enabled, and token_metadata_enabled
        flags work correctly.

        @since 3.3
        @jira_ticket PYTHON-327
        @expected_result schema metadata will not be populated when schema_metadata_enabled is fause
        token_metadata will be missing when token_metadata is set to false

        @test_category metadata
        """
        # Validate metadata is missing where appropriate
        no_schema = TestCluster(schema_metadata_enabled=False)
        no_schema_session = no_schema.connect()
        self.assertEqual(len(no_schema.metadata.keyspaces), 0)
        self.assertEqual(no_schema.metadata.export_schema_as_string(), '')
        no_token = TestCluster(token_metadata_enabled=False)
        no_token_session = no_token.connect()
        self.assertEqual(len(no_token.metadata.token_map.token_to_host_owner), 0)

        # Do a simple query to ensure queries are working
        query = "SELECT * FROM system.local"
        no_schema_rs = no_schema_session.execute(query)
        no_token_rs = no_token_session.execute(query)
        self.assertIsNotNone(no_schema_rs[0])
        self.assertIsNotNone(no_token_rs[0])
        no_schema.shutdown()
        no_token.shutdown()

    def make_create_statement(self, partition_cols, clustering_cols=None, other_cols=None):
        clustering_cols = clustering_cols or []
        other_cols = other_cols or []

        statement = "CREATE TABLE %s.%s (" % (self.keyspace_name, self.function_table_name)
        if len(partition_cols) == 1 and not clustering_cols:
            statement += "%s text PRIMARY KEY, " % protect_name(partition_cols[0])
        else:
            statement += ", ".join("%s text" % protect_name(col) for col in partition_cols)
            statement += ", "

        statement += ", ".join("%s text" % protect_name(col) for col in clustering_cols + other_cols)

        if len(partition_cols) != 1 or clustering_cols:
            statement += ", PRIMARY KEY ("

            if len(partition_cols) > 1:
                statement += "(" + ", ".join(protect_names(partition_cols)) + ")"
            else:
                statement += protect_name(partition_cols[0])

            if clustering_cols:
                statement += ", "
                statement += ", ".join(protect_names(clustering_cols))

            statement += ")"

        statement += ")"

        return statement

    def check_create_statement(self, tablemeta, original):
        recreate = tablemeta.as_cql_query(formatted=False)
        self.assertEqual(original, recreate[:len(original)])
        execute_until_pass(self.session, "DROP TABLE {0}.{1}".format(self.keyspace_name, self.function_table_name))
        execute_until_pass(self.session, recreate)

        # create the table again, but with formatting enabled
        execute_until_pass(self.session, "DROP TABLE {0}.{1}".format(self.keyspace_name, self.function_table_name))
        recreate = tablemeta.as_cql_query(formatted=True)
        execute_until_pass(self.session, recreate)

    def get_table_metadata(self):
        self.cluster.refresh_table_metadata(self.keyspace_name, self.function_table_name)
        return self.cluster.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name]

    def test_basic_table_meta_properties(self):
        create_statement = self.make_create_statement(["a"], [], ["b", "c"])
        self.session.execute(create_statement)

        self.cluster.refresh_schema_metadata()

        meta = self.cluster.metadata
        self.assertNotEqual(meta.cluster_name, None)
        self.assertTrue(self.keyspace_name in meta.keyspaces)
        ksmeta = meta.keyspaces[self.keyspace_name]

        self.assertEqual(ksmeta.name, self.keyspace_name)
        self.assertTrue(ksmeta.durable_writes)
        self.assertEqual(ksmeta.replication_strategy.name, 'SimpleStrategy')
        self.assertEqual(ksmeta.replication_strategy.replication_factor, 1)

        self.assertTrue(self.function_table_name in ksmeta.tables)
        tablemeta = ksmeta.tables[self.function_table_name]
        self.assertEqual(tablemeta.keyspace_name, ksmeta.name)
        self.assertEqual(tablemeta.name, self.function_table_name)
        self.assertEqual(tablemeta.name, self.function_table_name)

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([], tablemeta.clustering_key)
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        cc = self.cluster.control_connection._connection
        parser = get_schema_parser(
            cc,
            self.cluster.metadata.get_host(cc.host).release_version,
            self.cluster.metadata.get_host(cc.host).dse_version,
            1
        )

        for option in tablemeta.options:
            self.assertIn(option, parser.recognized_table_options)

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys(self):
        create_statement = self.make_create_statement(["a"], ["b"], ["c"])
        create_statement += " WITH CLUSTERING ORDER BY (b ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_protected(self):
        create_statement = self.make_create_statement(["Aa"], ["Bb"], ["Cc"])
        create_statement += ' WITH CLUSTERING ORDER BY ("Bb" ASC)'
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'Aa'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'Bb'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'Aa', u'Bb', u'Cc'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_more_columns(self):
        create_statement = self.make_create_statement(["a"], ["b", "c"], ["d", "e", "f"])
        create_statement += " WITH CLUSTERING ORDER BY (b ASC, c ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b', u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual(
            [u'a', u'b', u'c', u'd', u'e', u'f'],
            sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_primary_key(self):
        create_statement = self.make_create_statement(["a", "b"], [], ["c"])
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([], tablemeta.clustering_key)
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_in_compound_primary_key(self):
        create_statement = self.make_create_statement(["a", "b"], ["c"], ["d", "e"])
        create_statement += " WITH CLUSTERING ORDER BY (c ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c', u'd', u'e'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_compact(self):
        create_statement = self.make_create_statement(["a"], ["b"], ["c"])
        create_statement += " WITH CLUSTERING ORDER BY (b ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_cluster_column_ordering_reversed_metadata(self):
        """
        Simple test to ensure that the metatdata associated with cluster ordering is surfaced is surfaced correctly.

        Creates a table with a few clustering keys. Then checks the clustering order associated with clustering columns
        and ensure it's set correctly.
        @since 3.0.0
        @jira_ticket PYTHON-402
        @expected_result is_reversed is set on DESC order, and is False on ASC

        @test_category metadata
        """

        create_statement = self.make_create_statement(["a"], ["b", "c"], ["d"])
        create_statement += " WITH CLUSTERING ORDER BY (b ASC, c DESC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        b_column = tablemeta.columns['b']
        self.assertFalse(b_column.is_reversed)
        c_column = tablemeta.columns['c']
        self.assertTrue(c_column.is_reversed)

    def test_compound_primary_keys_more_columns_compact(self):
        create_statement = self.make_create_statement(["a"], ["b", "c"], ["d"])
        create_statement += " WITH CLUSTERING ORDER BY (b ASC, c ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'b', u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c', u'd'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_primary_key_compact(self):
        create_statement = self.make_create_statement(["a", "b"], [], ["c"])
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([], tablemeta.clustering_key)
        self.assertEqual([u'a', u'b', u'c'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    def test_composite_in_compound_primary_key_compact(self):
        create_statement = self.make_create_statement(["a", "b"], ["c"], ["d"])
        create_statement += " WITH CLUSTERING ORDER BY (c ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a', u'b'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([u'c'], [c.name for c in tablemeta.clustering_key])
        self.assertEqual([u'a', u'b', u'c', u'd'], sorted(tablemeta.columns.keys()))

        self.check_create_statement(tablemeta, create_statement)

    @lessthancass30
    def test_cql_compatibility(self):

        # having more than one non-PK column is okay if there aren't any
        # clustering columns
        create_statement = self.make_create_statement(["a"], [], ["b", "c", "d"])
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()

        self.assertEqual([u'a'], [c.name for c in tablemeta.partition_key])
        self.assertEqual([], tablemeta.clustering_key)
        self.assertEqual([u'a', u'b', u'c', u'd'], sorted(tablemeta.columns.keys()))

        self.assertTrue(tablemeta.is_cql_compatible)

        # It will be cql compatible after CASSANDRA-10857
        # since compact storage is being dropped
        tablemeta.clustering_key = ["foo", "bar"]
        tablemeta.columns["foo"] = None
        tablemeta.columns["bar"] = None
        self.assertTrue(tablemeta.is_cql_compatible)

    def test_compound_primary_keys_ordering(self):
        create_statement = self.make_create_statement(["a"], ["b"], ["c"])
        create_statement += " WITH CLUSTERING ORDER BY (b DESC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        self.check_create_statement(tablemeta, create_statement)

    def test_compound_primary_keys_more_columns_ordering(self):
        create_statement = self.make_create_statement(["a"], ["b", "c"], ["d", "e", "f"])
        create_statement += " WITH CLUSTERING ORDER BY (b DESC, c ASC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        self.check_create_statement(tablemeta, create_statement)

    def test_composite_in_compound_primary_key_ordering(self):
        create_statement = self.make_create_statement(["a", "b"], ["c"], ["d", "e"])
        create_statement += " WITH CLUSTERING ORDER BY (c DESC)"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        self.check_create_statement(tablemeta, create_statement)

    @lessthancass40
    def test_compact_storage(self):
        create_statement = self.make_create_statement(["a"], [], ["b"])
        create_statement += " WITH COMPACT STORAGE"

        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        self.check_create_statement(tablemeta, create_statement)

    @lessthancass40
    def test_dense_compact_storage(self):
        create_statement = self.make_create_statement(["a"], ["b"], ["c"])
        create_statement += " WITH COMPACT STORAGE"

        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        self.check_create_statement(tablemeta, create_statement)

    def test_counter(self):
        create_statement = (
            "CREATE TABLE {keyspace}.{table} ("
            "key text PRIMARY KEY, a1 counter)"
        ).format(keyspace=self.keyspace_name, table=self.function_table_name)

        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        self.check_create_statement(tablemeta, create_statement)

    @lessthancass40
    def test_counter_with_compact_storage(self):
        """ PYTHON-1100 """
        create_statement = (
            "CREATE TABLE {keyspace}.{table} ("
            "key text PRIMARY KEY, a1 counter) WITH COMPACT STORAGE"
        ).format(keyspace=self.keyspace_name, table=self.function_table_name)

        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        self.check_create_statement(tablemeta, create_statement)

    @lessthancass40
    def test_counter_with_dense_compact_storage(self):
        create_statement = (
            "CREATE TABLE {keyspace}.{table} ("
            "key text, c1 text, a1 counter, PRIMARY KEY (key, c1)) WITH COMPACT STORAGE"
        ).format(keyspace=self.keyspace_name, table=self.function_table_name)

        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        self.check_create_statement(tablemeta, create_statement)

    def test_indexes(self):
        create_statement = self.make_create_statement(["a"], ["b", "c"], ["d", "e", "f"])
        create_statement += " WITH CLUSTERING ORDER BY (b ASC, c ASC)"
        execute_until_pass(self.session, create_statement)

        d_index = "CREATE INDEX d_index ON %s.%s (d)" % (self.keyspace_name, self.function_table_name)
        e_index = "CREATE INDEX e_index ON %s.%s (e)" % (self.keyspace_name, self.function_table_name)
        execute_until_pass(self.session, d_index)
        execute_until_pass(self.session, e_index)

        tablemeta = self.get_table_metadata()
        statements = tablemeta.export_as_string().strip()
        statements = [s.strip() for s in statements.split(';')]
        statements = list(filter(bool, statements))
        self.assertEqual(3, len(statements))
        self.assertIn(d_index, statements)
        self.assertIn(e_index, statements)

        # make sure indexes are included in KeyspaceMetadata.export_as_string()
        ksmeta = self.cluster.metadata.keyspaces[self.keyspace_name]
        statement = ksmeta.export_as_string()
        self.assertIn('CREATE INDEX d_index', statement)
        self.assertIn('CREATE INDEX e_index', statement)

    @greaterthancass21
    def test_collection_indexes(self):

        self.session.execute("CREATE TABLE %s.%s (a int PRIMARY KEY, b map<text, text>)"
                             % (self.keyspace_name, self.function_table_name))
        self.session.execute("CREATE INDEX index1 ON %s.%s (keys(b))"
                             % (self.keyspace_name, self.function_table_name))

        tablemeta = self.get_table_metadata()
        self.assertIn('(keys(b))', tablemeta.export_as_string())

        self.session.execute("DROP INDEX %s.index1" % (self.keyspace_name,))
        self.session.execute("CREATE INDEX index2 ON %s.%s (b)"
                             % (self.keyspace_name, self.function_table_name))

        tablemeta = self.get_table_metadata()
        target = ' (b)' if CASSANDRA_VERSION < Version("3.0") else 'values(b))'  # explicit values in C* 3+
        self.assertIn(target, tablemeta.export_as_string())

        # test full indexes on frozen collections, if available
        if CASSANDRA_VERSION >= Version("2.1.3"):
            self.session.execute("DROP TABLE %s.%s" % (self.keyspace_name, self.function_table_name))
            self.session.execute("CREATE TABLE %s.%s (a int PRIMARY KEY, b frozen<map<text, text>>)"
                                 % (self.keyspace_name, self.function_table_name))
            self.session.execute("CREATE INDEX index3 ON %s.%s (full(b))"
                                 % (self.keyspace_name, self.function_table_name))

            tablemeta = self.get_table_metadata()
            self.assertIn('(full(b))', tablemeta.export_as_string())

    def test_compression_disabled(self):
        create_statement = self.make_create_statement(["a"], ["b"], ["c"])
        create_statement += " WITH compression = {}"
        self.session.execute(create_statement)
        tablemeta = self.get_table_metadata()
        expected = "compression = {}" if CASSANDRA_VERSION < Version("3.0") else "compression = {'enabled': 'false'}"
        self.assertIn(expected, tablemeta.export_as_string())

    def test_non_size_tiered_compaction(self):
        """
        test options for non-size-tiered compaction strategy

        Creates a table with LeveledCompactionStrategy, specifying one non-default option. Verifies that the option is
        present in generated CQL, and that other legacy table parameters (min_threshold, max_threshold) are not included.

        @since 2.6.0
        @jira_ticket PYTHON-352
        @expected_result the options map for LeveledCompactionStrategy does not contain min_threshold, max_threshold

        @test_category metadata
        """
        create_statement = self.make_create_statement(["a"], [], ["b", "c"])
        create_statement += "WITH COMPACTION = {'class': 'LeveledCompactionStrategy', 'tombstone_threshold': '0.3'}"
        self.session.execute(create_statement)

        table_meta = self.get_table_metadata()
        cql = table_meta.export_as_string()
        self.assertIn("'tombstone_threshold': '0.3'", cql)
        self.assertIn("LeveledCompactionStrategy", cql)
        # formerly legacy options; reintroduced in 4.0
        if CASSANDRA_VERSION < Version('4.0-a'):
            self.assertNotIn("min_threshold", cql)
            self.assertNotIn("max_threshold", cql)

    def test_refresh_schema_metadata(self):
        """
        test for synchronously refreshing all cluster metadata

        test_refresh_schema_metadata tests all cluster metadata is refreshed when calling refresh_schema_metadata().
        It creates a second cluster object with schema_event_refresh_window=-1 such that schema refreshes are disabled
        for schema change push events. It then alters the cluster, creating a new keyspace, using the first cluster
        object, and verifies that the cluster metadata has not changed in the second cluster object. It then calls
        refresh_schema_metadata() and verifies that the cluster metadata is updated in the second cluster object.
        Similarly, it then proceeds to altering keyspace, table, UDT, UDF, and UDA metadata and subsequently verfies
        that these metadata is updated when refresh_schema_metadata() is called.

        @since 2.6.0
        @jira_ticket PYTHON-291
        @expected_result Cluster, keyspace, table, UDT, UDF, and UDA metadata should be refreshed when refresh_schema_metadata() is called.

        @test_category metadata
        """
        cluster2 = TestCluster(schema_event_refresh_window=-1)
        cluster2.connect()

        self.assertNotIn("new_keyspace", cluster2.metadata.keyspaces)

        # Cluster metadata modification
        self.session.execute("CREATE KEYSPACE new_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
        self.assertNotIn("new_keyspace", cluster2.metadata.keyspaces)

        cluster2.refresh_schema_metadata()
        self.assertIn("new_keyspace", cluster2.metadata.keyspaces)

        # Keyspace metadata modification
        self.session.execute("ALTER KEYSPACE {0} WITH durable_writes = false".format(self.keyspace_name))
        self.assertTrue(cluster2.metadata.keyspaces[self.keyspace_name].durable_writes)
        cluster2.refresh_schema_metadata()
        self.assertFalse(cluster2.metadata.keyspaces[self.keyspace_name].durable_writes)

        # Table metadata modification
        table_name = "test"
        self.session.execute("CREATE TABLE {0}.{1} (a int PRIMARY KEY, b text)".format(self.keyspace_name, table_name))
        cluster2.refresh_schema_metadata()

        self.session.execute("ALTER TABLE {0}.{1} ADD c double".format(self.keyspace_name, table_name))
        self.assertNotIn("c", cluster2.metadata.keyspaces[self.keyspace_name].tables[table_name].columns)
        cluster2.refresh_schema_metadata()
        self.assertIn("c", cluster2.metadata.keyspaces[self.keyspace_name].tables[table_name].columns)

        if PROTOCOL_VERSION >= 3:
            # UDT metadata modification
            self.session.execute("CREATE TYPE {0}.user (age int, name text)".format(self.keyspace_name))
            self.assertEqual(cluster2.metadata.keyspaces[self.keyspace_name].user_types, {})
            cluster2.refresh_schema_metadata()
            self.assertIn("user", cluster2.metadata.keyspaces[self.keyspace_name].user_types)

        if PROTOCOL_VERSION >= 4:
            # UDF metadata modification
            self.session.execute("""CREATE FUNCTION {0}.sum_int(key int, val int)
                                RETURNS NULL ON NULL INPUT
                                RETURNS int
                                LANGUAGE java AS 'return key+val;';""".format(self.keyspace_name))

            self.assertEqual(cluster2.metadata.keyspaces[self.keyspace_name].functions, {})
            cluster2.refresh_schema_metadata()
            self.assertIn("sum_int(int,int)", cluster2.metadata.keyspaces[self.keyspace_name].functions)

            # UDA metadata modification
            self.session.execute("""CREATE AGGREGATE {0}.sum_agg(int)
                                 SFUNC sum_int
                                 STYPE int
                                 INITCOND 0"""
                                 .format(self.keyspace_name))

            self.assertEqual(cluster2.metadata.keyspaces[self.keyspace_name].aggregates, {})
            cluster2.refresh_schema_metadata()
            self.assertIn("sum_agg(int)", cluster2.metadata.keyspaces[self.keyspace_name].aggregates)

        # Cluster metadata modification
        self.session.execute("DROP KEYSPACE new_keyspace")
        self.assertIn("new_keyspace", cluster2.metadata.keyspaces)

        cluster2.refresh_schema_metadata()
        self.assertNotIn("new_keyspace", cluster2.metadata.keyspaces)

        cluster2.shutdown()

    def test_refresh_keyspace_metadata(self):
        """
        test for synchronously refreshing keyspace metadata

        test_refresh_keyspace_metadata tests that keyspace metadata is refreshed when calling refresh_keyspace_metadata().
        It creates a second cluster object with schema_event_refresh_window=-1 such that schema refreshes are disabled
        for schema change push events. It then alters the keyspace, disabling durable_writes, using the first cluster
        object, and verifies that the keyspace metadata has not changed in the second cluster object. Finally, it calls
        refresh_keyspace_metadata() and verifies that the keyspace metadata is updated in the second cluster object.

        @since 2.6.0
        @jira_ticket PYTHON-291
        @expected_result Keyspace metadata should be refreshed when refresh_keyspace_metadata() is called.

        @test_category metadata
        """

        cluster2 = TestCluster(schema_event_refresh_window=-1)
        cluster2.connect()

        self.assertTrue(cluster2.metadata.keyspaces[self.keyspace_name].durable_writes)
        self.session.execute("ALTER KEYSPACE {0} WITH durable_writes = false".format(self.keyspace_name))
        self.assertTrue(cluster2.metadata.keyspaces[self.keyspace_name].durable_writes)
        cluster2.refresh_keyspace_metadata(self.keyspace_name)
        self.assertFalse(cluster2.metadata.keyspaces[self.keyspace_name].durable_writes)

        cluster2.shutdown()

    def test_refresh_table_metadata(self):
        """
        test for synchronously refreshing table metadata

        test_refresh_table_metadata tests that table metadata is refreshed when calling test_refresh_table_metadata().
        It creates a second cluster object with schema_event_refresh_window=-1 such that schema refreshes are disabled
        for schema change push events. It then alters the table, adding a new column, using the first cluster
        object, and verifies that the table metadata has not changed in the second cluster object. Finally, it calls
        test_refresh_table_metadata() and verifies that the table metadata is updated in the second cluster object.

        @since 2.6.0
        @jira_ticket PYTHON-291
        @expected_result Table metadata should be refreshed when refresh_table_metadata() is called.

        @test_category metadata
        """

        table_name = "test"
        self.session.execute("CREATE TABLE {0}.{1} (a int PRIMARY KEY, b text)".format(self.keyspace_name, table_name))

        cluster2 = TestCluster(schema_event_refresh_window=-1)
        cluster2.connect()

        self.assertNotIn("c", cluster2.metadata.keyspaces[self.keyspace_name].tables[table_name].columns)
        self.session.execute("ALTER TABLE {0}.{1} ADD c double".format(self.keyspace_name, table_name))
        self.assertNotIn("c", cluster2.metadata.keyspaces[self.keyspace_name].tables[table_name].columns)

        cluster2.refresh_table_metadata(self.keyspace_name, table_name)
        self.assertIn("c", cluster2.metadata.keyspaces[self.keyspace_name].tables[table_name].columns)

        cluster2.shutdown()

    @greaterthanorequalcass30
    def test_refresh_metadata_for_mv(self):
        """
        test for synchronously refreshing materialized view metadata

        test_refresh_table_metadata_for_materialized_views tests that materialized view metadata is refreshed when calling
        test_refresh_table_metadata() with the materialized view name as the table. It creates a second cluster object
        with schema_event_refresh_window=-1 such that schema refreshes are disabled for schema change push events.
        It then creates a new materialized view , using the first cluster object, and verifies that the materialized view
        metadata has not changed in the second cluster object. Finally, it calls test_refresh_table_metadata() with the
        materialized view name as the table name, and verifies that the materialized view metadata is updated in the
        second cluster object.

        @since 3.0.0
        @jira_ticket PYTHON-371
        @expected_result Materialized view metadata should be refreshed when refresh_table_metadata() is called.

        @test_category metadata
        """

        self.session.execute("CREATE TABLE {0}.{1} (a int PRIMARY KEY, b text)".format(self.keyspace_name, self.function_table_name))

        cluster2 = TestCluster(schema_event_refresh_window=-1)
        cluster2.connect()

        try:
            self.assertNotIn("mv1", cluster2.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].views)
            self.session.execute("CREATE MATERIALIZED VIEW {0}.mv1 AS SELECT a, b FROM {0}.{1} "
                                 "WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a, b)"
                                 .format(self.keyspace_name, self.function_table_name))
            self.assertNotIn("mv1", cluster2.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].views)

            cluster2.refresh_table_metadata(self.keyspace_name, "mv1")
            self.assertIn("mv1", cluster2.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].views)
        finally:
            cluster2.shutdown()

        original_meta = self.cluster.metadata.keyspaces[self.keyspace_name].views['mv1']
        self.assertIs(original_meta, self.session.cluster.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].views['mv1'])
        self.cluster.refresh_materialized_view_metadata(self.keyspace_name, 'mv1')

        current_meta = self.cluster.metadata.keyspaces[self.keyspace_name].views['mv1']
        self.assertIsNot(current_meta, original_meta)
        self.assertIsNot(original_meta, self.session.cluster.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].views['mv1'])
        self.assertEqual(original_meta.as_cql_query(), current_meta.as_cql_query())

        cluster3 = TestCluster(schema_event_refresh_window=-1)
        cluster3.connect()
        try:
            self.assertNotIn("mv2", cluster3.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].views)
            self.session.execute(
                "CREATE MATERIALIZED VIEW {0}.mv2 AS SELECT a, b FROM {0}.{1} "
                "WHERE a IS NOT NULL AND b IS NOT NULL PRIMARY KEY (a, b)".format(
                    self.keyspace_name, self.function_table_name)
            )
            self.assertNotIn("mv2", cluster3.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].views)
            cluster3.refresh_materialized_view_metadata(self.keyspace_name, 'mv2')
            self.assertIn("mv2", cluster3.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].views)
        finally:
            cluster3.shutdown()

    def test_refresh_user_type_metadata(self):
        """
        test for synchronously refreshing UDT metadata in keyspace

        test_refresh_user_type_metadata tests that UDT metadata in a keyspace is refreshed when calling refresh_user_type_metadata().
        It creates a second cluster object with schema_event_refresh_window=-1 such that schema refreshes are disabled
        for schema change push events. It then alters the keyspace, creating a new UDT, using the first cluster
        object, and verifies that the UDT metadata has not changed in the second cluster object. Finally, it calls
        refresh_user_type_metadata() and verifies that the UDT metadata in the keyspace is updated in the second cluster object.

        @since 2.6.0
        @jira_ticket PYTHON-291
        @expected_result UDT metadata in the keyspace should be refreshed when refresh_user_type_metadata() is called.

        @test_category metadata
        """

        if PROTOCOL_VERSION < 3:
            raise unittest.SkipTest("Protocol 3+ is required for UDTs, currently testing against {0}".format(PROTOCOL_VERSION))

        cluster2 = TestCluster(schema_event_refresh_window=-1)
        cluster2.connect()

        self.assertEqual(cluster2.metadata.keyspaces[self.keyspace_name].user_types, {})
        self.session.execute("CREATE TYPE {0}.user (age int, name text)".format(self.keyspace_name))
        self.assertEqual(cluster2.metadata.keyspaces[self.keyspace_name].user_types, {})

        cluster2.refresh_user_type_metadata(self.keyspace_name, "user")
        self.assertIn("user", cluster2.metadata.keyspaces[self.keyspace_name].user_types)

        cluster2.shutdown()

    @greaterthancass20
    def test_refresh_user_type_metadata_proto_2(self):
        """
        Test to insure that protocol v1/v2 surface UDT metadata changes

        @since 3.7.0
        @jira_ticket PYTHON-106
        @expected_result UDT metadata in the keyspace should be updated regardless of protocol version

        @test_category metadata
        """
        supported_versions = get_supported_protocol_versions()
        if 2 not in supported_versions:  # 1 and 2 were dropped in the same version
                raise unittest.SkipTest("Protocol versions 1 and 2 are not supported in Cassandra version ".format(CASSANDRA_VERSION))

        for protocol_version in (1, 2):
            cluster = TestCluster()
            session = cluster.connect()
            self.assertEqual(cluster.metadata.keyspaces[self.keyspace_name].user_types, {})

            session.execute("CREATE TYPE {0}.user (age int, name text)".format(self.keyspace_name))
            self.assertIn("user", cluster.metadata.keyspaces[self.keyspace_name].user_types)
            self.assertIn("age", cluster.metadata.keyspaces[self.keyspace_name].user_types["user"].field_names)
            self.assertIn("name", cluster.metadata.keyspaces[self.keyspace_name].user_types["user"].field_names)

            session.execute("ALTER TYPE {0}.user ADD flag boolean".format(self.keyspace_name))
            self.assertIn("flag", cluster.metadata.keyspaces[self.keyspace_name].user_types["user"].field_names)

            session.execute("ALTER TYPE {0}.user RENAME flag TO something".format(self.keyspace_name))
            self.assertIn("something", cluster.metadata.keyspaces[self.keyspace_name].user_types["user"].field_names)

            session.execute("DROP TYPE {0}.user".format(self.keyspace_name))
            self.assertEqual(cluster.metadata.keyspaces[self.keyspace_name].user_types, {})
            cluster.shutdown()

    def test_refresh_user_function_metadata(self):
        """
        test for synchronously refreshing UDF metadata in keyspace

        test_refresh_user_function_metadata tests that UDF metadata in a keyspace is refreshed when calling
        refresh_user_function_metadata(). It creates a second cluster object with schema_event_refresh_window=-1 such
        that schema refreshes are disabled for schema change push events. It then alters the keyspace, creating a new
        UDF, using the first cluster object, and verifies that the UDF metadata has not changed in the second cluster
        object. Finally, it calls refresh_user_function_metadata() and verifies that the UDF metadata in the keyspace
        is updated in the second cluster object.

        @since 2.6.0
        @jira_ticket PYTHON-291
        @expected_result UDF metadata in the keyspace should be refreshed when refresh_user_function_metadata() is called.

        @test_category metadata
        """

        if PROTOCOL_VERSION < 4:
            raise unittest.SkipTest("Protocol 4+ is required for UDFs, currently testing against {0}".format(PROTOCOL_VERSION))

        cluster2 = TestCluster(schema_event_refresh_window=-1)
        cluster2.connect()

        self.assertEqual(cluster2.metadata.keyspaces[self.keyspace_name].functions, {})
        self.session.execute("""CREATE FUNCTION {0}.sum_int(key int, val int)
                            RETURNS NULL ON NULL INPUT
                            RETURNS int
                            LANGUAGE java AS ' return key + val;';""".format(self.keyspace_name))

        self.assertEqual(cluster2.metadata.keyspaces[self.keyspace_name].functions, {})
        cluster2.refresh_user_function_metadata(self.keyspace_name, UserFunctionDescriptor("sum_int", ["int", "int"]))
        self.assertIn("sum_int(int,int)", cluster2.metadata.keyspaces[self.keyspace_name].functions)

        cluster2.shutdown()

    def test_refresh_user_aggregate_metadata(self):
        """
        test for synchronously refreshing UDA metadata in keyspace

        test_refresh_user_aggregate_metadata tests that UDA metadata in a keyspace is refreshed when calling
        refresh_user_aggregate_metadata(). It creates a second cluster object with schema_event_refresh_window=-1 such
        that schema refreshes are disabled for schema change push events. It then alters the keyspace, creating a new
        UDA, using the first cluster object, and verifies that the UDA metadata has not changed in the second cluster
        object. Finally, it calls refresh_user_aggregate_metadata() and verifies that the UDF metadata in the keyspace
        is updated in the second cluster object.

        @since 2.6.0
        @jira_ticket PYTHON-291
        @expected_result UDA metadata in the keyspace should be refreshed when refresh_user_aggregate_metadata() is called.

        @test_category metadata
        """

        if PROTOCOL_VERSION < 4:
            raise unittest.SkipTest("Protocol 4+ is required for UDAs, currently testing against {0}".format(PROTOCOL_VERSION))

        cluster2 = TestCluster(schema_event_refresh_window=-1)
        cluster2.connect()

        self.assertEqual(cluster2.metadata.keyspaces[self.keyspace_name].aggregates, {})
        self.session.execute("""CREATE FUNCTION {0}.sum_int(key int, val int)
                            RETURNS NULL ON NULL INPUT
                            RETURNS int
                            LANGUAGE java AS 'return key + val;';""".format(self.keyspace_name))

        self.session.execute("""CREATE AGGREGATE {0}.sum_agg(int)
                             SFUNC sum_int
                             STYPE int
                             INITCOND 0"""
                             .format(self.keyspace_name))

        self.assertEqual(cluster2.metadata.keyspaces[self.keyspace_name].aggregates, {})
        cluster2.refresh_user_aggregate_metadata(self.keyspace_name, UserAggregateDescriptor("sum_agg", ["int"]))
        self.assertIn("sum_agg(int)", cluster2.metadata.keyspaces[self.keyspace_name].aggregates)

        cluster2.shutdown()

    @greaterthanorequalcass30
    def test_multiple_indices(self):
        """
        test multiple indices on the same column.

        Creates a table and two indices. Ensures that both indices metatdata is surface appropriately.

        @since 3.0.0
        @jira_ticket PYTHON-276
        @expected_result IndexMetadata is appropriately surfaced

        @test_category metadata
        """

        self.session.execute("CREATE TABLE {0}.{1} (a int PRIMARY KEY, b map<text, int>)".format(self.keyspace_name, self.function_table_name))
        self.session.execute("CREATE INDEX index_1 ON {0}.{1}(b)".format(self.keyspace_name, self.function_table_name))
        self.session.execute("CREATE INDEX index_2 ON {0}.{1}(keys(b))".format(self.keyspace_name, self.function_table_name))

        indices = self.cluster.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].indexes
        self.assertEqual(len(indices), 2)
        index_1 = indices["index_1"]
        index_2 = indices['index_2']
        self.assertEqual(index_1.table_name, "test_multiple_indices")
        self.assertEqual(index_1.name, "index_1")
        self.assertEqual(index_1.kind, "COMPOSITES")
        self.assertEqual(index_1.index_options["target"], "values(b)")
        self.assertEqual(index_1.keyspace_name, "schemametadatatests")
        self.assertEqual(index_2.table_name, "test_multiple_indices")
        self.assertEqual(index_2.name, "index_2")
        self.assertEqual(index_2.kind, "COMPOSITES")
        self.assertEqual(index_2.index_options["target"], "keys(b)")
        self.assertEqual(index_2.keyspace_name, "schemametadatatests")

    @greaterthanorequalcass30
    def test_table_extensions(self):
        s = self.session
        ks = self.keyspace_name
        ks_meta = s.cluster.metadata.keyspaces[ks]
        t = self.function_table_name
        v = t + 'view'

        s.execute("CREATE TABLE %s.%s (k text PRIMARY KEY, v int)" % (ks, t))
        s.execute(
            "CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM %s.%s "
            "WHERE v IS NOT NULL AND k IS NOT NULL PRIMARY KEY (v, k)" % (ks, v, ks, t)
        )

        table_meta = ks_meta.tables[t]
        view_meta = table_meta.views[v]

        self.assertFalse(table_meta.extensions)
        self.assertFalse(view_meta.extensions)

        original_table_cql = table_meta.export_as_string()
        original_view_cql = view_meta.export_as_string()

        # extensions registered, not present
        # --------------------------------------
        class Ext0(RegisteredTableExtension):
            name = t

            @classmethod
            def after_table_cql(cls, table_meta, ext_key, ext_blob):
                return "%s %s %s %s" % (cls.name, table_meta.name, ext_key, ext_blob)

        class Ext1(Ext0):
            name = t + '##'

        self.assertFalse(table_meta.extensions)
        self.assertFalse(view_meta.extensions)
        self.assertIn(Ext0.name, _RegisteredExtensionType._extension_registry)
        self.assertIn(Ext1.name, _RegisteredExtensionType._extension_registry)
        # There will bee the RLAC extension here.
        self.assertEqual(len(_RegisteredExtensionType._extension_registry), 3)

        self.cluster.refresh_table_metadata(ks, t)
        table_meta = ks_meta.tables[t]
        view_meta = table_meta.views[v]

        self.assertEqual(table_meta.export_as_string(), original_table_cql)
        self.assertEqual(view_meta.export_as_string(), original_view_cql)

        update_t = s.prepare('UPDATE system_schema.tables SET extensions=? WHERE keyspace_name=? AND table_name=?')  # for blob type coercing
        update_v = s.prepare('UPDATE system_schema.views SET extensions=? WHERE keyspace_name=? AND view_name=?')
        # extensions registered, one present
        # --------------------------------------
        ext_map = {Ext0.name: b"THA VALUE"}
        [(s.execute(update_t, (ext_map, ks, t)), s.execute(update_v, (ext_map, ks, v)))
         for _ in self.cluster.metadata.all_hosts()]  # we're manipulating metadata - do it on all hosts
        self.cluster.refresh_table_metadata(ks, t)
        self.cluster.refresh_materialized_view_metadata(ks, v)
        table_meta = ks_meta.tables[t]
        view_meta = table_meta.views[v]

        self.assertIn(Ext0.name, table_meta.extensions)
        new_cql = table_meta.export_as_string()
        self.assertNotEqual(new_cql, original_table_cql)
        self.assertIn(Ext0.after_table_cql(table_meta, Ext0.name, ext_map[Ext0.name]), new_cql)
        self.assertNotIn(Ext1.name, new_cql)

        self.assertIn(Ext0.name, view_meta.extensions)
        new_cql = view_meta.export_as_string()
        self.assertNotEqual(new_cql, original_view_cql)
        self.assertIn(Ext0.after_table_cql(view_meta, Ext0.name, ext_map[Ext0.name]), new_cql)
        self.assertNotIn(Ext1.name, new_cql)

        # extensions registered, one present
        # --------------------------------------
        ext_map = {Ext0.name: b"THA VALUE",
                   Ext1.name: b"OTHA VALUE"}
        [(s.execute(update_t, (ext_map, ks, t)), s.execute(update_v, (ext_map, ks, v)))
         for _ in self.cluster.metadata.all_hosts()]  # we're manipulating metadata - do it on all hosts
        self.cluster.refresh_table_metadata(ks, t)
        self.cluster.refresh_materialized_view_metadata(ks, v)
        table_meta = ks_meta.tables[t]
        view_meta = table_meta.views[v]

        self.assertIn(Ext0.name, table_meta.extensions)
        self.assertIn(Ext1.name, table_meta.extensions)
        new_cql = table_meta.export_as_string()
        self.assertNotEqual(new_cql, original_table_cql)
        self.assertIn(Ext0.after_table_cql(table_meta, Ext0.name, ext_map[Ext0.name]), new_cql)
        self.assertIn(Ext1.after_table_cql(table_meta, Ext1.name, ext_map[Ext1.name]), new_cql)

        self.assertIn(Ext0.name, view_meta.extensions)
        self.assertIn(Ext1.name, view_meta.extensions)
        new_cql = view_meta.export_as_string()
        self.assertNotEqual(new_cql, original_view_cql)
        self.assertIn(Ext0.after_table_cql(view_meta, Ext0.name, ext_map[Ext0.name]), new_cql)
        self.assertIn(Ext1.after_table_cql(view_meta, Ext1.name, ext_map[Ext1.name]), new_cql)


class TestCodeCoverage(unittest.TestCase):

    def test_export_schema(self):
        """
        Test export schema functionality
        """

        cluster = TestCluster()
        cluster.connect()

        self.assertIsInstance(cluster.metadata.export_schema_as_string(), str)
        cluster.shutdown()

    def test_export_keyspace_schema(self):
        """
        Test export keyspace schema functionality
        """

        cluster = TestCluster()
        cluster.connect()

        for keyspace in cluster.metadata.keyspaces:
            keyspace_metadata = cluster.metadata.keyspaces[keyspace]
            self.assertIsInstance(keyspace_metadata.export_as_string(), str)
            self.assertIsInstance(keyspace_metadata.as_cql_query(), str)
        cluster.shutdown()

    def assert_equal_diff(self, received, expected):
        if received != expected:
            diff_string = '\n'.join(difflib.unified_diff(expected.split('\n'),
                                                         received.split('\n'),
                                                         'EXPECTED', 'RECEIVED',
                                                         lineterm=''))
            self.fail(diff_string)

    def assert_startswith_diff(self, received, prefix):
        if not received.startswith(prefix):
            prefix_lines = prefix.split('\n')
            diff_string = '\n'.join(difflib.unified_diff(prefix_lines,
                                                         received.split('\n')[:len(prefix_lines)],
                                                         'EXPECTED', 'RECEIVED',
                                                         lineterm=''))
            self.fail(diff_string)

    @greaterthancass20
    def test_export_keyspace_schema_udts(self):
        """
        Test udt exports
        """

        if PROTOCOL_VERSION < 3:
            raise unittest.SkipTest(
                "Protocol 3.0+ is required for UDT change events, currently testing against %r"
                % (PROTOCOL_VERSION,))

        if sys.version_info[0:2] != (2, 7):
            raise unittest.SkipTest('This test compares static strings generated from dict items, which may change orders. Test with 2.7.')

        cluster = TestCluster()
        session = cluster.connect()

        session.execute("""
            CREATE KEYSPACE export_udts
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            AND durable_writes = true;
        """)
        session.execute("""
            CREATE TYPE export_udts.street (
                street_number int,
                street_name text)
        """)
        session.execute("""
            CREATE TYPE export_udts.zip (
                zipcode int,
                zip_plus_4 int)
        """)
        session.execute("""
            CREATE TYPE export_udts.address (
                street_address frozen<street>,
                zip_code frozen<zip>)
        """)
        session.execute("""
            CREATE TABLE export_udts.users (
            user text PRIMARY KEY,
            addresses map<text, frozen<address>>)
        """)

        expected_prefix = """CREATE KEYSPACE export_udts WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TYPE export_udts.street (
    street_number int,
    street_name text
);

CREATE TYPE export_udts.zip (
    zipcode int,
    zip_plus_4 int
);

CREATE TYPE export_udts.address (
    street_address frozen<street>,
    zip_code frozen<zip>
);

CREATE TABLE export_udts.users (
    user text PRIMARY KEY,
    addresses map<text, frozen<address>>"""

        self.assert_startswith_diff(cluster.metadata.keyspaces['export_udts'].export_as_string(), expected_prefix)

        table_meta = cluster.metadata.keyspaces['export_udts'].tables['users']

        expected_prefix = """CREATE TABLE export_udts.users (
    user text PRIMARY KEY,
    addresses map<text, frozen<address>>"""

        self.assert_startswith_diff(table_meta.export_as_string(), expected_prefix)

        cluster.shutdown()

    @greaterthancass21
    def test_case_sensitivity(self):
        """
        Test that names that need to be escaped in CREATE statements are
        """

        cluster = TestCluster()
        session = cluster.connect()

        ksname = 'AnInterestingKeyspace'
        cfname = 'AnInterestingTable'

        session.execute("DROP KEYSPACE IF EXISTS {0}".format(ksname))
        session.execute("""
            CREATE KEYSPACE "%s"
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """ % (ksname,))
        session.execute("""
            CREATE TABLE "%s"."%s" (
                k int,
                "A" int,
                "B" int,
                "MyColumn" int,
                PRIMARY KEY (k, "A"))
            WITH CLUSTERING ORDER BY ("A" DESC)
            """ % (ksname, cfname))
        session.execute("""
            CREATE INDEX myindex ON "%s"."%s" ("MyColumn")
            """ % (ksname, cfname))
        session.execute("""
            CREATE INDEX "AnotherIndex" ON "%s"."%s" ("B")
            """ % (ksname, cfname))

        ksmeta = cluster.metadata.keyspaces[ksname]
        schema = ksmeta.export_as_string()
        self.assertIn('CREATE KEYSPACE "AnInterestingKeyspace"', schema)
        self.assertIn('CREATE TABLE "AnInterestingKeyspace"."AnInterestingTable"', schema)
        self.assertIn('"A" int', schema)
        self.assertIn('"B" int', schema)
        self.assertIn('"MyColumn" int', schema)
        self.assertIn('PRIMARY KEY (k, "A")', schema)
        self.assertIn('WITH CLUSTERING ORDER BY ("A" DESC)', schema)
        self.assertIn('CREATE INDEX myindex ON "AnInterestingKeyspace"."AnInterestingTable" ("MyColumn")', schema)
        self.assertIn('CREATE INDEX "AnotherIndex" ON "AnInterestingKeyspace"."AnInterestingTable" ("B")', schema)
        cluster.shutdown()

    def test_already_exists_exceptions(self):
        """
        Ensure AlreadyExists exception is thrown when hit
        """

        cluster = TestCluster()
        session = cluster.connect()

        ksname = 'test3rf'
        cfname = 'test'

        ddl = '''
            CREATE KEYSPACE %s
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}'''
        self.assertRaises(AlreadyExists, session.execute, ddl % ksname)

        ddl = '''
            CREATE TABLE %s.%s (
                k int PRIMARY KEY,
                v int )'''
        self.assertRaises(AlreadyExists, session.execute, ddl % (ksname, cfname))
        cluster.shutdown()

    @local
    def test_replicas(self):
        """
        Ensure cluster.metadata.get_replicas return correctly when not attached to keyspace
        """
        if murmur3 is None:
            raise unittest.SkipTest('the murmur3 extension is not available')

        cluster = TestCluster()
        self.assertEqual(cluster.metadata.get_replicas('test3rf', 'key'), [])

        cluster.connect('test3rf')

        self.assertNotEqual(list(cluster.metadata.get_replicas('test3rf', b'key')), [])
        host = list(cluster.metadata.get_replicas('test3rf', b'key'))[0]
        self.assertEqual(host.datacenter, 'dc1')
        self.assertEqual(host.rack, 'r1')
        cluster.shutdown()

    def test_token_map(self):
        """
        Test token mappings
        """

        cluster = TestCluster()
        cluster.connect('test3rf')
        ring = cluster.metadata.token_map.ring
        owners = list(cluster.metadata.token_map.token_to_host_owner[token] for token in ring)
        get_replicas = cluster.metadata.token_map.get_replicas

        for ksname in ('test1rf', 'test2rf', 'test3rf'):
            self.assertNotEqual(list(get_replicas(ksname, ring[0])), [])

        for i, token in enumerate(ring):
            self.assertEqual(set(get_replicas('test3rf', token)), set(owners))
            self.assertEqual(set(get_replicas('test2rf', token)), set([owners[i], owners[(i + 1) % 3]]))
            self.assertEqual(set(get_replicas('test1rf', token)), set([owners[i]]))
        cluster.shutdown()


class TokenMetadataTest(unittest.TestCase):
    """
    Test of TokenMap creation and other behavior.
    """
    @local
    def test_token(self):
        expected_node_count = len(get_cluster().nodes)

        cluster = TestCluster()
        cluster.connect()
        tmap = cluster.metadata.token_map
        self.assertTrue(issubclass(tmap.token_class, Token))
        self.assertEqual(expected_node_count, len(tmap.ring))
        cluster.shutdown()


class KeyspaceAlterMetadata(unittest.TestCase):
    """
    Test verifies that table metadata is preserved on keyspace alter
    """
    def setUp(self):
        self.cluster = TestCluster()
        self.session = self.cluster.connect()
        name = self._testMethodName.lower()
        crt_ks = '''
                CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} AND durable_writes = true''' % name
        self.session.execute(crt_ks)

    def tearDown(self):
        name = self._testMethodName.lower()
        self.session.execute('DROP KEYSPACE %s' % name)
        self.cluster.shutdown()

    def test_keyspace_alter(self):
        """
        Table info is preserved upon keyspace alter:
        Create table
        Verify schema
        Alter ks
        Verify that table metadata is still present

        PYTHON-173
        """
        name = self._testMethodName.lower()

        self.session.execute('CREATE TABLE %s.d (d INT PRIMARY KEY)' % name)
        original_keyspace_meta = self.cluster.metadata.keyspaces[name]
        self.assertEqual(original_keyspace_meta.durable_writes, True)
        self.assertEqual(len(original_keyspace_meta.tables), 1)

        self.session.execute('ALTER KEYSPACE %s WITH durable_writes = false' % name)
        new_keyspace_meta = self.cluster.metadata.keyspaces[name]
        self.assertNotEqual(original_keyspace_meta, new_keyspace_meta)
        self.assertEqual(new_keyspace_meta.durable_writes, False)


class IndexMapTests(unittest.TestCase):

    keyspace_name = 'index_map_tests'

    @property
    def table_name(self):
        return self._testMethodName.lower()

    @classmethod
    def setup_class(cls):
        cls.cluster = TestCluster()
        cls.session = cls.cluster.connect()
        try:
            if cls.keyspace_name in cls.cluster.metadata.keyspaces:
                cls.session.execute("DROP KEYSPACE %s" % cls.keyspace_name)

            cls.session.execute(
                """
                CREATE KEYSPACE %s
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
                """ % cls.keyspace_name)
            cls.session.set_keyspace(cls.keyspace_name)
        except Exception:
            cls.cluster.shutdown()
            raise

    @classmethod
    def teardown_class(cls):
        try:
            cls.session.execute("DROP KEYSPACE %s" % cls.keyspace_name)
        finally:
            cls.cluster.shutdown()

    def create_basic_table(self):
        self.session.execute("CREATE TABLE %s (k int PRIMARY KEY, a int)" % self.table_name)

    def drop_basic_table(self):
        self.session.execute("DROP TABLE %s" % self.table_name)

    def test_index_updates(self):
        self.create_basic_table()

        ks_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
        table_meta = ks_meta.tables[self.table_name]
        self.assertNotIn('a_idx', ks_meta.indexes)
        self.assertNotIn('b_idx', ks_meta.indexes)
        self.assertNotIn('a_idx', table_meta.indexes)
        self.assertNotIn('b_idx', table_meta.indexes)

        self.session.execute("CREATE INDEX a_idx ON %s (a)" % self.table_name)
        self.session.execute("ALTER TABLE %s ADD b int" % self.table_name)
        self.session.execute("CREATE INDEX b_idx ON %s (b)" % self.table_name)

        ks_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
        table_meta = ks_meta.tables[self.table_name]
        self.assertIsInstance(ks_meta.indexes['a_idx'], IndexMetadata)
        self.assertIsInstance(ks_meta.indexes['b_idx'], IndexMetadata)
        self.assertIsInstance(table_meta.indexes['a_idx'], IndexMetadata)
        self.assertIsInstance(table_meta.indexes['b_idx'], IndexMetadata)

        # both indexes updated when index dropped
        self.session.execute("DROP INDEX a_idx")

        # temporarily synchronously refresh the schema metadata, until CASSANDRA-9391 is merged in
        self.cluster.refresh_table_metadata(self.keyspace_name, self.table_name)

        ks_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
        table_meta = ks_meta.tables[self.table_name]
        self.assertNotIn('a_idx', ks_meta.indexes)
        self.assertIsInstance(ks_meta.indexes['b_idx'], IndexMetadata)
        self.assertNotIn('a_idx', table_meta.indexes)
        self.assertIsInstance(table_meta.indexes['b_idx'], IndexMetadata)

        # keyspace index updated when table dropped
        self.drop_basic_table()
        ks_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
        self.assertNotIn(self.table_name, ks_meta.tables)
        self.assertNotIn('a_idx', ks_meta.indexes)
        self.assertNotIn('b_idx', ks_meta.indexes)

    def test_index_follows_alter(self):
        self.create_basic_table()

        idx = self.table_name + '_idx'
        self.session.execute("CREATE INDEX %s ON %s (a)" % (idx, self.table_name))
        ks_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
        table_meta = ks_meta.tables[self.table_name]
        self.assertIsInstance(ks_meta.indexes[idx], IndexMetadata)
        self.assertIsInstance(table_meta.indexes[idx], IndexMetadata)
        self.session.execute('ALTER KEYSPACE %s WITH durable_writes = false' % self.keyspace_name)
        old_meta = ks_meta
        ks_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
        self.assertIsNot(ks_meta, old_meta)
        table_meta = ks_meta.tables[self.table_name]
        self.assertIsInstance(ks_meta.indexes[idx], IndexMetadata)
        self.assertIsInstance(table_meta.indexes[idx], IndexMetadata)
        self.drop_basic_table()


class FunctionTest(unittest.TestCase):
    """
    Base functionality for Function and Aggregate metadata test classes
    """

    def setUp(self):
        """
        Tests are skipped if run with native protocol version < 4
        """

        if PROTOCOL_VERSION < 4:
            raise unittest.SkipTest("Function metadata requires native protocol version 4+")

    @property
    def function_name(self):
        return self._testMethodName.lower()

    @classmethod
    def setup_class(cls):
        if PROTOCOL_VERSION >= 4:
            cls.cluster = TestCluster()
            cls.keyspace_name = cls.__name__.lower()
            cls.session = cls.cluster.connect()
            cls.session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}" % cls.keyspace_name)
            cls.session.set_keyspace(cls.keyspace_name)
            cls.keyspace_function_meta = cls.cluster.metadata.keyspaces[cls.keyspace_name].functions
            cls.keyspace_aggregate_meta = cls.cluster.metadata.keyspaces[cls.keyspace_name].aggregates

    @classmethod
    def teardown_class(cls):
        if PROTOCOL_VERSION >= 4:
            cls.session.execute("DROP KEYSPACE IF EXISTS %s" % cls.keyspace_name)
            cls.cluster.shutdown()

    class Verified(object):

        def __init__(self, test_case, meta_class, element_meta, **function_kwargs):
            self.test_case = test_case
            self.function_kwargs = dict(function_kwargs)
            self.meta_class = meta_class
            self.element_meta = element_meta

        def __enter__(self):
            tc = self.test_case
            expected_meta = self.meta_class(**self.function_kwargs)
            tc.assertNotIn(expected_meta.signature, self.element_meta)
            tc.session.execute(expected_meta.as_cql_query())
            tc.assertIn(expected_meta.signature, self.element_meta)

            generated_meta = self.element_meta[expected_meta.signature]
            self.test_case.assertEqual(generated_meta.as_cql_query(), expected_meta.as_cql_query())
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            tc = self.test_case
            tc.session.execute("DROP %s %s.%s" % (self.meta_class.__name__, tc.keyspace_name, self.signature))
            tc.assertNotIn(self.signature, self.element_meta)

        @property
        def signature(self):
            return SignatureDescriptor.format_signature(self.function_kwargs['name'],
                                                        self.function_kwargs['argument_types'])

    class VerifiedFunction(Verified):
        def __init__(self, test_case, **kwargs):
            super(FunctionTest.VerifiedFunction, self).__init__(test_case, Function, test_case.keyspace_function_meta, **kwargs)

    class VerifiedAggregate(Verified):
        def __init__(self, test_case, **kwargs):
            super(FunctionTest.VerifiedAggregate, self).__init__(test_case, Aggregate, test_case.keyspace_aggregate_meta, **kwargs)


class FunctionMetadata(FunctionTest):

    def make_function_kwargs(self, called_on_null=True):
        return {'keyspace': self.keyspace_name,
                'name': self.function_name,
                'argument_types': ['double', 'int'],
                'argument_names': ['d', 'i'],
                'return_type': 'double',
                'language': 'java',
                'body': 'return new Double(0.0);',
                'called_on_null_input': called_on_null,
                'deterministic': False,
                'monotonic': False,
                'monotonic_on': []}

    def test_functions_after_udt(self):
        """
        Test to to ensure functions come after UDTs in in keyspace dump

        test_functions_after_udt creates a basic function. Then queries that function and  make sure that in the results
        that UDT's are listed before any corresponding functions, when we dump the keyspace

        Ideally we would make a function that takes a udt type, but this presently fails because C* c059a56 requires
        udt to be frozen to create, but does not store meta indicating frozen
        SEE https://issues.apache.org/jira/browse/CASSANDRA-9186
        Maybe update this after release
        kwargs = self.make_function_kwargs()
        kwargs['argument_types'][0] = "frozen<%s>" % udt_name
        expected_meta = Function(**kwargs)
        with self.VerifiedFunction(self, **kwargs):

        @since 2.6.0
        @jira_ticket PYTHON-211
        @expected_result UDT's should come before any functions
        @test_category function
        """

        self.assertNotIn(self.function_name, self.keyspace_function_meta)

        udt_name = 'udtx'
        self.session.execute("CREATE TYPE %s (x int)" % udt_name)

        with self.VerifiedFunction(self, **self.make_function_kwargs()):
            # udts must come before functions in keyspace dump
            keyspace_cql = self.cluster.metadata.keyspaces[self.keyspace_name].export_as_string()
            type_idx = keyspace_cql.rfind("CREATE TYPE")
            func_idx = keyspace_cql.find("CREATE FUNCTION")
            self.assertNotIn(-1, (type_idx, func_idx), "TYPE or FUNCTION not found in keyspace_cql: " + keyspace_cql)
            self.assertGreater(func_idx, type_idx)

    def test_function_same_name_diff_types(self):
        """
        Test to verify to that functions with different signatures are differentiated in metadata

        test_function_same_name_diff_types Creates two functions. One with the same name but a slightly different
        signature. Then ensures that both are surfaced separately in our metadata.

        @since 2.6.0
        @jira_ticket PYTHON-211
        @expected_result function with the same name but different signatures should be surfaced separately
        @test_category function
        """

        # Create a function
        kwargs = self.make_function_kwargs()
        with self.VerifiedFunction(self, **kwargs):

            # another function: same name, different type sig.
            self.assertGreater(len(kwargs['argument_types']), 1)
            self.assertGreater(len(kwargs['argument_names']), 1)
            kwargs['argument_types'] = kwargs['argument_types'][:1]
            kwargs['argument_names'] = kwargs['argument_names'][:1]

            # Ensure they are surfaced separately
            with self.VerifiedFunction(self, **kwargs):
                functions = [f for f in self.keyspace_function_meta.values() if f.name == self.function_name]
                self.assertEqual(len(functions), 2)
                self.assertNotEqual(functions[0].argument_types, functions[1].argument_types)

    def test_function_no_parameters(self):
        """
        Test to verify CQL output for functions with zero parameters

        Creates a function with no input parameters, verify that CQL output is correct.

        @since 2.7.1
        @jira_ticket PYTHON-392
        @expected_result function with no parameters should generate proper CQL
        @test_category function
        """
        kwargs = self.make_function_kwargs()
        kwargs['argument_types'] = []
        kwargs['argument_names'] = []
        kwargs['return_type'] = 'bigint'
        kwargs['body'] = 'return System.currentTimeMillis() / 1000L;'

        with self.VerifiedFunction(self, **kwargs) as vf:
            fn_meta = self.keyspace_function_meta[vf.signature]
            self.assertRegex(fn_meta.as_cql_query(), "CREATE FUNCTION.*%s\(\) .*" % kwargs['name'])

    def test_functions_follow_keyspace_alter(self):
        """
        Test to verify to that functions maintain equality after a keyspace is altered

        test_functions_follow_keyspace_alter creates a function then alters a the keyspace associated with that function.
        After the alter we validate that the function maintains the same metadata


        @since 2.6.0
        @jira_ticket PYTHON-211
        @expected_result functions are the same after parent keyspace is altered
        @test_category function
        """

        # Create function
        with self.VerifiedFunction(self, **self.make_function_kwargs()):
            original_keyspace_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
            self.session.execute('ALTER KEYSPACE %s WITH durable_writes = false' % self.keyspace_name)

            # After keyspace alter ensure that we maintain function equality.
            try:
                new_keyspace_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
                self.assertNotEqual(original_keyspace_meta, new_keyspace_meta)
                self.assertIs(original_keyspace_meta.functions, new_keyspace_meta.functions)
            finally:
                self.session.execute('ALTER KEYSPACE %s WITH durable_writes = true' % self.keyspace_name)

    def test_function_cql_called_on_null(self):
        """
        Test to verify to that that called on null argument is honored on function creation.

        test_functions_follow_keyspace_alter create two functions. One with the called_on_null_input set to true,
        the other with it set to false. We then verify that the metadata constructed from those function is correctly
        reflected


        @since 2.6.0
        @jira_ticket PYTHON-211
        @expected_result functions metadata correctly reflects called_on_null_input flag.
        @test_category function
        """

        kwargs = self.make_function_kwargs()
        kwargs['called_on_null_input'] = True
        with self.VerifiedFunction(self, **kwargs) as vf:
            fn_meta = self.keyspace_function_meta[vf.signature]
            self.assertRegex(fn_meta.as_cql_query(), "CREATE FUNCTION.*\) CALLED ON NULL INPUT RETURNS .*")

        kwargs['called_on_null_input'] = False
        with self.VerifiedFunction(self, **kwargs) as vf:
            fn_meta = self.keyspace_function_meta[vf.signature]
            self.assertRegex(fn_meta.as_cql_query(), "CREATE FUNCTION.*\) RETURNS NULL ON NULL INPUT RETURNS .*")


class AggregateMetadata(FunctionTest):

    @classmethod
    def setup_class(cls):
        if PROTOCOL_VERSION >= 4:
            super(AggregateMetadata, cls).setup_class()

            cls.session.execute("""CREATE OR REPLACE FUNCTION sum_int(s int, i int)
                                   RETURNS NULL ON NULL INPUT
                                   RETURNS int
                                   LANGUAGE java AS 'return s + i;';""")
            cls.session.execute("""CREATE OR REPLACE FUNCTION sum_int_two(s int, i int, j int)
                                   RETURNS NULL ON NULL INPUT
                                   RETURNS int
                                   LANGUAGE java AS 'return s + i + j;';""")
            cls.session.execute("""CREATE OR REPLACE FUNCTION "List_As_String"(l list<text>)
                                   RETURNS NULL ON NULL INPUT
                                   RETURNS int
                                   LANGUAGE java AS 'return l.size();';""")
            cls.session.execute("""CREATE OR REPLACE FUNCTION extend_list(s list<text>, i int)
                                   CALLED ON NULL INPUT
                                   RETURNS list<text>
                                   LANGUAGE java AS 'if (i != null) s.add(i.toString()); return s;';""")
            cls.session.execute("""CREATE OR REPLACE FUNCTION update_map(s map<int, int>, i int)
                                   RETURNS NULL ON NULL INPUT
                                   RETURNS map<int, int>
                                   LANGUAGE java AS 's.put(new Integer(i), new Integer(i)); return s;';""")
            cls.session.execute("""CREATE TABLE IF NOT EXISTS t
                                   (k int PRIMARY KEY, v int)""")
            for x in range(4):
                cls.session.execute("INSERT INTO t (k,v) VALUES (%s, %s)", (x, x))
            cls.session.execute("INSERT INTO t (k) VALUES (%s)", (4,))

    def make_aggregate_kwargs(self, state_func, state_type, final_func=None, init_cond=None):
        return {'keyspace': self.keyspace_name,
                'name': self.function_name + '_aggregate',
                'argument_types': ['int'],
                'state_func': state_func,
                'state_type': state_type,
                'final_func': final_func,
                'initial_condition': init_cond,
                'return_type': "does not matter for creation",
                'deterministic': False}

    def test_return_type_meta(self):
        """
        Test to verify to that the return type of a an aggregate is honored in the metadata

        test_return_type_meta creates an aggregate then ensures the return type of the created
        aggregate is correctly surfaced in the metadata


        @since 2.6.0
        @jira_ticket PYTHON-211
        @expected_result aggregate has the correct return typ in the metadata
        @test_category aggregate
        """

        with self.VerifiedAggregate(self, **self.make_aggregate_kwargs('sum_int', 'int', init_cond='1')) as va:
            self.assertEqual(self.keyspace_aggregate_meta[va.signature].return_type, 'int')

    def test_init_cond(self):
        """
        Test to verify that various initial conditions are correctly surfaced in various aggregate functions

        test_init_cond creates several different types of aggregates, and given various initial conditions it verifies that
        they correctly impact the aggregate's execution


        @since 2.6.0
        @jira_ticket PYTHON-211
        @expected_result initial conditions are correctly evaluated as part of the aggregates
        @test_category aggregate
        """

        # This is required until the java driver bundled with C* is updated to support v4
        c = TestCluster(protocol_version=3)
        s = c.connect(self.keyspace_name)

        encoder = Encoder()

        expected_values = range(4)

        # int32
        for init_cond in (-1, 0, 1):
            cql_init = encoder.cql_encode_all_types(init_cond)
            with self.VerifiedAggregate(self, **self.make_aggregate_kwargs('sum_int', 'int', init_cond=cql_init)) as va:
                sum_res = s.execute("SELECT %s(v) AS sum FROM t" % va.function_kwargs['name'])[0].sum
                self.assertEqual(sum_res, int(init_cond) + sum(expected_values))

        # list<text>
        for init_cond in ([], ['1', '2']):
            cql_init = encoder.cql_encode_all_types(init_cond)
            with self.VerifiedAggregate(self, **self.make_aggregate_kwargs('extend_list', 'list<text>', init_cond=cql_init)) as va:
                list_res = s.execute("SELECT %s(v) AS list_res FROM t" % va.function_kwargs['name'])[0].list_res
                self.assertListEqual(list_res[:len(init_cond)], init_cond)
                self.assertEqual(set(i for i in list_res[len(init_cond):]),
                                 set(str(i) for i in expected_values))

        # map<int,int>
        expected_map_values = dict((i, i) for i in expected_values)
        expected_key_set = set(expected_values)
        for init_cond in ({}, {1: 2, 3: 4}, {5: 5}):
            cql_init = encoder.cql_encode_all_types(init_cond)
            with self.VerifiedAggregate(self, **self.make_aggregate_kwargs('update_map', 'map<int, int>', init_cond=cql_init)) as va:
                map_res = s.execute("SELECT %s(v) AS map_res FROM t" % va.function_kwargs['name'])[0].map_res
                self.assertLessEqual(expected_map_values.items(), map_res.items())
                init_not_updated = dict((k, init_cond[k]) for k in set(init_cond) - expected_key_set)
                self.assertLessEqual(init_not_updated.items(), map_res.items())
        c.shutdown()

    def test_aggregates_after_functions(self):
        """
        Test to verify that aggregates are listed after function in metadata

        test_aggregates_after_functions creates an aggregate, and then verifies that they are listed
        after any function creations when the keypspace dump is preformed


        @since 2.6.0
        @jira_ticket PYTHON-211
        @expected_result aggregates are declared after any functions
        @test_category aggregate
        """

        # functions must come before functions in keyspace dump
        with self.VerifiedAggregate(self, **self.make_aggregate_kwargs('extend_list', 'list<text>')):
            keyspace_cql = self.cluster.metadata.keyspaces[self.keyspace_name].export_as_string()
            func_idx = keyspace_cql.find("CREATE FUNCTION")
            aggregate_idx = keyspace_cql.rfind("CREATE AGGREGATE")
            self.assertNotIn(-1, (aggregate_idx, func_idx), "AGGREGATE or FUNCTION not found in keyspace_cql: " + keyspace_cql)
            self.assertGreater(aggregate_idx, func_idx)

    def test_same_name_diff_types(self):
        """
        Test to verify to that aggregates with different signatures are differentiated in metadata

        test_same_name_diff_types Creates two Aggregates. One with the same name but a slightly different
        signature. Then ensures that both are surfaced separately in our metadata.

        @since 2.6.0
        @jira_ticket PYTHON-211
        @expected_result aggregates with the same name but different signatures should be surfaced separately
        @test_category function
        """

        kwargs = self.make_aggregate_kwargs('sum_int', 'int', init_cond='0')
        with self.VerifiedAggregate(self, **kwargs):
            kwargs['state_func'] = 'sum_int_two'
            kwargs['argument_types'] = ['int', 'int']
            with self.VerifiedAggregate(self, **kwargs):
                aggregates = [a for a in self.keyspace_aggregate_meta.values() if a.name == kwargs['name']]
                self.assertEqual(len(aggregates), 2)
                self.assertNotEqual(aggregates[0].argument_types, aggregates[1].argument_types)

    def test_aggregates_follow_keyspace_alter(self):
        """
        Test to verify to that aggregates maintain equality after a keyspace is altered

        test_aggregates_follow_keyspace_alter creates a function then alters a the keyspace associated with that
        function. After the alter we validate that the function maintains the same metadata


        @since 2.6.0
        @jira_ticket PYTHON-211
        @expected_result aggregates are the same after parent keyspace is altered
        @test_category function
        """

        with self.VerifiedAggregate(self, **self.make_aggregate_kwargs('sum_int', 'int', init_cond='0')):
            original_keyspace_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
            self.session.execute('ALTER KEYSPACE %s WITH durable_writes = false' % self.keyspace_name)
            try:
                new_keyspace_meta = self.cluster.metadata.keyspaces[self.keyspace_name]
                self.assertNotEqual(original_keyspace_meta, new_keyspace_meta)
                self.assertIs(original_keyspace_meta.aggregates, new_keyspace_meta.aggregates)
            finally:
                self.session.execute('ALTER KEYSPACE %s WITH durable_writes = true' % self.keyspace_name)

    def test_cql_optional_params(self):
        """
        Test to verify that the initial_cond and final_func parameters are correctly honored

        test_cql_optional_params creates various aggregates with different combinations of initial_condition,
        and final_func parameters set. It then ensures they are correctly honored.


        @since 2.6.0
        @jira_ticket PYTHON-211
        @expected_result initial_condition and final_func parameters are honored correctly
        @test_category function
        """

        kwargs = self.make_aggregate_kwargs('extend_list', 'list<text>')
        encoder = Encoder()

        # no initial condition, final func
        self.assertIsNone(kwargs['initial_condition'])
        self.assertIsNone(kwargs['final_func'])
        with self.VerifiedAggregate(self, **kwargs) as va:
            meta = self.keyspace_aggregate_meta[va.signature]
            self.assertIsNone(meta.initial_condition)
            self.assertIsNone(meta.final_func)
            cql = meta.as_cql_query()
            self.assertEqual(cql.find('INITCOND'), -1)
            self.assertEqual(cql.find('FINALFUNC'), -1)

        # initial condition, no final func
        kwargs['initial_condition'] = encoder.cql_encode_all_types(['init', 'cond'])
        with self.VerifiedAggregate(self, **kwargs) as va:
            meta = self.keyspace_aggregate_meta[va.signature]
            self.assertEqual(meta.initial_condition, kwargs['initial_condition'])
            self.assertIsNone(meta.final_func)
            cql = meta.as_cql_query()
            search_string = "INITCOND %s" % kwargs['initial_condition']
            self.assertGreater(cql.find(search_string), 0, '"%s" search string not found in cql:\n%s' % (search_string, cql))
            self.assertEqual(cql.find('FINALFUNC'), -1)

        # no initial condition, final func
        kwargs['initial_condition'] = None
        kwargs['final_func'] = 'List_As_String'
        with self.VerifiedAggregate(self, **kwargs) as va:
            meta = self.keyspace_aggregate_meta[va.signature]
            self.assertIsNone(meta.initial_condition)
            self.assertEqual(meta.final_func, kwargs['final_func'])
            cql = meta.as_cql_query()
            self.assertEqual(cql.find('INITCOND'), -1)
            search_string = 'FINALFUNC "%s"' % kwargs['final_func']
            self.assertGreater(cql.find(search_string), 0, '"%s" search string not found in cql:\n%s' % (search_string, cql))

        # both
        kwargs['initial_condition'] = encoder.cql_encode_all_types(['init', 'cond'])
        kwargs['final_func'] = 'List_As_String'
        with self.VerifiedAggregate(self, **kwargs) as va:
            meta = self.keyspace_aggregate_meta[va.signature]
            self.assertEqual(meta.initial_condition, kwargs['initial_condition'])
            self.assertEqual(meta.final_func, kwargs['final_func'])
            cql = meta.as_cql_query()
            init_cond_idx = cql.find("INITCOND %s" % kwargs['initial_condition'])
            final_func_idx = cql.find('FINALFUNC "%s"' % kwargs['final_func'])
            self.assertNotIn(-1, (init_cond_idx, final_func_idx))
            self.assertGreater(init_cond_idx, final_func_idx)


class BadMetaTest(unittest.TestCase):
    """
    Test behavior when metadata has unexpected form
    Verify that new cluster/session can still connect, and the CQL output indicates the exception with a warning.
    PYTHON-370
    """

    class BadMetaException(Exception):
        pass

    @property
    def function_name(self):
        return self._testMethodName.lower()

    @classmethod
    def setup_class(cls):
        cls.cluster = TestCluster()
        cls.keyspace_name = cls.__name__.lower()
        cls.session = cls.cluster.connect()
        cls.session.execute("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}" % cls.keyspace_name)
        cls.session.set_keyspace(cls.keyspace_name)
        connection = cls.cluster.control_connection._connection

        cls.parser_class = get_schema_parser(
            connection,
            cls.cluster.metadata.get_host(connection.host).release_version,
            cls.cluster.metadata.get_host(connection.host).dse_version,
            timeout=20
        ).__class__
        cls.cluster.control_connection.reconnect = Mock()

    @classmethod
    def teardown_class(cls):
        drop_keyspace_shutdown_cluster(cls.keyspace_name, cls.session, cls.cluster)

    def test_bad_keyspace(self):
        with patch.object(self.parser_class, '_build_keyspace_metadata_internal', side_effect=self.BadMetaException):
            self.cluster.refresh_keyspace_metadata(self.keyspace_name)
            m = self.cluster.metadata.keyspaces[self.keyspace_name]
            self.assertIs(m._exc_info[0], self.BadMetaException)
            self.assertIn("/*\nWarning:", m.export_as_string())

    def test_bad_table(self):
        self.session.execute('CREATE TABLE %s (k int PRIMARY KEY, v int)' % self.function_name)
        with patch.object(self.parser_class, '_build_column_metadata', side_effect=self.BadMetaException):
            self.cluster.refresh_table_metadata(self.keyspace_name, self.function_name)
            m = self.cluster.metadata.keyspaces[self.keyspace_name].tables[self.function_name]
            self.assertIs(m._exc_info[0], self.BadMetaException)
            self.assertIn("/*\nWarning:", m.export_as_string())

    def test_bad_index(self):
        self.session.execute('CREATE TABLE %s (k int PRIMARY KEY, v int)' % self.function_name)
        self.session.execute('CREATE INDEX ON %s(v)' % self.function_name)
        with patch.object(self.parser_class, '_build_index_metadata', side_effect=self.BadMetaException):
            self.cluster.refresh_table_metadata(self.keyspace_name, self.function_name)
            m = self.cluster.metadata.keyspaces[self.keyspace_name].tables[self.function_name]
            self.assertIs(m._exc_info[0], self.BadMetaException)
            self.assertIn("/*\nWarning:", m.export_as_string())

    @greaterthancass20
    def test_bad_user_type(self):
        self.session.execute('CREATE TYPE %s (i int, d double)' % self.function_name)
        with patch.object(self.parser_class, '_build_user_type', side_effect=self.BadMetaException):
            self.cluster.refresh_schema_metadata()   # presently do not capture these errors on udt direct refresh -- make sure it's contained during full refresh
            m = self.cluster.metadata.keyspaces[self.keyspace_name]
            self.assertIs(m._exc_info[0], self.BadMetaException)
            self.assertIn("/*\nWarning:", m.export_as_string())

    @greaterthancass21
    def test_bad_user_function(self):
        self.session.execute("""CREATE FUNCTION IF NOT EXISTS %s (key int, val int)
                                RETURNS NULL ON NULL INPUT
                                RETURNS int
                                LANGUAGE java AS 'return key + val;';""" % self.function_name)

        #We need to patch as well the reconnect function because after patching the _build_function
        #there will an Error refreshing schema which will trigger a reconnection. If this happened
        #in a timely manner in the call self.cluster.refresh_schema_metadata() it would return an exception
        #due to that a connection would be closed
        with patch.object(self.cluster.control_connection, 'reconnect'):
            with patch.object(self.parser_class, '_build_function', side_effect=self.BadMetaException):
                self.cluster.refresh_schema_metadata()   # presently do not capture these errors on udt direct refresh -- make sure it's contained during full refresh
                m = self.cluster.metadata.keyspaces[self.keyspace_name]
                self.assertIs(m._exc_info[0], self.BadMetaException)
                self.assertIn("/*\nWarning:", m.export_as_string())

    @greaterthancass21
    def test_bad_user_aggregate(self):
        self.session.execute("""CREATE FUNCTION IF NOT EXISTS sum_int (key int, val int)
                                RETURNS NULL ON NULL INPUT
                                RETURNS int
                                LANGUAGE java AS 'return key + val;';""")
        self.session.execute("""CREATE AGGREGATE %s(int)
                                 SFUNC sum_int
                                 STYPE int
                                 INITCOND 0""" % self.function_name)
        #We have the same issue here as in test_bad_user_function
        with patch.object(self.cluster.control_connection, 'reconnect'):
            with patch.object(self.parser_class, '_build_aggregate', side_effect=self.BadMetaException):
                self.cluster.refresh_schema_metadata()   # presently do not capture these errors on udt direct refresh -- make sure it's contained during full refresh
                m = self.cluster.metadata.keyspaces[self.keyspace_name]
                self.assertIs(m._exc_info[0], self.BadMetaException)
                self.assertIn("/*\nWarning:", m.export_as_string())


class DynamicCompositeTypeTest(BasicSharedKeyspaceUnitTestCase):

    def test_dct_alias(self):
        """
        Tests to make sure DCT's have correct string formatting

        Constructs a DCT and check the format as generated. To insure it matches what is expected

        @since 3.6.0
        @jira_ticket PYTHON-579
        @expected_result DCT subtypes should always have fully qualified names

        @test_category metadata
        """
        self.session.execute("CREATE TABLE {0}.{1} ("
                             "k int PRIMARY KEY,"
                             "c1 'DynamicCompositeType(s => UTF8Type, i => Int32Type)',"
                             "c2 Text)".format(self.ks_name, self.function_table_name))
        dct_table = self.cluster.metadata.keyspaces.get(self.ks_name).tables.get(self.function_table_name)

        # Format can very slightly between versions, strip out whitespace for consistency sake
        table_text = dct_table.as_cql_query().replace(" ", "")
        dynamic_type_text = "c1'org.apache.cassandra.db.marshal.DynamicCompositeType("
        self.assertIn("c1'org.apache.cassandra.db.marshal.DynamicCompositeType(", table_text)
        # Types within in the composite can come out in random order, so grab the type definition and find each one
        type_definition_start = table_text.index("(", table_text.find(dynamic_type_text))
        type_definition_end = table_text.index(")")
        type_definition_text = table_text[type_definition_start:type_definition_end]
        self.assertIn("s=>org.apache.cassandra.db.marshal.UTF8Type", type_definition_text)
        self.assertIn("i=>org.apache.cassandra.db.marshal.Int32Type", type_definition_text)


@greaterthanorequalcass30
class MaterializedViewMetadataTestSimple(BasicSharedKeyspaceUnitTestCase):

    def setUp(self):
        self.session.execute("CREATE TABLE {0}.{1} (pk int PRIMARY KEY, c int)".format(self.keyspace_name, self.function_table_name))
        self.session.execute(
            "CREATE MATERIALIZED VIEW {0}.mv1 AS SELECT pk, c FROM {0}.{1} "
            "WHERE pk IS NOT NULL AND c IS NOT NULL PRIMARY KEY (pk, c)".format(
                self.keyspace_name, self.function_table_name)
        )

    def tearDown(self):
        self.session.execute("DROP MATERIALIZED VIEW {0}.mv1".format(self.keyspace_name))
        self.session.execute("DROP TABLE {0}.{1}".format(self.keyspace_name, self.function_table_name))

    def test_materialized_view_metadata_creation(self):
        """
        test for materialized view metadata creation

        test_materialized_view_metadata_creation tests that materialized view metadata properly created implicitly in
        both keyspace and table metadata under "views". It creates a simple base table and then creates a view based
        on that table. It then checks that the materialized view metadata is contained in the keyspace and table
        metadata. Finally, it checks that the keyspace_name and the base_table_name in the view metadata is properly set.

        @since 3.0.0
        @jira_ticket PYTHON-371
        @expected_result Materialized view metadata in both the ks and table should be created with a new view is created.

        @test_category metadata
        """

        self.assertIn("mv1", self.cluster.metadata.keyspaces[self.keyspace_name].views)
        self.assertIn("mv1", self.cluster.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].views)

        self.assertEqual(self.keyspace_name, self.cluster.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].views["mv1"].keyspace_name)
        self.assertEqual(self.function_table_name, self.cluster.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].views["mv1"].base_table_name)

    def test_materialized_view_metadata_alter(self):
        """
        test for materialized view metadata alteration

        test_materialized_view_metadata_alter tests that materialized view metadata is properly updated implicitly in the
        table metadata once that view is updated. It creates a simple base table and then creates a view based
        on that table. It then alters that materalized view and checks that the materialized view metadata is altered in
        the table metadata.

        @since 3.0.0
        @jira_ticket PYTHON-371
        @expected_result Materialized view metadata should be updated with the view is altered.

        @test_category metadata
        """
        compaction = self.cluster.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].views["mv1"].options["compaction"]["class"]
        if HCD_VERSION:
            self.assertIn("UnifiedCompactionStrategy", compaction)
        else:
            self.assertIn("SizeTieredCompactionStrategy", compaction)

        self.session.execute("ALTER MATERIALIZED VIEW {0}.mv1 WITH compaction = {{ 'class' : 'LeveledCompactionStrategy' }}".format(self.keyspace_name))
        compaction = self.cluster.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].views["mv1"].options["compaction"]["class"]
        self.assertIn("LeveledCompactionStrategy", compaction)

    def test_materialized_view_metadata_drop(self):
        """
        test for materialized view metadata dropping

        test_materialized_view_metadata_drop tests that materialized view metadata is properly removed implicitly in
        both keyspace and table metadata once that view is dropped. It creates a simple base table and then creates a view
        based on that table. It then drops that materalized view and checks that the materialized view metadata is removed
        from the keyspace and table metadata.

        @since 3.0.0
        @jira_ticket PYTHON-371
        @expected_result Materialized view metadata in both the ks and table should be removed with the view is dropped.

        @test_category metadata
        """

        self.session.execute("DROP MATERIALIZED VIEW {0}.mv1".format(self.keyspace_name))

        self.assertNotIn("mv1", self.cluster.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].views)
        self.assertNotIn("mv1", self.cluster.metadata.keyspaces[self.keyspace_name].views)
        self.assertDictEqual({}, self.cluster.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].views)
        self.assertDictEqual({}, self.cluster.metadata.keyspaces[self.keyspace_name].views)

        self.session.execute(
            "CREATE MATERIALIZED VIEW {0}.mv1 AS SELECT pk, c FROM {0}.{1} "
            "WHERE pk IS NOT NULL AND c IS NOT NULL PRIMARY KEY (pk, c)".format(
                self.keyspace_name, self.function_table_name)
        )


@greaterthanorequalcass30
class MaterializedViewMetadataTestComplex(BasicSegregatedKeyspaceUnitTestCase):
    def test_create_view_metadata(self):
        """
        test to ensure that materialized view metadata is properly constructed

        test_create_view_metadata tests that materialized views metadata is properly constructed. It runs a simple
        query to construct a materialized view, then proceeds to inspect the metadata associated with that MV.
        Columns are inspected to insure that all are of the proper type, and in the proper type.

        @since 3.0.0
        @jira_ticket PYTHON-371
        @expected_result Materialized view metadata should be constructed appropriately.

        @test_category metadata
        """
        create_table = """CREATE TABLE {0}.scores(
                        user TEXT,
                        game TEXT,
                        year INT,
                        month INT,
                        day INT,
                        score INT,
                        PRIMARY KEY (user, game, year, month, day)
                        )""".format(self.keyspace_name)

        self.session.execute(create_table)

        create_mv = """CREATE MATERIALIZED VIEW {0}.monthlyhigh AS
                        SELECT game, year, month, score, user, day FROM {0}.scores
                        WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND day IS NOT NULL
                        PRIMARY KEY ((game, year, month), score, user, day)
                        WITH CLUSTERING ORDER BY (score DESC, user ASC, day ASC)""".format(self.keyspace_name)

        self.session.execute(create_mv)
        score_table = self.cluster.metadata.keyspaces[self.keyspace_name].tables['scores']
        mv = self.cluster.metadata.keyspaces[self.keyspace_name].views['monthlyhigh']

        self.assertIsNotNone(score_table.views["monthlyhigh"])
        self.assertIsNotNone(len(score_table.views), 1)

        # Make sure user is a partition key, and not null
        self.assertEqual(len(score_table.partition_key), 1)
        self.assertIsNotNone(score_table.columns['user'])
        self.assertTrue(score_table.columns['user'], score_table.partition_key[0])

        # Validate clustering keys
        self.assertEqual(len(score_table.clustering_key), 4)

        self.assertIsNotNone(score_table.columns['game'])
        self.assertTrue(score_table.columns['game'], score_table.clustering_key[0])

        self.assertIsNotNone(score_table.columns['year'])
        self.assertTrue(score_table.columns['year'], score_table.clustering_key[1])

        self.assertIsNotNone(score_table.columns['month'])
        self.assertTrue(score_table.columns['month'], score_table.clustering_key[2])

        self.assertIsNotNone(score_table.columns['day'])
        self.assertTrue(score_table.columns['day'], score_table.clustering_key[3])

        self.assertIsNotNone(score_table.columns['score'])

        # Validate basic mv information
        self.assertEqual(mv.keyspace_name, self.keyspace_name)
        self.assertEqual(mv.name, "monthlyhigh")
        self.assertEqual(mv.base_table_name, "scores")
        self.assertFalse(mv.include_all_columns)

        # Validate that all columns are preset and correct
        mv_columns = list(mv.columns.values())
        self.assertEqual(len(mv_columns), 6)

        game_column = mv_columns[0]
        self.assertIsNotNone(game_column)
        self.assertEqual(game_column.name, 'game')
        self.assertEqual(game_column, mv.partition_key[0])

        year_column = mv_columns[1]
        self.assertIsNotNone(year_column)
        self.assertEqual(year_column.name, 'year')
        self.assertEqual(year_column, mv.partition_key[1])

        month_column = mv_columns[2]
        self.assertIsNotNone(month_column)
        self.assertEqual(month_column.name, 'month')
        self.assertEqual(month_column, mv.partition_key[2])

        def compare_columns(a, b, name):
            self.assertEqual(a.name, name)
            self.assertEqual(a.name, b.name)
            self.assertEqual(a.table, b.table)
            self.assertEqual(a.cql_type, b.cql_type)
            self.assertEqual(a.is_static, b.is_static)
            self.assertEqual(a.is_reversed, b.is_reversed)

        score_column = mv_columns[3]
        compare_columns(score_column, mv.clustering_key[0], 'score')

        user_column = mv_columns[4]
        compare_columns(user_column, mv.clustering_key[1], 'user')

        day_column = mv_columns[5]
        compare_columns(day_column, mv.clustering_key[2], 'day')

    def test_base_table_column_addition_mv(self):
        """
        test to ensure that materialized view metadata is properly updated with base columns are added

        test_create_view_metadata tests that materialized views metadata is properly updated when columns are added to
        the base table.

        @since 3.0.0
        @jira_ticket PYTHON-419
        @expected_result Materialized view metadata should be updated correctly

        @test_category metadata
        """
        create_table = """CREATE TABLE {0}.scores(
                        user TEXT,
                        game TEXT,
                        year INT,
                        month INT,
                        day INT,
                        score TEXT,
                        PRIMARY KEY (user, game, year, month, day)
                        )""".format(self.keyspace_name)

        self.session.execute(create_table)

        create_mv = """CREATE MATERIALIZED VIEW {0}.monthlyhigh AS
                        SELECT game, year, month, score, user, day FROM {0}.scores
                        WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND day IS NOT NULL
                        PRIMARY KEY ((game, year, month), score, user, day)
                        WITH CLUSTERING ORDER BY (score DESC, user ASC, day ASC)""".format(self.keyspace_name)

        create_mv_alltime = """CREATE MATERIALIZED VIEW {0}.alltimehigh AS
                        SELECT * FROM {0}.scores
                        WHERE game IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL
                        PRIMARY KEY (game, score, user, year, month, day)
                        WITH CLUSTERING ORDER BY (score DESC, user ASC, year ASC, month ASC, day ASC)""".format(self.keyspace_name)

        self.session.execute(create_mv)

        self.session.execute(create_mv_alltime)

        score_table = self.cluster.metadata.keyspaces[self.keyspace_name].tables['scores']

        self.assertIsNotNone(score_table.views["monthlyhigh"])
        self.assertIsNotNone(score_table.views["alltimehigh"])
        self.assertEqual(len(self.cluster.metadata.keyspaces[self.keyspace_name].views), 2)

        insert_fouls = """ALTER TABLE {0}.scores ADD fouls INT""".format((self.keyspace_name))

        self.session.execute(insert_fouls)
        self.assertEqual(len(self.cluster.metadata.keyspaces[self.keyspace_name].views), 2)

        score_table = self.cluster.metadata.keyspaces[self.keyspace_name].tables['scores']
        self.assertIn("fouls", score_table.columns)

        # This is a workaround for mv notifications being separate from base table schema responses.
        # This maybe fixed with future protocol changes
        for i in range(10):
            mv_alltime = self.cluster.metadata.keyspaces[self.keyspace_name].views["alltimehigh"]
            if("fouls" in mv_alltime.columns):
                break
            time.sleep(.2)

        self.assertIn("fouls", mv_alltime.columns)

        mv_alltime_fouls_comumn = self.cluster.metadata.keyspaces[self.keyspace_name].views["alltimehigh"].columns['fouls']
        self.assertEqual(mv_alltime_fouls_comumn.cql_type, 'int')

    @lessthancass30
    def test_base_table_type_alter_mv(self):
        """
        test to ensure that materialized view metadata is properly updated when a type in the base table
        is updated.

        test_create_view_metadata tests that materialized views metadata is properly updated when the type of base table
        column is changed.

        Support for alter type was removed in CASSANDRA-12443

        @since 3.0.0
        @jira_ticket CASSANDRA-10424
        @expected_result Materialized view metadata should be updated correctly

        @test_category metadata
        """
        create_table = """CREATE TABLE {0}.scores(
                        user TEXT,
                        game TEXT,
                        year INT,
                        month INT,
                        day INT,
                        score TEXT,
                        PRIMARY KEY (user, game, year, month, day)
                        )""".format(self.keyspace_name)

        self.session.execute(create_table)

        create_mv = """CREATE MATERIALIZED VIEW {0}.monthlyhigh AS
                        SELECT game, year, month, score, user, day FROM {0}.scores
                        WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND day IS NOT NULL
                        PRIMARY KEY ((game, year, month), score, user, day)
                        WITH CLUSTERING ORDER BY (score DESC, user ASC, day ASC)""".format(self.keyspace_name)

        self.session.execute(create_mv)
        self.assertEqual(len(self.cluster.metadata.keyspaces[self.keyspace_name].views), 1)
        alter_scores = """ALTER TABLE {0}.scores ALTER score TYPE blob""".format((self.keyspace_name))
        self.session.execute(alter_scores)
        self.assertEqual(len(self.cluster.metadata.keyspaces[self.keyspace_name].views), 1)

        score_column = self.cluster.metadata.keyspaces[self.keyspace_name].tables['scores'].columns['score']
        self.assertEqual(score_column.cql_type, 'blob')

        # until CASSANDRA-9920+CASSANDRA-10500 MV updates are only available later with an async event
        for i in range(10):
            score_mv_column = self.cluster.metadata.keyspaces[self.keyspace_name].views["monthlyhigh"].columns['score']
            if "blob" == score_mv_column.cql_type:
                break
            time.sleep(.2)

        self.assertEqual(score_mv_column.cql_type, 'blob')

    def test_metadata_with_quoted_identifiers(self):
        """
        test to ensure that materialized view metadata is properly constructed when quoted identifiers are used

        test_metadata_with_quoted_identifiers tests that materialized views metadata is properly constructed.
        It runs a simple query to construct a materialized view, then proceeds to inspect the metadata associated with
        that MV. The caveat here is that the tables and the materialized view both have quoted identifiers
        Columns are inspected to insure that all are of the proper type, and in the proper type.

        @since 3.0.0
        @jira_ticket PYTHON-371
        @expected_result Materialized view metadata should be constructed appropriately even with quoted identifiers.

        @test_category metadata
        """
        create_table = """CREATE TABLE {0}.t1 (
                        "theKey" int,
                        "the;Clustering" int,
                        "the Value" int,
                        PRIMARY KEY ("theKey", "the;Clustering"))""".format(self.keyspace_name)

        self.session.execute(create_table)

        create_mv = """CREATE MATERIALIZED VIEW {0}.mv1 AS
                    SELECT "theKey", "the;Clustering", "the Value"
                    FROM {0}.t1
                    WHERE "theKey" IS NOT NULL AND "the;Clustering" IS NOT NULL AND "the Value" IS NOT NULL
                    PRIMARY KEY ("theKey", "the;Clustering")""".format(self.keyspace_name)

        self.session.execute(create_mv)

        t1_table = self.cluster.metadata.keyspaces[self.keyspace_name].tables['t1']
        mv = self.cluster.metadata.keyspaces[self.keyspace_name].views['mv1']

        self.assertIsNotNone(t1_table.views["mv1"])
        self.assertIsNotNone(len(t1_table.views), 1)

        # Validate partition key, and not null
        self.assertEqual(len(t1_table.partition_key), 1)
        self.assertIsNotNone(t1_table.columns['theKey'])
        self.assertTrue(t1_table.columns['theKey'], t1_table.partition_key[0])

        # Validate clustering key column
        self.assertEqual(len(t1_table.clustering_key), 1)
        self.assertIsNotNone(t1_table.columns['the;Clustering'])
        self.assertTrue(t1_table.columns['the;Clustering'], t1_table.clustering_key[0])

        # Validate regular column
        self.assertIsNotNone(t1_table.columns['the Value'])

        # Validate basic mv information
        self.assertEqual(mv.keyspace_name, self.keyspace_name)
        self.assertEqual(mv.name, "mv1")
        self.assertEqual(mv.base_table_name, "t1")
        self.assertFalse(mv.include_all_columns)

        # Validate that all columns are preset and correct
        mv_columns = list(mv.columns.values())
        self.assertEqual(len(mv_columns), 3)

        theKey_column = mv_columns[0]
        self.assertIsNotNone(theKey_column)
        self.assertEqual(theKey_column.name, 'theKey')
        self.assertEqual(theKey_column, mv.partition_key[0])

        cluster_column = mv_columns[1]
        self.assertIsNotNone(cluster_column)
        self.assertEqual(cluster_column.name, 'the;Clustering')
        self.assertEqual(cluster_column.name, mv.clustering_key[0].name)
        self.assertEqual(cluster_column.table, mv.clustering_key[0].table)
        self.assertEqual(cluster_column.is_static, mv.clustering_key[0].is_static)
        self.assertEqual(cluster_column.is_reversed, mv.clustering_key[0].is_reversed)

        value_column = mv_columns[2]
        self.assertIsNotNone(value_column)
        self.assertEqual(value_column.name, 'the Value')

    @greaterthanorequaldse51
    def test_dse_workloads(self):
        """
        Test to ensure dse_workloads is populated appropriately.
        Field added in DSE 5.1

        @jira_ticket PYTHON-667
        @expected_result dse_workloads set is set on host model

        @test_category metadata
        """
        for host in self.cluster.metadata.all_hosts():
            self.assertIsInstance(host.dse_workloads, SortedSet)
            self.assertIn("Cassandra", host.dse_workloads)


class GroupPerHost(BasicSharedKeyspaceUnitTestCase):
    @classmethod
    def setUpClass(cls):
        cls.common_setup(rf=1, create_class_table=True)
        cls.table_two_pk = "table_with_two_pk"
        cls.session.execute(
            '''
            CREATE TABLE {0}.{1} (
                k_one int,
                k_two int,
                v int,
                PRIMARY KEY ((k_one, k_two))
            )'''.format(cls.ks_name, cls.table_two_pk)
        )

    def test_group_keys_by_host(self):
        """
        Test to ensure group_keys_by_host functions as expected. It is tried
        with a table with a single field for the partition key and a table
        with two fields for the partition key
        @since 3.13
        @jira_ticket PYTHON-647
        @expected_result group_keys_by_host return the expected value

        @test_category metadata
        """
        stmt = """SELECT * FROM {}.{}
                         WHERE k_one = ? AND k_two = ? """.format(self.ks_name, self.table_two_pk)
        keys = ((1, 2), (2, 2), (2, 3), (3, 4))
        self._assert_group_keys_by_host(keys, self.table_two_pk, stmt)

        stmt = """SELECT * FROM {}.{}
                                 WHERE k = ? """.format(self.ks_name, self.ks_name)
        keys = ((1,), (2,), (2,), (3,))
        self._assert_group_keys_by_host(keys, self.ks_name, stmt)

    def _assert_group_keys_by_host(self, keys, table_name, stmt):
        keys_per_host = group_keys_by_replica(self.session, self.ks_name, table_name, keys)
        self.assertNotIn(NO_VALID_REPLICA, keys_per_host)

        prepared_stmt = self.session.prepare(stmt)
        for key in keys:
            routing_key = prepared_stmt.bind(key).routing_key
            hosts = self.cluster.metadata.get_replicas(self.ks_name, routing_key)
            self.assertEqual(1, len(hosts))  # RF is 1 for this keyspace
            self.assertIn(key, keys_per_host[hosts[0]])


class VirtualKeypaceTest(BasicSharedKeyspaceUnitTestCase):
    virtual_ks_names = ('system_virtual_schema', 'system_views')

    def test_existing_keyspaces_have_correct_virtual_tags(self):
        for name, ks in self.cluster.metadata.keyspaces.items():
            if name in self.virtual_ks_names:
                self.assertTrue(
                    ks.virtual,
                    'incorrect .virtual value for {}'.format(name)
                )
            else:
                self.assertFalse(
                    ks.virtual,
                    'incorrect .virtual value for {}'.format(name)
                )

    @greaterthanorequalcass40
    @greaterthanorequaldse67
    def test_expected_keyspaces_exist_and_are_virtual(self):
        for name in self.virtual_ks_names:
            self.assertTrue(
                self.cluster.metadata.keyspaces[name].virtual,
                'incorrect .virtual value for {}'.format(name)
            )

    @greaterthanorequalcass40
    @greaterthanorequaldse67
    def test_virtual_keyspaces_have_expected_schema_structure(self):
        self.maxDiff = None

        ingested_virtual_ks_structure = defaultdict(dict)
        for ks_name, ks in self.cluster.metadata.keyspaces.items():
            if not ks.virtual:
                continue
            for tab_name, tab in ks.tables.items():
                ingested_virtual_ks_structure[ks_name][tab_name] = set(
                    tab.columns.keys()
                )

        # Identify a couple known values to verify we parsed the structure correctly
        self.assertIn('table_name', ingested_virtual_ks_structure['system_virtual_schema']['tables'])
        self.assertIn('type', ingested_virtual_ks_structure['system_virtual_schema']['columns'])
        self.assertIn('total', ingested_virtual_ks_structure['system_views']['sstable_tasks'])
