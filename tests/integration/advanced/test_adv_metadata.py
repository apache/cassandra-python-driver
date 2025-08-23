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

from packaging.version import Version

from tests.integration import (BasicExistingKeyspaceUnitTestCase, BasicSharedKeyspaceUnitTestCase,
                               BasicSharedKeyspaceUnitTestCaseRF1,
                               greaterthanorequaldse51, greaterthanorequaldse60,
                               greaterthanorequaldse68, use_single_node,
                               DSE_VERSION, requiredse, TestCluster)

import unittest

import logging
import time


log = logging.getLogger(__name__)


def setup_module():
    if DSE_VERSION:
        use_single_node()


@requiredse
@greaterthanorequaldse60
class FunctionAndAggregateMetadataTests(BasicSharedKeyspaceUnitTestCaseRF1):

    @classmethod
    def setUpClass(cls):
        if DSE_VERSION:
            super(FunctionAndAggregateMetadataTests, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        if DSE_VERSION:
            super(FunctionAndAggregateMetadataTests, cls).setUpClass()

    def setUp(self):
        self.func_name = self.function_table_name + '_func'
        self.agg_name = self.function_table_name + '_agg(int)'

    def _populated_ks_meta_attr(self, attr_name):
        val, start_time = None, time.time()
        while not val:
            self.cluster.refresh_schema_metadata()
            val = getattr(self.cluster.metadata.keyspaces[self.keyspace_name],
                          attr_name)
            self.assertLess(time.time(), start_time + 30,
                            'did not see func in metadata in 30s')
        log.debug('done blocking; dict is populated: {}'.format(val))
        return val

    def test_monotonic_on_and_deterministic_function(self):
        self.session.execute("""
            CREATE FUNCTION {ksn}.{ftn}(key int, val int)
            RETURNS NULL ON NULL INPUT
            RETURNS int
            DETERMINISTIC
            MONOTONIC ON val
            LANGUAGE java AS 'return key+val;';
        """.format(ksn=self.keyspace_name,
                   ftn=self.func_name))
        fn = self._populated_ks_meta_attr('functions')[
            '{}(int,int)'.format(self.func_name)
        ]
        self.assertEqual(fn.monotonic_on, ['val'])
        # monotonic is not set by MONOTONIC ON
        self.assertFalse(fn.monotonic)
        self.assertTrue(fn.deterministic)
        self.assertEqual('CREATE FUNCTION {ksn}.{ftn}(key int, val int) '
                         'RETURNS NULL ON NULL INPUT '
                         'RETURNS int DETERMINISTIC MONOTONIC ON val '
                         'LANGUAGE java AS $$return key+val;$$'
                         ''.format(ksn=self.keyspace_name,
                                   ftn=self.func_name),
                         fn.as_cql_query())
        self.session.execute('DROP FUNCTION {}.{}'.format(self.keyspace_name,
                                                          self.func_name))
        self.session.execute(fn.as_cql_query())

    def test_monotonic_all_and_nondeterministic_function(self):
        self.session.execute("""
            CREATE FUNCTION {ksn}.{ftn}(key int, val int)
            RETURNS NULL ON NULL INPUT
            RETURNS int
            MONOTONIC
            LANGUAGE java AS 'return key+val;';
        """.format(ksn=self.keyspace_name,
                   ftn=self.func_name))
        fn = self._populated_ks_meta_attr('functions')[
            '{}(int,int)'.format(self.func_name)
        ]
        self.assertEqual(set(fn.monotonic_on), {'key', 'val'})
        self.assertTrue(fn.monotonic)
        self.assertFalse(fn.deterministic)
        self.assertEqual('CREATE FUNCTION {ksn}.{ftn}(key int, val int) '
                         'RETURNS NULL ON NULL INPUT RETURNS int MONOTONIC '
                         'LANGUAGE java AS $$return key+val;$$'
                         ''.format(ksn=self.keyspace_name,
                                   ftn=self.func_name),
                         fn.as_cql_query())
        self.session.execute('DROP FUNCTION {}.{}'.format(self.keyspace_name,
                                                          self.func_name))
        self.session.execute(fn.as_cql_query())

    def _create_func_for_aggregate(self):
        self.session.execute("""
            CREATE FUNCTION {ksn}.{ftn}(key int, val int)
            RETURNS NULL ON NULL INPUT
            RETURNS int
            DETERMINISTIC
            LANGUAGE java AS 'return key+val;';
        """.format(ksn=self.keyspace_name,
                   ftn=self.func_name))

    def test_deterministic_aggregate(self):
        self._create_func_for_aggregate()
        self.session.execute("""
            CREATE AGGREGATE {ksn}.{an}
            SFUNC {ftn}
            STYPE int
            INITCOND 0
            DETERMINISTIC
        """.format(ksn=self.keyspace_name,
                   ftn=self.func_name,
                   an=self.agg_name))
        ag = self._populated_ks_meta_attr('aggregates')[self.agg_name]
        self.assertTrue(ag.deterministic)
        self.assertEqual(
            'CREATE AGGREGATE {ksn}.{an} SFUNC '
            '{ftn} STYPE int INITCOND 0 DETERMINISTIC'
            ''.format(ksn=self.keyspace_name,
                      ftn=self.func_name,
                      an=self.agg_name),
            ag.as_cql_query())
        self.session.execute('DROP AGGREGATE {}.{}'.format(self.keyspace_name,
                                                           self.agg_name))
        self.session.execute(ag.as_cql_query())

    def test_nondeterministic_aggregate(self):
        self._create_func_for_aggregate()
        self.session.execute("""
            CREATE AGGREGATE {ksn}.{an}
            SFUNC {ftn}
            STYPE int
            INITCOND 0
        """.format(ksn=self.keyspace_name,
                   ftn=self.func_name,
                   an=self.agg_name))
        ag = self._populated_ks_meta_attr('aggregates')[self.agg_name]
        self.assertFalse(ag.deterministic)
        self.assertEqual(
            'CREATE AGGREGATE {ksn}.{an} SFUNC '
            '{ftn} STYPE int INITCOND 0'
            ''.format(ksn=self.keyspace_name,
                      ftn=self.func_name,
                      an=self.agg_name),
            ag.as_cql_query())
        self.session.execute('DROP AGGREGATE {}.{}'.format(self.keyspace_name,
                                                           self.agg_name))
        self.session.execute(ag.as_cql_query())


@requiredse
class RLACMetadataTests(BasicSharedKeyspaceUnitTestCase):

    @classmethod
    def setUpClass(cls):
        if DSE_VERSION:
            super(RLACMetadataTests, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        if DSE_VERSION:
            super(RLACMetadataTests, cls).setUpClass()

    @greaterthanorequaldse51
    def test_rlac_on_table(self):
        """
        Checks to ensure that the RLAC table extension appends the proper cql on export to tables

        @since 3.20
        @jira_ticket PYTHON-638
        @expected_result Invalid hosts on the contact list should be excluded

        @test_category metadata
        """
        self.session.execute("CREATE TABLE {0}.reports ("
                             " report_user text, "
                             " report_number int, "
                             " report_month int, "
                             " report_year int, "
                             " report_text text,"
                             " PRIMARY KEY (report_user, report_number))".format(self.keyspace_name))
        restrict_cql = "RESTRICT ROWS ON {0}.reports USING report_user".format(self.keyspace_name)
        self.session.execute(restrict_cql)
        table_meta = self.cluster.metadata.keyspaces[self.keyspace_name].tables['reports']
        self.assertTrue(restrict_cql in table_meta.export_as_string())

    @unittest.skip("Dse 5.1 doesn't support MV and RLAC remove after update")
    @greaterthanorequaldse51
    def test_rlac_on_mv(self):
        """
        Checks to ensure that the RLAC table extension appends the proper cql to export on mV's

        @since 3.20
        @jira_ticket PYTHON-682
        @expected_result Invalid hosts on the contact list should be excluded

        @test_category metadata
        """
        self.session.execute("CREATE TABLE {0}.reports2 ("
                             " report_user text, "
                             " report_number int, "
                             " report_month int, "
                             " report_year int, "
                             " report_text text,"
                             " PRIMARY KEY (report_user, report_number))".format(self.keyspace_name))
        self.session.execute("CREATE MATERIALIZED VIEW {0}.reports_by_year AS "
                             " SELECT report_year, report_user, report_number, report_text FROM {0}.reports2 "
                             " WHERE report_user IS NOT NULL AND report_number IS NOT NULL AND report_year IS NOT NULL "
                             " PRIMARY KEY ((report_year, report_user), report_number)".format(self.keyspace_name))

        restrict_cql_table = "RESTRICT ROWS ON {0}.reports2 USING report_user".format(self.keyspace_name)
        self.session.execute(restrict_cql_table)
        restrict_cql_view = "RESTRICT ROWS ON {0}.reports_by_year USING report_user".format(self.keyspace_name)
        self.session.execute(restrict_cql_view)
        table_cql = self.cluster.metadata.keyspaces[self.keyspace_name].tables['reports2'].export_as_string()
        view_cql = self.cluster.metadata.keyspaces[self.keyspace_name].tables['reports2'].views["reports_by_year"].export_as_string()
        self.assertTrue(restrict_cql_table in table_cql)
        self.assertTrue(restrict_cql_view in table_cql)
        self.assertTrue(restrict_cql_view in view_cql)
        self.assertTrue(restrict_cql_table not in view_cql)


@requiredse
class NodeSyncMetadataTests(BasicSharedKeyspaceUnitTestCase):

    @classmethod
    def setUpClass(cls):
        if DSE_VERSION:
            super(NodeSyncMetadataTests, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        if DSE_VERSION:
            super(NodeSyncMetadataTests, cls).setUpClass()

    @greaterthanorequaldse60
    def test_nodesync_on_table(self):
        """
        Checks to ensure that nodesync is visible through driver metadata

        @since 3.20
        @jira_ticket PYTHON-799
        @expected_result nodesync should be enabled

        @test_category metadata
        """
        self.session.execute("CREATE TABLE {0}.reports ("
                             " report_user text PRIMARY KEY"
                             ") WITH  nodesync = {{"
                             "'enabled': 'true', 'deadline_target_sec' : 86400 }};".format(
                                 self.keyspace_name
                             ))
        table_meta = self.cluster.metadata.keyspaces[self.keyspace_name].tables['reports']
        self.assertIn('nodesync =', table_meta.export_as_string())
        self.assertIn('nodesync', table_meta.options)


@greaterthanorequaldse68
class GraphMetadataTests(BasicExistingKeyspaceUnitTestCase):
    """
    Various tests to ensure that graph metadata are visible through driver metadata
    @since DSE6.8
    @jira_ticket PYTHON-996
    @expected_result graph metadata are fetched
    @test_category metadata
    """

    @classmethod
    def setUpClass(cls):
        if DSE_VERSION and DSE_VERSION >= Version('6.8'):
            super(GraphMetadataTests, cls).setUpClass()
            cls.session.execute("""
            CREATE KEYSPACE ks_no_graph_engine WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
            """)
            cls.session.execute("""
            CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} and graph_engine = 'Core';
            """ % (cls.ks_name,))

            cls.session.execute("""
            CREATE TABLE %s.person (name text PRIMARY KEY) WITH VERTEX LABEL;
            """ % (cls.ks_name,))

            cls.session.execute("""
            CREATE TABLE %s.software(company  text, name text, version int, PRIMARY KEY((company, name), version)) WITH VERTEX LABEL rocksolidsoftware;
            """ % (cls.ks_name,))

            cls.session.execute("""
              CREATE TABLE %s.contributors (contributor text, company_name text, software_name text, software_version int, 
              PRIMARY KEY (contributor, company_name, software_name, software_version) ) 
              WITH CLUSTERING ORDER BY (company_name ASC, software_name ASC, software_version ASC)
              AND EDGE LABEL contrib FROM person(contributor) TO rocksolidsoftware((company_name, software_name), software_version);
            """ % (cls.ks_name,))

    @classmethod
    def tearDownClass(cls):
        if DSE_VERSION and DSE_VERSION >= Version('6.8'):
            cls.session.execute('DROP KEYSPACE {0}'.format('ks_no_graph_engine'))
            cls.session.execute('DROP KEYSPACE {0}'.format(cls.ks_name))
            cls.cluster.shutdown()

    def test_keyspace_metadata(self):
        self.assertIsNone(self.cluster.metadata.keyspaces['ks_no_graph_engine'].graph_engine, None)
        self.assertEqual(self.cluster.metadata.keyspaces[self.ks_name].graph_engine, 'Core')

    def test_keyspace_metadata_alter_graph_engine(self):
        self.session.execute("ALTER KEYSPACE %s WITH graph_engine = 'Tinker'" % (self.ks_name,))
        self.assertEqual(self.cluster.metadata.keyspaces[self.ks_name].graph_engine, 'Tinker')
        self.session.execute("ALTER KEYSPACE %s WITH graph_engine = 'Core'" % (self.ks_name,))
        self.assertEqual(self.cluster.metadata.keyspaces[self.ks_name].graph_engine, 'Core')

    def test_vertex_metadata(self):
        vertex_meta = self.cluster.metadata.keyspaces[self.ks_name].tables['person'].vertex
        self.assertEqual(vertex_meta.keyspace_name, self.ks_name)
        self.assertEqual(vertex_meta.table_name, 'person')
        self.assertEqual(vertex_meta.label_name, 'person')

        vertex_meta = self.cluster.metadata.keyspaces[self.ks_name].tables['software'].vertex
        self.assertEqual(vertex_meta.keyspace_name, self.ks_name)
        self.assertEqual(vertex_meta.table_name, 'software')
        self.assertEqual(vertex_meta.label_name, 'rocksolidsoftware')

    def test_edge_metadata(self):
        edge_meta = self.cluster.metadata.keyspaces[self.ks_name].tables['contributors'].edge
        self.assertEqual(edge_meta.keyspace_name, self.ks_name)
        self.assertEqual(edge_meta.table_name, 'contributors')
        self.assertEqual(edge_meta.label_name, 'contrib')
        self.assertEqual(edge_meta.from_table, 'person')
        self.assertEqual(edge_meta.from_label, 'person')
        self.assertEqual(edge_meta.from_partition_key_columns, ['contributor'])
        self.assertEqual(edge_meta.from_clustering_columns, [])
        self.assertEqual(edge_meta.to_table, 'software')
        self.assertEqual(edge_meta.to_label, 'rocksolidsoftware')
        self.assertEqual(edge_meta.to_partition_key_columns, ['company_name', 'software_name'])
        self.assertEqual(edge_meta.to_clustering_columns, ['software_version'])


@greaterthanorequaldse68
class GraphMetadataSchemaErrorTests(BasicExistingKeyspaceUnitTestCase):
    """
    Test that we can connect when the graph schema is broken.
    """

    def test_connection_on_graph_schema_error(self):
        self.session = self.cluster.connect()

        self.session.execute("""
        CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} and graph_engine = 'Core';
        """ % (self.ks_name,))

        self.session.execute("""
        CREATE TABLE %s.person (name text PRIMARY KEY) WITH VERTEX LABEL;
        """ % (self.ks_name,))

        self.session.execute("""
        CREATE TABLE %s.software(company  text, name text, version int, PRIMARY KEY((company, name), version)) WITH VERTEX LABEL rocksolidsoftware;
        """ % (self.ks_name,))

        self.session.execute("""
        CREATE TABLE %s.contributors (contributor text, company_name text, software_name text, software_version int,
        PRIMARY KEY (contributor, company_name, software_name, software_version) )
        WITH CLUSTERING ORDER BY (company_name ASC, software_name ASC, software_version ASC)
        AND EDGE LABEL contrib FROM person(contributor) TO rocksolidsoftware((company_name, software_name), software_version);
        """ % (self.ks_name,))

        self.session.execute('TRUNCATE system_schema.vertices')
        TestCluster().connect().shutdown()
