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


from tests.integration import use_singledc, PROTOCOL_VERSION, TestCluster, CASSANDRA_VERSION

import unittest

from packaging.version import Version

from cassandra import InvalidRequest, DriverException

from cassandra import ConsistencyLevel, ProtocolVersion
from cassandra.query import PreparedStatement, UNSET_VALUE
from tests.integration import (get_server_versions, greaterthanorequalcass40, greaterthanorequaldse50,
    requirecassandra, BasicSharedKeyspaceUnitTestCase)

import logging


LOG = logging.getLogger(__name__)


def setup_module():
    use_singledc()


class PreparedStatementTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cass_version = get_server_versions()

    def setUp(self):
        self.cluster = TestCluster(metrics_enabled=True, allow_beta_protocol_version=True)
        self.session = self.cluster.connect()

    def tearDown(self):
        self.cluster.shutdown()

    def test_basic(self):
        """
        Test basic PreparedStatement usage
        """
        self.session.execute(
            """
            DROP KEYSPACE IF EXISTS preparedtests
            """
        )
        self.session.execute(
            """
            CREATE KEYSPACE preparedtests
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)

        self.session.set_keyspace("preparedtests")
        self.session.execute(
            """
            CREATE TABLE cf0 (
                a text,
                b text,
                c text,
                PRIMARY KEY (a, b)
            )
            """)

        prepared = self.session.prepare(
            """
            INSERT INTO cf0 (a, b, c) VALUES  (?, ?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind(('a', 'b', 'c'))

        self.session.execute(bound)

        prepared = self.session.prepare(
            """
            SELECT * FROM cf0 WHERE a=?
            """)
        self.assertIsInstance(prepared, PreparedStatement)

        bound = prepared.bind(('a'))
        results = self.session.execute(bound)
        self.assertEqual(results, [('a', 'b', 'c')])

        # test with new dict binding
        prepared = self.session.prepare(
            """
            INSERT INTO cf0 (a, b, c) VALUES  (?, ?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind({
            'a': 'x',
            'b': 'y',
            'c': 'z'
        })

        self.session.execute(bound)

        prepared = self.session.prepare(
            """
            SELECT * FROM cf0 WHERE a=?
            """)

        self.assertIsInstance(prepared, PreparedStatement)

        bound = prepared.bind({'a': 'x'})
        results = self.session.execute(bound)
        self.assertEqual(results, [('x', 'y', 'z')])

    def test_missing_primary_key(self):
        """
        Ensure an InvalidRequest is thrown
        when prepared statements are missing the primary key
        """

        self._run_missing_primary_key(self.session)

    def _run_missing_primary_key(self, session):
        statement_to_prepare = """INSERT INTO test3rf.test (v) VALUES  (?)"""
        # logic needed work with changes in CASSANDRA-6237
        if self.cass_version[0] >= (3, 0, 0):
            self.assertRaises(InvalidRequest, session.prepare, statement_to_prepare)
        else:
            prepared = session.prepare(statement_to_prepare)
            self.assertIsInstance(prepared, PreparedStatement)
            bound = prepared.bind((1,))
            self.assertRaises(InvalidRequest, session.execute, bound)

    def test_missing_primary_key_dicts(self):
        """
        Ensure an InvalidRequest is thrown
        when prepared statements are missing the primary key
        with dict bindings
        """
        self._run_missing_primary_key_dicts(self.session)

    def _run_missing_primary_key_dicts(self, session):
        statement_to_prepare = """ INSERT INTO test3rf.test (v) VALUES  (?)"""
        # logic needed work with changes in CASSANDRA-6237
        if self.cass_version[0] >= (3, 0, 0):
            self.assertRaises(InvalidRequest, session.prepare, statement_to_prepare)
        else:
            prepared = session.prepare(statement_to_prepare)
            self.assertIsInstance(prepared, PreparedStatement)
            bound = prepared.bind({'v': 1})
            self.assertRaises(InvalidRequest, session.execute, bound)

    def test_too_many_bind_values(self):
        """
        Ensure a ValueError is thrown when attempting to bind too many variables
        """
        self._run_too_many_bind_values(self.session)

    def _run_too_many_bind_values(self, session):
        statement_to_prepare = """ INSERT INTO test3rf.test (v) VALUES  (?)"""
         # logic needed work with changes in CASSANDRA-6237
        if self.cass_version[0] >= (3, 0, 0):
            self.assertRaises(InvalidRequest, session.prepare, statement_to_prepare)
        else:
            prepared = session.prepare(statement_to_prepare)
            self.assertIsInstance(prepared, PreparedStatement)
            self.assertRaises(ValueError, prepared.bind, (1, 2))

    def test_imprecise_bind_values_dicts(self):
        """
        Ensure an error is thrown when attempting to bind the wrong values
        with dict bindings
        """

        prepared = self.session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)

        # too many values is ok - others are ignored
        prepared.bind({'k': 1, 'v': 2, 'v2': 3})

        # right number, but one does not belong
        if PROTOCOL_VERSION < 4:
            # pre v4, the driver bails with key error when 'v' is found missing
            self.assertRaises(KeyError, prepared.bind, {'k': 1, 'v2': 3})
        else:
            # post v4, the driver uses UNSET_VALUE for 'v' and 'v2' is ignored
            prepared.bind({'k': 1, 'v2': 3})

        # also catch too few variables with dicts
        self.assertIsInstance(prepared, PreparedStatement)
        if PROTOCOL_VERSION < 4:
            self.assertRaises(KeyError, prepared.bind, {})
        else:
            # post v4, the driver attempts to use UNSET_VALUE for unspecified keys
            self.assertRaises(ValueError, prepared.bind, {})

    def test_none_values(self):
        """
        Ensure binding None is handled correctly
        """

        prepared = self.session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind((1, None))
        self.session.execute(bound)

        prepared = self.session.prepare(
            """
            SELECT * FROM test3rf.test WHERE k=?
            """)
        self.assertIsInstance(prepared, PreparedStatement)

        bound = prepared.bind((1,))
        results = self.session.execute(bound)
        self.assertEqual(results[0].v, None)

    def test_unset_values(self):
        """
        Test to validate that UNSET_VALUEs are bound, and have the expected effect

        Prepare a statement and insert all values. Then follow with execute excluding
        parameters. Verify that the original values are unaffected.

        @since 2.6.0

        @jira_ticket PYTHON-317
        @expected_result UNSET_VALUE is implicitly added to bind parameters, and properly encoded, leaving unset values unaffected.

        @test_category prepared_statements:binding
        """
        if PROTOCOL_VERSION < 4:
            raise unittest.SkipTest("Binding UNSET values is not supported in protocol version < 4")

        # table with at least two values so one can be used as a marker
        self.session.execute("CREATE TABLE IF NOT EXISTS test1rf.test_unset_values (k int PRIMARY KEY, v0 int, v1 int)")
        insert = self.session.prepare("INSERT INTO test1rf.test_unset_values (k, v0, v1) VALUES  (?, ?, ?)")
        select = self.session.prepare("SELECT * FROM test1rf.test_unset_values WHERE k=?")

        bind_expected = [
            # initial condition
            ((0, 0, 0),                            (0, 0, 0)),
            # unset implicit
            ((0, 1,),                              (0, 1, 0)),
            ({'k': 0, 'v0': 2},                    (0, 2, 0)),
            ({'k': 0, 'v1': 1},                    (0, 2, 1)),
            # unset explicit
            ((0, 3, UNSET_VALUE),                  (0, 3, 1)),
            ((0, UNSET_VALUE, 2),                  (0, 3, 2)),
            ({'k': 0, 'v0': 4, 'v1': UNSET_VALUE}, (0, 4, 2)),
            ({'k': 0, 'v0': UNSET_VALUE, 'v1': 3}, (0, 4, 3)),
            # nulls still work
            ((0, None, None),                      (0, None, None)),
        ]

        for params, expected in bind_expected:
            self.session.execute(insert, params)
            results = self.session.execute(select, (0,))
            self.assertEqual(results[0], expected)

        self.assertRaises(ValueError, self.session.execute, select, (UNSET_VALUE, 0, 0))

    def test_no_meta(self):

        prepared = self.session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (0, 0)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind(None)
        bound.consistency_level = ConsistencyLevel.ALL
        self.session.execute(bound)

        prepared = self.session.prepare(
            """
            SELECT * FROM test3rf.test WHERE k=0
            """)
        self.assertIsInstance(prepared, PreparedStatement)

        bound = prepared.bind(None)
        bound.consistency_level = ConsistencyLevel.ALL
        results = self.session.execute(bound)
        self.assertEqual(results[0].v, 0)

    def test_none_values_dicts(self):
        """
        Ensure binding None is handled correctly with dict bindings
        """

        # test with new dict binding
        prepared = self.session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        bound = prepared.bind({'k': 1, 'v': None})
        self.session.execute(bound)

        prepared = self.session.prepare(
            """
            SELECT * FROM test3rf.test WHERE k=?
            """)
        self.assertIsInstance(prepared, PreparedStatement)

        bound = prepared.bind({'k': 1})
        results = self.session.execute(bound)
        self.assertEqual(results[0].v, None)

    def test_async_binding(self):
        """
        Ensure None binding over async queries
        """

        prepared = self.session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        future = self.session.execute_async(prepared, (873, None))
        future.result()

        prepared = self.session.prepare(
            """
            SELECT * FROM test3rf.test WHERE k=?
            """)
        self.assertIsInstance(prepared, PreparedStatement)

        future = self.session.execute_async(prepared, (873,))
        results = future.result()
        self.assertEqual(results[0].v, None)

    def test_async_binding_dicts(self):
        """
        Ensure None binding over async queries with dict bindings
        """
        prepared = self.session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        self.assertIsInstance(prepared, PreparedStatement)
        future = self.session.execute_async(prepared, {'k': 873, 'v': None})
        future.result()

        prepared = self.session.prepare(
            """
            SELECT * FROM test3rf.test WHERE k=?
            """)
        self.assertIsInstance(prepared, PreparedStatement)

        future = self.session.execute_async(prepared, {'k': 873})
        results = future.result()
        self.assertEqual(results[0].v, None)

    def test_raise_error_on_prepared_statement_execution_dropped_table(self):
        """
        test for error in executing prepared statement on a dropped table

        test_raise_error_on_execute_prepared_statement_dropped_table tests that an InvalidRequest is raised when a
        prepared statement is executed after its corresponding table is dropped. This happens because if a prepared
        statement is invalid, the driver attempts to automatically re-prepare it on a non-existing table.

        @expected_errors InvalidRequest If a prepared statement is executed on a dropped table

        @since 2.6.0
        @jira_ticket PYTHON-207
        @expected_result InvalidRequest error should be raised upon prepared statement execution.

        @test_category prepared_statements
        """

        self.session.execute("CREATE TABLE test3rf.error_test (k int PRIMARY KEY, v int)")
        prepared = self.session.prepare("SELECT * FROM test3rf.error_test WHERE k=?")
        self.session.execute("DROP TABLE test3rf.error_test")

        with self.assertRaises(InvalidRequest):
            self.session.execute(prepared, [0])

    @unittest.skipIf((CASSANDRA_VERSION >= Version('3.11.12') and CASSANDRA_VERSION < Version('4.0')) or \
        CASSANDRA_VERSION >= Version('4.0.2'),
        "Fixed server-side in Cassandra 3.11.12, 4.0.2")
    def test_fail_if_different_query_id_on_reprepare(self):
        """ PYTHON-1124 and CASSANDRA-15252 """
        keyspace = "test_fail_if_different_query_id_on_reprepare"
        self.session.execute(
            "CREATE KEYSPACE IF NOT EXISTS {} WITH replication = "
            "{{'class': 'SimpleStrategy', 'replication_factor': 1}}".format(keyspace)
        )
        self.session.execute("CREATE TABLE IF NOT EXISTS {}.foo(k int PRIMARY KEY)".format(keyspace))
        prepared = self.session.prepare("SELECT * FROM {}.foo WHERE k=?".format(keyspace))
        self.session.execute("DROP TABLE {}.foo".format(keyspace))
        self.session.execute("CREATE TABLE {}.foo(k int PRIMARY KEY)".format(keyspace))
        self.session.execute("USE {}".format(keyspace))
        with self.assertRaises(DriverException) as e:
            self.session.execute(prepared, [0])
        self.assertIn("ID mismatch", str(e.exception))


@greaterthanorequalcass40
class PreparedStatementInvalidationTest(BasicSharedKeyspaceUnitTestCase):

    def setUp(self):
        self.table_name = "{}.prepared_statement_invalidation_test".format(self.keyspace_name)
        self.session.execute("CREATE TABLE {} (a int PRIMARY KEY, b int, d int);".format(self.table_name))
        self.session.execute("INSERT INTO {} (a, b, d) VALUES (1, 1, 1);".format(self.table_name))
        self.session.execute("INSERT INTO {} (a, b, d) VALUES (2, 2, 2);".format(self.table_name))
        self.session.execute("INSERT INTO {} (a, b, d) VALUES (3, 3, 3);".format(self.table_name))
        self.session.execute("INSERT INTO {} (a, b, d) VALUES (4, 4, 4);".format(self.table_name))

    def tearDown(self):
        self.session.execute("DROP TABLE {}".format(self.table_name))

    def test_invalidated_result_metadata(self):
        """
        Tests to make sure cached metadata is updated when an invalidated prepared statement is reprepared.

        @since 2.7.0
        @jira_ticket PYTHON-621

        Prior to this fix, the request would blow up with a protocol error when the result was decoded expecting a different
        number of columns.
        """
        wildcard_prepared = self.session.prepare("SELECT * FROM {}".format(self.table_name))
        original_result_metadata = wildcard_prepared.result_metadata
        self.assertEqual(len(original_result_metadata), 3)

        r = self.session.execute(wildcard_prepared)
        self.assertEqual(r[0], (1, 1, 1))

        self.session.execute("ALTER TABLE {} DROP d".format(self.table_name))

        # Get a bunch of requests in the pipeline with varying states of result_meta, reprepare, resolved
        futures = set(self.session.execute_async(wildcard_prepared.bind(None)) for _ in range(200))
        for f in futures:
            self.assertEqual(f.result()[0], (1, 1))

        self.assertIsNot(wildcard_prepared.result_metadata, original_result_metadata)

    def test_prepared_id_is_update(self):
        """
        Tests that checks the query id from the prepared statement
        is updated properly if the table that the prepared statement is querying
        is altered.

        @since 3.12
        @jira_ticket PYTHON-808

        The query id from the prepared statement must have changed
        """
        prepared_statement = self.session.prepare("SELECT * from {} WHERE a = ?".format(self.table_name))
        id_before = prepared_statement.result_metadata_id
        self.assertEqual(len(prepared_statement.result_metadata), 3)

        self.session.execute("ALTER TABLE {} ADD c int".format(self.table_name))
        bound_statement = prepared_statement.bind((1, ))
        self.session.execute(bound_statement, timeout=1)

        id_after = prepared_statement.result_metadata_id

        self.assertNotEqual(id_before, id_after)
        self.assertEqual(len(prepared_statement.result_metadata), 4)

    def test_prepared_id_is_updated_across_pages(self):
        """
        Test that checks that the query id from the prepared statement
        is updated if the table hat the prepared statement is querying
        is altered while fetching pages in a single query.
        Then it checks that the updated rows have the expected result.

        @since 3.12
        @jira_ticket PYTHON-808
        """
        prepared_statement = self.session.prepare("SELECT * from {}".format(self.table_name))
        id_before = prepared_statement.result_metadata_id
        self.assertEqual(len(prepared_statement.result_metadata), 3)

        prepared_statement.fetch_size = 2
        result = self.session.execute(prepared_statement.bind((None)))

        self.assertTrue(result.has_more_pages)

        self.session.execute("ALTER TABLE {} ADD c int".format(self.table_name))

        result_set = set(x for x in ((1, 1, 1), (2, 2, 2), (3, 3, None, 3), (4, 4, None, 4)))
        expected_result_set = set(row for row in result)

        id_after = prepared_statement.result_metadata_id

        self.assertEqual(result_set, expected_result_set)
        self.assertNotEqual(id_before, id_after)
        self.assertEqual(len(prepared_statement.result_metadata), 4)

    def test_prepare_id_is_updated_across_session(self):
        """
        Test that checks that the query id from the prepared statement
        is updated if the table hat the prepared statement is querying
        is altered by a different session

        @since 3.12
        @jira_ticket PYTHON-808
        """
        one_cluster = TestCluster(metrics_enabled=True)
        one_session = one_cluster.connect()
        self.addCleanup(one_cluster.shutdown)

        stm = "SELECT * from {} WHERE a = ?".format(self.table_name)
        one_prepared_stm = one_session.prepare(stm)
        self.assertEqual(len(one_prepared_stm.result_metadata), 3)

        one_id_before = one_prepared_stm.result_metadata_id

        self.session.execute("ALTER TABLE {} ADD c int".format(self.table_name))
        one_session.execute(one_prepared_stm, (1, ))

        one_id_after = one_prepared_stm.result_metadata_id
        self.assertNotEqual(one_id_before, one_id_after)
        self.assertEqual(len(one_prepared_stm.result_metadata), 4)

    def test_not_reprepare_invalid_statements(self):
        """
        Test that checks that an InvalidRequest is arisen if a column
        expected by the prepared statement is dropped.

        @since 3.12
        @jira_ticket PYTHON-808
        """
        prepared_statement = self.session.prepare(
            "SELECT a, b, d FROM {} WHERE a = ?".format(self.table_name))
        self.session.execute("ALTER TABLE {} DROP d".format(self.table_name))
        with self.assertRaises(InvalidRequest):
            self.session.execute(prepared_statement.bind((1, )))

    def test_id_is_not_updated_conditional_v4(self):
        """
        Test that verifies that the result_metadata and the
        result_metadata_id are updated correctly in conditional statements
        in protocol V4

        @since 3.13
        @jira_ticket PYTHON-847
        """
        cluster = TestCluster(protocol_version=ProtocolVersion.V4)
        session = cluster.connect()
        self.addCleanup(cluster.shutdown)
        self._test_updated_conditional(session, 9)

    @requirecassandra
    def test_id_is_not_updated_conditional_v5(self):
        """
        Test that verifies that the result_metadata and the
        result_metadata_id are updated correctly in conditional statements
        in protocol V5
        @since 3.13
        @jira_ticket PYTHON-847
        """
        cluster = TestCluster(protocol_version=ProtocolVersion.V5)
        session = cluster.connect()
        self.addCleanup(cluster.shutdown)
        self._test_updated_conditional(session, 10)

    @greaterthanorequaldse50
    def test_id_is_not_updated_conditional_dsev1(self):
        """
        Test that verifies that the result_metadata and the
        result_metadata_id are updated correctly in conditional statements
        in protocol DSE V1

        @since 3.13
        @jira_ticket PYTHON-847
        """
        cluster = TestCluster(protocol_version=ProtocolVersion.DSE_V1)
        session = cluster.connect()
        self.addCleanup(cluster.shutdown)
        self._test_updated_conditional(session, 10)

    @greaterthanorequaldse50
    def test_id_is_not_updated_conditional_dsev2(self):
        """
        Test that verifies that the result_metadata and the
        result_metadata_id are updated correctly in conditional statements
        in protocol DSE V2

        @since 3.13
        @jira_ticket PYTHON-847
        """
        cluster = TestCluster(protocol_version=ProtocolVersion.DSE_V2)
        session = cluster.connect()
        self.addCleanup(cluster.shutdown)
        self._test_updated_conditional(session, 10)

    def _test_updated_conditional(self, session, value):
        prepared_statement = session.prepare(
            "INSERT INTO {}(a, b, d) VALUES "
            "(?, ? , ?) IF NOT EXISTS".format(self.table_name))
        first_id = prepared_statement.result_metadata_id
        LOG.debug('initial result_metadata_id: {}'.format(first_id))

        def check_result_and_metadata(expected):
            self.assertEqual(
                session.execute(prepared_statement, (value, value, value))[0],
                expected
            )
            self.assertEqual(prepared_statement.result_metadata_id, first_id)
            self.assertIsNone(prepared_statement.result_metadata)

        # Successful conditional update
        check_result_and_metadata((True,))

        # Failed conditional update
        check_result_and_metadata((False, value, value, value))

        session.execute("ALTER TABLE {} ADD c int".format(self.table_name))

        # Failed conditional update
        check_result_and_metadata((False, value, value, None, value))
