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

from uuid import uuid4

from cassandra.query import FETCH_SIZE_UNSET
from cassandra.cqlengine.statements import BaseCQLStatement
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.statements import InsertStatement, UpdateStatement, SelectStatement, DeleteStatement, \
    WhereClause
from cassandra.cqlengine.operators import EqualsOperator, LikeOperator
from cassandra.cqlengine.columns import Column

from tests.integration.cqlengine.base import BaseCassEngTestCase, TestQueryUpdateModel
from tests.integration.cqlengine import DEFAULT_KEYSPACE
from tests.integration import greaterthanorequalcass3_10, lessthandse69, TestCluster

from cassandra.cqlengine.connection import execute


class BaseStatementTest(unittest.TestCase):

    def test_fetch_size(self):
        """ tests that fetch_size is correctly set """
        stmt = BaseCQLStatement('table', None, fetch_size=1000)
        self.assertEqual(stmt.fetch_size, 1000)

        stmt = BaseCQLStatement('table', None, fetch_size=None)
        self.assertEqual(stmt.fetch_size, FETCH_SIZE_UNSET)

        stmt = BaseCQLStatement('table', None)
        self.assertEqual(stmt.fetch_size, FETCH_SIZE_UNSET)


class ExecuteStatementTest(BaseCassEngTestCase):
    text = "text_for_db"

    @classmethod
    def setUpClass(cls):
        super(ExecuteStatementTest, cls).setUpClass()
        sync_table(TestQueryUpdateModel)
        cls.table_name = '{0}.test_query_update_model'.format(DEFAULT_KEYSPACE)

    @classmethod
    def tearDownClass(cls):
        super(ExecuteStatementTest, cls).tearDownClass()
        drop_table(TestQueryUpdateModel)

    def _verify_statement(self, original):
        st = SelectStatement(self.table_name)
        result = execute(st)
        response = result[0]

        for assignment in original.assignments:
            self.assertEqual(response[assignment.field], assignment.value)
        self.assertEqual(len(response), 7)

    def test_insert_statement_execute(self):
        """
        Test to verify the execution of BaseCQLStatements using connection.execute

        @since 3.10
        @jira_ticket PYTHON-505
        @expected_result inserts a row in C*, updates the rows and then deletes
        all the rows using BaseCQLStatements

        @test_category data_types:object_mapper
        """
        partition = uuid4()
        cluster = 1
        self._insert_statement(partition, cluster)

        # Verifying update statement
        where = [WhereClause('partition', EqualsOperator(), partition),
                 WhereClause('cluster', EqualsOperator(), cluster)]

        st = UpdateStatement(self.table_name, where=where)
        st.add_assignment(Column(db_field='count'), 2)
        st.add_assignment(Column(db_field='text'), "text_for_db_update")
        st.add_assignment(Column(db_field='text_set'), set(("foo_update", "bar_update")))
        st.add_assignment(Column(db_field='text_list'), ["foo_update", "bar_update"])
        st.add_assignment(Column(db_field='text_map'), {"foo": '3', "bar": '4'})

        execute(st)
        self._verify_statement(st)

        # Verifying delete statement
        execute(DeleteStatement(self.table_name, where=where))
        self.assertEqual(TestQueryUpdateModel.objects.count(), 0)

    @greaterthanorequalcass3_10
    @lessthandse69
    def test_like_operator(self):
        """
        Test to verify the like operator works appropriately

        @since 3.13
        @jira_ticket PYTHON-512
        @expected_result the expected row is read using LIKE

        @test_category data_types:object_mapper
        """
        cluster = TestCluster()
        session = cluster.connect()
        self.addCleanup(cluster.shutdown)

        session.execute("""CREATE CUSTOM INDEX text_index ON {} (text)
                                    USING 'org.apache.cassandra.index.sasi.SASIIndex';""".format(self.table_name))
        self.addCleanup(session.execute, "DROP INDEX {}.text_index".format(DEFAULT_KEYSPACE))

        partition = uuid4()
        cluster = 1
        self._insert_statement(partition, cluster)

        ss = SelectStatement(self.table_name)
        like_clause = "text_for_%"
        ss.add_where(Column(db_field='text'), LikeOperator(), like_clause)
        self.assertEqual(str(ss),
                         'SELECT * FROM {} WHERE "text" LIKE %(0)s'.format(self.table_name))

        result = execute(ss)
        self.assertEqual(result[0]["text"], self.text)

        q = TestQueryUpdateModel.objects.filter(text__like=like_clause).allow_filtering()
        self.assertEqual(q[0].text, self.text)

        q = TestQueryUpdateModel.objects.filter(text__like=like_clause)
        self.assertEqual(q[0].text, self.text)

    def _insert_statement(self, partition, cluster):
        # Verifying insert statement
        st = InsertStatement(self.table_name)
        st.add_assignment(Column(db_field='partition'), partition)
        st.add_assignment(Column(db_field='cluster'), cluster)

        st.add_assignment(Column(db_field='count'), 1)
        st.add_assignment(Column(db_field='text'), self.text)
        st.add_assignment(Column(db_field='text_set'), set(("foo", "bar")))
        st.add_assignment(Column(db_field='text_list'), ["foo", "bar"])
        st.add_assignment(Column(db_field='text_map'), {"foo": '1', "bar": '2'})

        execute(st)
        self._verify_statement(st)
