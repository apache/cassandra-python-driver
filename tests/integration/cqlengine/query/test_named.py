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
from cassandra.cqlengine import operators
from cassandra.cqlengine.named import NamedKeyspace
from cassandra.cqlengine.operators import EqualsOperator, GreaterThanOrEqualOperator
from cassandra.cqlengine.query import ResultObject
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.cqlengine import models

from tests.integration.cqlengine import setup_connection, execute_count
from tests.integration.cqlengine.base import BaseCassEngTestCase
from tests.integration.cqlengine.query.test_queryset import BaseQuerySetUsage


from tests.integration import BasicSharedKeyspaceUnitTestCase, greaterthanorequalcass30


class TestQuerySetOperation(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestQuerySetOperation, cls).setUpClass()
        cls.keyspace = NamedKeyspace('cqlengine_test')
        cls.table = cls.keyspace.table('test_model')

    def test_query_filter_parsing(self):
        """
        Tests the queryset filter method parses it's kwargs properly
        """
        query1 = self.table.objects(test_id=5)
        assert len(query1._where) == 1

        op = query1._where[0]
        assert isinstance(op.operator, operators.EqualsOperator)
        assert op.value == 5

        query2 = query1.filter(expected_result__gte=1)
        assert len(query2._where) == 2

        op = query2._where[1]
        assert isinstance(op.operator, operators.GreaterThanOrEqualOperator)
        assert op.value == 1

    def test_query_expression_parsing(self):
        """ Tests that query experessions are evaluated properly """
        query1 = self.table.filter(self.table.column('test_id') == 5)
        assert len(query1._where) == 1

        op = query1._where[0]
        assert isinstance(op.operator, operators.EqualsOperator)
        assert op.value == 5

        query2 = query1.filter(self.table.column('expected_result') >= 1)
        assert len(query2._where) == 2

        op = query2._where[1]
        assert isinstance(op.operator, operators.GreaterThanOrEqualOperator)
        assert op.value == 1

    def test_filter_method_where_clause_generation(self):
        """
        Tests the where clause creation
        """
        query1 = self.table.objects(test_id=5)
        self.assertEqual(len(query1._where), 1)
        where = query1._where[0]
        self.assertEqual(where.field, 'test_id')
        self.assertEqual(where.value, 5)

        query2 = query1.filter(expected_result__gte=1)
        self.assertEqual(len(query2._where), 2)

        where = query2._where[0]
        self.assertEqual(where.field, 'test_id')
        self.assertIsInstance(where.operator, EqualsOperator)
        self.assertEqual(where.value, 5)

        where = query2._where[1]
        self.assertEqual(where.field, 'expected_result')
        self.assertIsInstance(where.operator, GreaterThanOrEqualOperator)
        self.assertEqual(where.value, 1)

    def test_query_expression_where_clause_generation(self):
        """
        Tests the where clause creation
        """
        query1 = self.table.objects(self.table.column('test_id') == 5)
        self.assertEqual(len(query1._where), 1)
        where = query1._where[0]
        self.assertEqual(where.field, 'test_id')
        self.assertEqual(where.value, 5)

        query2 = query1.filter(self.table.column('expected_result') >= 1)
        self.assertEqual(len(query2._where), 2)

        where = query2._where[0]
        self.assertEqual(where.field, 'test_id')
        self.assertIsInstance(where.operator, EqualsOperator)
        self.assertEqual(where.value, 5)

        where = query2._where[1]
        self.assertEqual(where.field, 'expected_result')
        self.assertIsInstance(where.operator, GreaterThanOrEqualOperator)
        self.assertEqual(where.value, 1)


class TestQuerySetCountSelectionAndIteration(BaseQuerySetUsage):

    @classmethod
    def setUpClass(cls):
        super(TestQuerySetCountSelectionAndIteration, cls).setUpClass()

        from tests.integration.cqlengine.query.test_queryset import TestModel

        ks, tn = TestModel.column_family_name().split('.')
        cls.keyspace = NamedKeyspace(ks)
        cls.table = cls.keyspace.table(tn)

    @execute_count(2)
    def test_count(self):
        """ Tests that adding filtering statements affects the count query as expected """
        assert self.table.objects.count() == 12

        q = self.table.objects(test_id=0)
        assert q.count() == 4

    @execute_count(2)
    def test_query_expression_count(self):
        """ Tests that adding query statements affects the count query as expected """
        assert self.table.objects.count() == 12

        q = self.table.objects(self.table.column('test_id') == 0)
        assert q.count() == 4

    @execute_count(3)
    def test_iteration(self):
        """ Tests that iterating over a query set pulls back all of the expected results """
        q = self.table.objects(test_id=0)
        # tuple of expected attempt_id, expected_result values
        compare_set = set([(0, 5), (1, 10), (2, 15), (3, 20)])
        for t in q:
            val = t.attempt_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)
        assert len(compare_set) == 0

        # test with regular filtering
        q = self.table.objects(attempt_id=3).allow_filtering()
        assert len(q) == 3
        # tuple of expected test_id, expected_result values
        compare_set = set([(0, 20), (1, 20), (2, 75)])
        for t in q:
            val = t.test_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)
        assert len(compare_set) == 0

        # test with query method
        q = self.table.objects(self.table.column('attempt_id') == 3).allow_filtering()
        assert len(q) == 3
        # tuple of expected test_id, expected_result values
        compare_set = set([(0, 20), (1, 20), (2, 75)])
        for t in q:
            val = t.test_id, t.expected_result
            assert val in compare_set
            compare_set.remove(val)
        assert len(compare_set) == 0

    @execute_count(2)
    def test_multiple_iterations_work_properly(self):
        """ Tests that iterating over a query set more than once works """
        # test with both the filtering method and the query method
        for q in (self.table.objects(test_id=0), self.table.objects(self.table.column('test_id') == 0)):
            # tuple of expected attempt_id, expected_result values
            compare_set = set([(0, 5), (1, 10), (2, 15), (3, 20)])
            for t in q:
                val = t.attempt_id, t.expected_result
                assert val in compare_set
                compare_set.remove(val)
            assert len(compare_set) == 0

            # try it again
            compare_set = set([(0, 5), (1, 10), (2, 15), (3, 20)])
            for t in q:
                val = t.attempt_id, t.expected_result
                assert val in compare_set
                compare_set.remove(val)
            assert len(compare_set) == 0

    @execute_count(2)
    def test_multiple_iterators_are_isolated(self):
        """
        tests that the use of one iterator does not affect the behavior of another
        """
        for q in (self.table.objects(test_id=0), self.table.objects(self.table.column('test_id') == 0)):
            q = q.order_by('attempt_id')
            expected_order = [0, 1, 2, 3]
            iter1 = iter(q)
            iter2 = iter(q)
            for attempt_id in expected_order:
                assert next(iter1).attempt_id == attempt_id
                assert next(iter2).attempt_id == attempt_id

    @execute_count(3)
    def test_get_success_case(self):
        """
        Tests that the .get() method works on new and existing querysets
        """
        m = self.table.objects.get(test_id=0, attempt_id=0)
        assert isinstance(m, ResultObject)
        assert m.test_id == 0
        assert m.attempt_id == 0

        q = self.table.objects(test_id=0, attempt_id=0)
        m = q.get()
        assert isinstance(m, ResultObject)
        assert m.test_id == 0
        assert m.attempt_id == 0

        q = self.table.objects(test_id=0)
        m = q.get(attempt_id=0)
        assert isinstance(m, ResultObject)
        assert m.test_id == 0
        assert m.attempt_id == 0

    @execute_count(3)
    def test_query_expression_get_success_case(self):
        """
        Tests that the .get() method works on new and existing querysets
        """
        m = self.table.get(self.table.column('test_id') == 0, self.table.column('attempt_id') == 0)
        assert isinstance(m, ResultObject)
        assert m.test_id == 0
        assert m.attempt_id == 0

        q = self.table.objects(self.table.column('test_id') == 0, self.table.column('attempt_id') == 0)
        m = q.get()
        assert isinstance(m, ResultObject)
        assert m.test_id == 0
        assert m.attempt_id == 0

        q = self.table.objects(self.table.column('test_id') == 0)
        m = q.get(self.table.column('attempt_id') == 0)
        assert isinstance(m, ResultObject)
        assert m.test_id == 0
        assert m.attempt_id == 0

    @execute_count(1)
    def test_get_doesnotexist_exception(self):
        """
        Tests that get calls that don't return a result raises a DoesNotExist error
        """
        with self.assertRaises(self.table.DoesNotExist):
            self.table.objects.get(test_id=100)

    @execute_count(1)
    def test_get_multipleobjects_exception(self):
        """
        Tests that get calls that return multiple results raise a MultipleObjectsReturned error
        """
        with self.assertRaises(self.table.MultipleObjectsReturned):
            self.table.objects.get(test_id=1)


class TestNamedWithMV(BasicSharedKeyspaceUnitTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestNamedWithMV, cls).setUpClass()
        cls.default_keyspace = models.DEFAULT_KEYSPACE
        models.DEFAULT_KEYSPACE = cls.ks_name

    @classmethod
    def tearDownClass(cls):
        models.DEFAULT_KEYSPACE = cls.default_keyspace
        super(TestNamedWithMV, cls).tearDownClass()

    @greaterthanorequalcass30
    @execute_count(5)
    def test_named_table_with_mv(self):
        """
        Test NamedTable access to materialized views

        Creates some materialized views using Traditional CQL. Then ensures we can access those materialized view using
        the NamedKeyspace, and NamedTable interfaces. Tests basic filtering as well.

        @since 3.0.0
        @jira_ticket PYTHON-406
        @expected_result Named Tables should have access to materialized views

        @test_category materialized_view
        """
        ks = models.DEFAULT_KEYSPACE
        self.session.execute("DROP MATERIALIZED VIEW IF EXISTS {0}.alltimehigh".format(ks))
        self.session.execute("DROP MATERIALIZED VIEW IF EXISTS {0}.monthlyhigh".format(ks))
        self.session.execute("DROP TABLE IF EXISTS {0}.scores".format(ks))
        create_table = """CREATE TABLE {0}.scores(
                        user TEXT,
                        game TEXT,
                        year INT,
                        month INT,
                        day INT,
                        score INT,
                        PRIMARY KEY (user, game, year, month, day)
                        )""".format(ks)

        self.session.execute(create_table)
        create_mv = """CREATE MATERIALIZED VIEW {0}.monthlyhigh AS
                        SELECT game, year, month, score, user, day FROM {0}.scores
                        WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND day IS NOT NULL
                        PRIMARY KEY ((game, year, month), score, user, day)
                        WITH CLUSTERING ORDER BY (score DESC, user ASC, day ASC)""".format(ks)

        self.session.execute(create_mv)

        create_mv_alltime = """CREATE MATERIALIZED VIEW {0}.alltimehigh AS
                        SELECT * FROM {0}.scores
                        WHERE game IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL
                        PRIMARY KEY (game, score, user, year, month, day)
                        WITH CLUSTERING ORDER BY (score DESC, user DESC, year DESC, month DESC, day DESC)""".format(ks)

        self.session.execute(create_mv_alltime)

        # Populate the base table with data
        prepared_insert = self.session.prepare("""INSERT INTO {0}.scores (user, game, year, month, day, score) VALUES  (?, ?, ? ,? ,?, ?)""".format(ks))
        parameters = (('pcmanus', 'Coup', 2015, 5, 1, 4000),
                      ('jbellis', 'Coup', 2015, 5, 3, 1750),
                      ('yukim', 'Coup', 2015, 5, 3, 2250),
                      ('tjake', 'Coup', 2015, 5, 3, 500),
                      ('iamaleksey', 'Coup', 2015, 6, 1, 2500),
                      ('tjake', 'Coup', 2015, 6, 2, 1000),
                      ('pcmanus', 'Coup', 2015, 6, 2, 2000),
                      ('jmckenzie', 'Coup', 2015, 6, 9, 2700),
                      ('jbellis', 'Coup', 2015, 6, 20, 3500),
                      ('jbellis', 'Checkers', 2015, 6, 20, 1200),
                      ('jbellis', 'Chess', 2015, 6, 21, 3500),
                      ('pcmanus', 'Chess', 2015, 1, 25, 3200))
        prepared_insert.consistency_level = ConsistencyLevel.ALL
        execute_concurrent_with_args(self.session, prepared_insert, parameters)

        # Attempt to query the data using Named Table interface
        # Also test filtering on mv's
        key_space = NamedKeyspace(ks)
        mv_monthly = key_space.table("monthlyhigh")
        mv_all_time = key_space.table("alltimehigh")
        self.assertTrue(self.check_table_size("scores", key_space, len(parameters)))
        self.assertTrue(self.check_table_size("monthlyhigh", key_space, len(parameters)))
        self.assertTrue(self.check_table_size("alltimehigh", key_space, len(parameters)))

        filtered_mv_monthly_objects = mv_monthly.objects.filter(game='Chess', year=2015, month=6)
        self.assertEqual(len(filtered_mv_monthly_objects), 1)
        self.assertEqual(filtered_mv_monthly_objects[0]['score'], 3500)
        self.assertEqual(filtered_mv_monthly_objects[0]['user'], 'jbellis')
        filtered_mv_alltime_objects = mv_all_time.objects.filter(game='Chess')
        self.assertEqual(len(filtered_mv_alltime_objects), 2)
        self.assertEqual(filtered_mv_alltime_objects[0]['score'], 3500)

    def check_table_size(self, table_name, key_space, expected_size):
        table = key_space.table(table_name)
        attempts = 0
        while attempts < 10:
            attempts += 1
            table_size = len(table.objects.all())
            if(table_size is not expected_size):
                print("Table {0} size was {1} and was expected to be {2}".format(table_name, table_size, expected_size))
            else:
                return True

        return False
