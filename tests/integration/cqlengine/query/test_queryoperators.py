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

from datetime import datetime

from cassandra.cqlengine import columns
from cassandra.cqlengine import functions
from cassandra.cqlengine import query
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.named import NamedTable
from cassandra.cqlengine.operators import EqualsOperator
from cassandra.cqlengine.statements import WhereClause
from tests.integration.cqlengine import DEFAULT_KEYSPACE
from tests.integration.cqlengine.base import BaseCassEngTestCase
from tests.integration.cqlengine import execute_count


class TestQuerySetOperation(BaseCassEngTestCase):

    def test_maxtimeuuid_function(self):
        """
        Tests that queries with helper functions are generated properly
        """
        now = datetime.now()
        where = WhereClause('time', EqualsOperator(), functions.MaxTimeUUID(now))
        where.set_context_id(5)

        self.assertEqual(str(where), '"time" = MaxTimeUUID(%(5)s)')
        ctx = {}
        where.update_context(ctx)
        self.assertEqual(ctx, {'5': columns.DateTime().to_database(now)})

    def test_mintimeuuid_function(self):
        """
        Tests that queries with helper functions are generated properly
        """
        now = datetime.now()
        where = WhereClause('time', EqualsOperator(), functions.MinTimeUUID(now))
        where.set_context_id(5)

        self.assertEqual(str(where), '"time" = MinTimeUUID(%(5)s)')
        ctx = {}
        where.update_context(ctx)
        self.assertEqual(ctx, {'5': columns.DateTime().to_database(now)})


class TokenTestModel(Model):
    __table_name__ = "token_test_model"
    key = columns.Integer(primary_key=True)
    val = columns.Integer()


class TestTokenFunction(BaseCassEngTestCase):

    def setUp(self):
        super(TestTokenFunction, self).setUp()
        sync_table(TokenTestModel)

    def tearDown(self):
        super(TestTokenFunction, self).tearDown()
        drop_table(TokenTestModel)

    @execute_count(15)
    def test_token_function(self):
        """ Tests that token functions work properly """
        assert TokenTestModel.objects.count() == 0
        for i in range(10):
            TokenTestModel.create(key=i, val=i)
        assert TokenTestModel.objects.count() == 10
        seen_keys = set()
        last_token = None
        for instance in TokenTestModel.objects().limit(5):
            last_token = instance.key
            seen_keys.add(last_token)
        assert len(seen_keys) == 5
        for instance in TokenTestModel.objects(pk__token__gt=functions.Token(last_token)):
            seen_keys.add(instance.key)

        assert len(seen_keys) == 10
        assert all([i in seen_keys for i in range(10)])

        # pk__token equality
        r = TokenTestModel.objects(pk__token=functions.Token(last_token))
        self.assertEqual(len(r), 1)
        r.all()  # Attempt to obtain queryset for results. This has thrown an exception in the past

    def test_compound_pk_token_function(self):

        class TestModel(Model):

            p1 = columns.Text(partition_key=True)
            p2 = columns.Text(partition_key=True)

        func = functions.Token('a', 'b')

        q = TestModel.objects.filter(pk__token__gt=func)
        where = q._where[0]
        where.set_context_id(1)
        self.assertEqual(str(where), 'token("p1", "p2") > token(%({0})s, %({1})s)'.format(1, 2))

        # Verify that a SELECT query can be successfully generated
        str(q._select_query())

        # Token(tuple()) is also possible for convenience
        # it (allows for Token(obj.pk) syntax)
        func = functions.Token(('a', 'b'))

        q = TestModel.objects.filter(pk__token__gt=func)
        where = q._where[0]
        where.set_context_id(1)
        self.assertEqual(str(where), 'token("p1", "p2") > token(%({0})s, %({1})s)'.format(1, 2))
        str(q._select_query())

        # The 'pk__token' virtual column may only be compared to a Token
        self.assertRaises(query.QueryException, TestModel.objects.filter, pk__token__gt=10)

        # A Token may only be compared to the `pk__token' virtual column
        func = functions.Token('a', 'b')
        self.assertRaises(query.QueryException, TestModel.objects.filter, p1__gt=func)

        # The # of arguments to Token must match the # of partition keys
        func = functions.Token('a')
        self.assertRaises(query.QueryException, TestModel.objects.filter, pk__token__gt=func)

    @execute_count(7)
    def test_named_table_pk_token_function(self):
        """
        Test to ensure that token function work with named tables.

        @since 3.2
        @jira_ticket PYTHON-272
        @expected_result partition key token functions should all for pagination. Prior to Python-272
        this would fail with an AttributeError

        @test_category object_mapper
        """

        for i in range(5):
            TokenTestModel.create(key=i, val=i)
        named = NamedTable(DEFAULT_KEYSPACE, TokenTestModel.__table_name__)

        query = named.all().limit(1)
        first_page = list(query)
        last = first_page[-1]
        self.assertTrue(len(first_page) is 1)
        next_page = list(query.filter(pk__token__gt=functions.Token(last.key)))
        self.assertTrue(len(next_page) is 1)
