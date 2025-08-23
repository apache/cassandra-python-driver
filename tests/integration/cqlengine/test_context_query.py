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

from cassandra.cqlengine import columns
from cassandra.cqlengine.management import drop_keyspace, sync_table, create_keyspace_simple
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import ContextQuery
from tests.integration.cqlengine.base import BaseCassEngTestCase


class TestModel(Model):

    __keyspace__ = 'ks1'

    partition = columns.Integer(primary_key=True)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer()
    text = columns.Text()


class ContextQueryTests(BaseCassEngTestCase):

    KEYSPACES = ('ks1', 'ks2', 'ks3', 'ks4')

    @classmethod
    def setUpClass(cls):
        super(ContextQueryTests, cls).setUpClass()
        for ks in cls.KEYSPACES:
            create_keyspace_simple(ks, 1)
        sync_table(TestModel, keyspaces=cls.KEYSPACES)

    @classmethod
    def tearDownClass(cls):
        super(ContextQueryTests, cls).tearDownClass()
        for ks in cls.KEYSPACES:
            drop_keyspace(ks)


    def setUp(self):
        super(ContextQueryTests, self).setUp()
        for ks in self.KEYSPACES:
            with ContextQuery(TestModel, keyspace=ks) as tm:
                for obj in tm.all():
                    obj.delete()

    def test_context_manager(self):
        """
        Validates that when a context query is constructed that the
        keyspace of the returned model is toggled appropriately

        @since 3.6
        @jira_ticket PYTHON-598
        @expected_result default keyspace should be used

        @test_category query
        """
        # model keyspace write/read
        for ks in self.KEYSPACES:
            with ContextQuery(TestModel, keyspace=ks) as tm:
                self.assertEqual(tm.__keyspace__, ks)

        self.assertEqual(TestModel._get_keyspace(), 'ks1')

    def test_default_keyspace(self):
        """
        Tests the use of context queries with the default model keyspsace

        @since 3.6
        @jira_ticket PYTHON-598
        @expected_result default keyspace should be used

        @test_category query
        """
        # model keyspace write/read
        for i in range(5):
            TestModel.objects.create(partition=i, cluster=i)

        with ContextQuery(TestModel) as tm:
            self.assertEqual(5, len(tm.objects.all()))

        with ContextQuery(TestModel, keyspace='ks1') as tm:
            self.assertEqual(5, len(tm.objects.all()))

        for ks in self.KEYSPACES[1:]:
            with ContextQuery(TestModel, keyspace=ks) as tm:
                self.assertEqual(0, len(tm.objects.all()))

    def test_context_keyspace(self):
        """
        Tests the use of context queries with non default keyspaces

        @since 3.6
        @jira_ticket PYTHON-598
        @expected_result queries should be routed to appropriate keyspaces

        @test_category query
        """
        for i in range(5):
            with ContextQuery(TestModel, keyspace='ks4') as tm:
                tm.objects.create(partition=i, cluster=i)

        with ContextQuery(TestModel, keyspace='ks4') as tm:
            self.assertEqual(5, len(tm.objects.all()))

        self.assertEqual(0, len(TestModel.objects.all()))

        for ks in self.KEYSPACES[:2]:
            with ContextQuery(TestModel, keyspace=ks) as tm:
                self.assertEqual(0, len(tm.objects.all()))

        # simple data update
        with ContextQuery(TestModel, keyspace='ks4') as tm:
            obj = tm.objects.get(partition=1)
            obj.update(count=42)

            self.assertEqual(42, tm.objects.get(partition=1).count)

    def test_context_multiple_models(self):
        """
        Tests the use of multiple models with the context manager

        @since 3.7
        @jira_ticket PYTHON-613
        @expected_result all models are properly updated with the context

        @test_category query
        """

        with ContextQuery(TestModel, TestModel, keyspace='ks4') as (tm1, tm2):

            self.assertNotEqual(tm1, tm2)
            self.assertEqual(tm1.__keyspace__, 'ks4')
            self.assertEqual(tm2.__keyspace__, 'ks4')

    def test_context_invalid_parameters(self):
        """
        Tests that invalid parameters are raised by the context manager

        @since 3.7
        @jira_ticket PYTHON-613
        @expected_result a ValueError is raised when passing invalid parameters

        @test_category query
        """

        with self.assertRaises(ValueError):
            with ContextQuery(keyspace='ks2'):
                pass

        with self.assertRaises(ValueError):
            with ContextQuery(42) as tm:
                pass

        with self.assertRaises(ValueError):
            with ContextQuery(TestModel, 42):
                pass

        with self.assertRaises(ValueError):
            with ContextQuery(TestModel, unknown_param=42):
                pass

        with self.assertRaises(ValueError):
            with ContextQuery(TestModel, keyspace='ks2', unknown_param=42):
                pass