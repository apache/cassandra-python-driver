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
from unittest import mock
from uuid import uuid4

from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import BatchQuery, BatchType, LWTException, IfExistsWithCounterColumn

from tests.integration.cqlengine.base import BaseCassEngTestCase
from tests.integration import PROTOCOL_VERSION


class TestIfExistsModel(Model):

    id = columns.UUID(primary_key=True, default=lambda: uuid4())
    count = columns.Integer()
    text = columns.Text(required=False)


class TestIfExistsModel2(Model):

    id = columns.Integer(primary_key=True)
    count = columns.Integer(primary_key=True, required=False)
    text = columns.Text(required=False)


class TestIfExistsWithCounterModel(Model):

    id = columns.UUID(primary_key=True, default=lambda: uuid4())
    likes = columns.Counter()


class BaseIfExistsTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseIfExistsTest, cls).setUpClass()
        sync_table(TestIfExistsModel)
        sync_table(TestIfExistsModel2)

    @classmethod
    def tearDownClass(cls):
        super(BaseIfExistsTest, cls).tearDownClass()
        drop_table(TestIfExistsModel)
        drop_table(TestIfExistsModel2)


class BaseIfExistsWithCounterTest(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BaseIfExistsWithCounterTest, cls).setUpClass()
        sync_table(TestIfExistsWithCounterModel)

    @classmethod
    def tearDownClass(cls):
        super(BaseIfExistsWithCounterTest, cls).tearDownClass()
        drop_table(TestIfExistsWithCounterModel)


class IfExistsUpdateTests(BaseIfExistsTest):

    @unittest.skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_update_if_exists(self):
        """
        Tests that update with if_exists work as expected

        @since 3.1
        @jira_ticket PYTHON-432
        @expected_result updates to be applied when primary key exists, otherwise LWT exception to be thrown

        @test_category object_mapper
        """

        id = uuid4()

        m = TestIfExistsModel.create(id=id, count=8, text='123456789')
        m.text = 'changed'
        m.if_exists().update()
        m = TestIfExistsModel.get(id=id)
        self.assertEqual(m.text, 'changed')

        # save()
        m.text = 'changed_again'
        m.if_exists().save()
        m = TestIfExistsModel.get(id=id)
        self.assertEqual(m.text, 'changed_again')

        m = TestIfExistsModel(id=uuid4(), count=44)  # do not exist
        with self.assertRaises(LWTException) as assertion:
            m.if_exists().update()

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

        # queryset update
        with self.assertRaises(LWTException) as assertion:
            TestIfExistsModel.objects(id=uuid4()).if_exists().update(count=8)

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

    @unittest.skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_batch_update_if_exists_success(self):
        """
        Tests that batch update with if_exists work as expected

        @since 3.1
        @jira_ticket PYTHON-432
        @expected_result

        @test_category object_mapper
        """

        id = uuid4()

        m = TestIfExistsModel.create(id=id, count=8, text='123456789')

        with BatchQuery() as b:
            m.text = '111111111'
            m.batch(b).if_exists().update()

        with self.assertRaises(LWTException) as assertion:
            with BatchQuery() as b:
                m = TestIfExistsModel(id=uuid4(), count=42)  # Doesn't exist
                m.batch(b).if_exists().update()

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

        q = TestIfExistsModel.objects(id=id)
        self.assertEqual(len(q), 1)

        tm = q.first()
        self.assertEqual(tm.count, 8)
        self.assertEqual(tm.text, '111111111')

    @unittest.skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_batch_mixed_update_if_exists_success(self):
        """
        Tests that batch update with one bad query will still fail with LWTException

        @since 3.1
        @jira_ticket PYTHON-432
        @expected_result

        @test_category object_mapper
        """

        m = TestIfExistsModel2.create(id=1, count=8, text='123456789')
        with self.assertRaises(LWTException) as assertion:
            with BatchQuery() as b:
                m.text = '111111112'
                m.batch(b).if_exists().update()  # Does exist
                n = TestIfExistsModel2(id=1, count=10, text="Failure")  # Doesn't exist
                n.batch(b).if_exists().update()

        self.assertEqual(assertion.exception.existing.get('[applied]'), False)

    @unittest.skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_delete_if_exists(self):
        """
        Tests that delete with if_exists work, and throws proper LWT exception when they are not applied

        @since 3.1
        @jira_ticket PYTHON-432
        @expected_result Deletes will be preformed if they exist, otherwise throw LWT exception

        @test_category object_mapper
        """

        id = uuid4()

        m = TestIfExistsModel.create(id=id, count=8, text='123456789')
        m.if_exists().delete()
        q = TestIfExistsModel.objects(id=id)
        self.assertEqual(len(q), 0)

        m = TestIfExistsModel(id=uuid4(), count=44)  # do not exist
        with self.assertRaises(LWTException) as assertion:
            m.if_exists().delete()

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

        # queryset delete
        with self.assertRaises(LWTException) as assertion:
            TestIfExistsModel.objects(id=uuid4()).if_exists().delete()

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

    @unittest.skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_batch_delete_if_exists_success(self):
        """
        Tests that batch deletes with if_exists work, and throws proper LWTException when they are not applied

        @since 3.1
        @jira_ticket PYTHON-432
        @expected_result Deletes will be preformed if they exist, otherwise throw LWTException

        @test_category object_mapper
        """

        id = uuid4()

        m = TestIfExistsModel.create(id=id, count=8, text='123456789')

        with BatchQuery() as b:
            m.batch(b).if_exists().delete()

        q = TestIfExistsModel.objects(id=id)
        self.assertEqual(len(q), 0)

        with self.assertRaises(LWTException) as assertion:
            with BatchQuery() as b:
                m = TestIfExistsModel(id=uuid4(), count=42)  # Doesn't exist
                m.batch(b).if_exists().delete()

        self.assertEqual(assertion.exception.existing, {
            '[applied]': False,
        })

    @unittest.skipUnless(PROTOCOL_VERSION >= 2, "only runs against the cql3 protocol v2.0")
    def test_batch_delete_mixed(self):
        """
        Tests that batch deletes  with multiple queries and throws proper LWTException when they are not all applicable

        @since 3.1
        @jira_ticket PYTHON-432
        @expected_result If one delete clause doesn't exist all should fail.

        @test_category object_mapper
        """

        m = TestIfExistsModel2.create(id=3, count=8, text='123456789')

        with self.assertRaises(LWTException) as assertion:
            with BatchQuery() as b:
                m.batch(b).if_exists().delete()  # Does exist
                n = TestIfExistsModel2(id=3, count=42, text='1111111')  # Doesn't exist
                n.batch(b).if_exists().delete()

        self.assertEqual(assertion.exception.existing.get('[applied]'), False)
        q = TestIfExistsModel2.objects(id=3, count=8)
        self.assertEqual(len(q), 1)


class IfExistsQueryTest(BaseIfExistsTest):

    def test_if_exists_included_on_queryset_update(self):

        with mock.patch.object(self.session, 'execute') as m:
            TestIfExistsModel.objects(id=uuid4()).if_exists().update(count=42)

        query = m.call_args[0][0].query_string
        self.assertIn("IF EXISTS", query)

    def test_if_exists_included_on_update(self):
        """ tests that if_exists on models update works as expected """

        with mock.patch.object(self.session, 'execute') as m:
            TestIfExistsModel(id=uuid4()).if_exists().update(count=8)

        query = m.call_args[0][0].query_string
        self.assertIn("IF EXISTS", query)

    def test_if_exists_included_on_delete(self):
        """ tests that if_exists on models delete works as expected """

        with mock.patch.object(self.session, 'execute') as m:
            TestIfExistsModel(id=uuid4()).if_exists().delete()

        query = m.call_args[0][0].query_string
        self.assertIn("IF EXISTS", query)


class IfExistWithCounterTest(BaseIfExistsWithCounterTest):

    def test_instance_raise_exception(self):
        """
        Tests if exists is used with a counter column model that exception are thrown

        @since 3.1
        @jira_ticket PYTHON-432
        @expected_result Deletes will be preformed if they exist, otherwise throw LWTException

        @test_category object_mapper
        """
        id = uuid4()
        with self.assertRaises(IfExistsWithCounterColumn):
            TestIfExistsWithCounterModel.if_exists()
