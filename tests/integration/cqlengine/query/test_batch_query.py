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

from unittest import mock

from cassandra.cqlengine import columns
from cassandra.cqlengine.connection import NOT_SET
from cassandra.cqlengine.management import drop_table, sync_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import BatchQuery, DMLQuery
from tests.integration.cqlengine.base import BaseCassEngTestCase
from tests.integration.cqlengine import execute_count
from cassandra.cluster import Session
from cassandra.query import BatchType as cassandra_BatchType
from cassandra.cqlengine.query import BatchType as cqlengine_BatchType


class TestMultiKeyModel(Model):

    partition = columns.Integer(primary_key=True)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer(required=False)
    text = columns.Text(required=False)

class BatchQueryLogModel(Model):

    # simple k/v table
    k = columns.Integer(primary_key=True)
    v = columns.Integer()


class CounterBatchQueryModel(Model):
    k = columns.Integer(primary_key=True)
    v = columns.Counter()

class BatchQueryTests(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(BatchQueryTests, cls).setUpClass()
        drop_table(TestMultiKeyModel)
        sync_table(TestMultiKeyModel)

    @classmethod
    def tearDownClass(cls):
        super(BatchQueryTests, cls).tearDownClass()
        drop_table(TestMultiKeyModel)

    def setUp(self):
        super(BatchQueryTests, self).setUp()
        self.pkey = 1
        for obj in TestMultiKeyModel.filter(partition=self.pkey):
            obj.delete()

    @execute_count(3)
    def test_insert_success_case(self):

        b = BatchQuery()
        inst = TestMultiKeyModel.batch(b).create(partition=self.pkey, cluster=2, count=3, text='4')

        with self.assertRaises(TestMultiKeyModel.DoesNotExist):
            TestMultiKeyModel.get(partition=self.pkey, cluster=2)

        b.execute()

        TestMultiKeyModel.get(partition=self.pkey, cluster=2)

    @execute_count(4)
    def test_update_success_case(self):

        inst = TestMultiKeyModel.create(partition=self.pkey, cluster=2, count=3, text='4')

        b = BatchQuery()

        inst.count = 4
        inst.batch(b).save()

        inst2 = TestMultiKeyModel.get(partition=self.pkey, cluster=2)
        self.assertEqual(inst2.count, 3)

        b.execute()

        inst3 = TestMultiKeyModel.get(partition=self.pkey, cluster=2)
        self.assertEqual(inst3.count, 4)

    @execute_count(4)
    def test_delete_success_case(self):

        inst = TestMultiKeyModel.create(partition=self.pkey, cluster=2, count=3, text='4')

        b = BatchQuery()

        inst.batch(b).delete()

        TestMultiKeyModel.get(partition=self.pkey, cluster=2)

        b.execute()

        with self.assertRaises(TestMultiKeyModel.DoesNotExist):
            TestMultiKeyModel.get(partition=self.pkey, cluster=2)

    @execute_count(11)
    def test_context_manager(self):

        with BatchQuery() as b:
            for i in range(5):
                TestMultiKeyModel.batch(b).create(partition=self.pkey, cluster=i, count=3, text='4')

            for i in range(5):
                with self.assertRaises(TestMultiKeyModel.DoesNotExist):
                    TestMultiKeyModel.get(partition=self.pkey, cluster=i)

        for i in range(5):
            TestMultiKeyModel.get(partition=self.pkey, cluster=i)

    @execute_count(9)
    def test_bulk_delete_success_case(self):

        for i in range(1):
            for j in range(5):
                TestMultiKeyModel.create(partition=i, cluster=j, count=i*j, text='{0}:{1}'.format(i,j))

        with BatchQuery() as b:
            TestMultiKeyModel.objects.batch(b).filter(partition=0).delete()
            self.assertEqual(TestMultiKeyModel.filter(partition=0).count(), 5)

        self.assertEqual(TestMultiKeyModel.filter(partition=0).count(), 0)
        #cleanup
        for m in TestMultiKeyModel.all():
            m.delete()

    @execute_count(0)
    def test_none_success_case(self):
        """ Tests that passing None into the batch call clears any batch object """
        b = BatchQuery()

        q = TestMultiKeyModel.objects.batch(b)
        self.assertEqual(q._batch, b)

        q = q.batch(None)
        self.assertIsNone(q._batch)

    @execute_count(0)
    def test_dml_none_success_case(self):
        """ Tests that passing None into the batch call clears any batch object """
        b = BatchQuery()

        q = DMLQuery(TestMultiKeyModel, batch=b)
        self.assertEqual(q._batch, b)

        q.batch(None)
        self.assertIsNone(q._batch)

    @execute_count(3)
    def test_batch_execute_on_exception_succeeds(self):
        # makes sure if execute_on_exception == True we still apply the batch
        drop_table(BatchQueryLogModel)
        sync_table(BatchQueryLogModel)

        obj = BatchQueryLogModel.objects(k=1)
        self.assertEqual(0, len(obj))

        try:
            with BatchQuery(execute_on_exception=True) as b:
                BatchQueryLogModel.batch(b).create(k=1, v=1)
                raise Exception("Blah")
        except:
            pass

        obj = BatchQueryLogModel.objects(k=1)
        # should be 1 because the batch should execute
        self.assertEqual(1, len(obj))

    @execute_count(2)
    def test_batch_execute_on_exception_skips_if_not_specified(self):
        # makes sure if execute_on_exception == True we still apply the batch
        drop_table(BatchQueryLogModel)
        sync_table(BatchQueryLogModel)

        obj = BatchQueryLogModel.objects(k=2)
        self.assertEqual(0, len(obj))

        try:
            with BatchQuery() as b:
                BatchQueryLogModel.batch(b).create(k=2, v=2)
                raise Exception("Blah")
        except:
            pass

        obj = BatchQueryLogModel.objects(k=2)

        # should be 0 because the batch should not execute
        self.assertEqual(0, len(obj))

    @execute_count(1)
    def test_batch_execute_timeout(self):
        with mock.patch.object(Session, 'execute') as mock_execute:
            with BatchQuery(timeout=1) as b:
                BatchQueryLogModel.batch(b).create(k=2, v=2)
            self.assertEqual(mock_execute.call_args[-1]['timeout'], 1)

    @execute_count(1)
    def test_batch_execute_no_timeout(self):
        with mock.patch.object(Session, 'execute') as mock_execute:
            with BatchQuery() as b:
                BatchQueryLogModel.batch(b).create(k=2, v=2)
            self.assertEqual(mock_execute.call_args[-1]['timeout'], NOT_SET)


class BatchTypeQueryTests(BaseCassEngTestCase):
    def setUp(self):
        sync_table(TestMultiKeyModel)
        sync_table(CounterBatchQueryModel)

    def tearDown(self):
        drop_table(TestMultiKeyModel)
        drop_table(CounterBatchQueryModel)

    @execute_count(6)
    def test_cassandra_batch_type(self):
        """
        Tests the different types of `class: cassandra.query.BatchType`

        @since 3.13
        @jira_ticket PYTHON-88
        @expected_result batch query succeeds and the results
        are correctly readen

        @test_category query
        """
        with BatchQuery(batch_type=cassandra_BatchType.UNLOGGED) as b:
            TestMultiKeyModel.batch(b).create(partition=1, cluster=1)
            TestMultiKeyModel.batch(b).create(partition=1, cluster=2)

        obj = TestMultiKeyModel.objects(partition=1)
        self.assertEqual(2, len(obj))

        with BatchQuery(batch_type=cassandra_BatchType.COUNTER) as b:
            CounterBatchQueryModel.batch(b).create(k=1, v=1)
            CounterBatchQueryModel.batch(b).create(k=1, v=2)
            CounterBatchQueryModel.batch(b).create(k=1, v=10)

        obj = CounterBatchQueryModel.objects(k=1)
        self.assertEqual(1, len(obj))
        self.assertEqual(obj[0].v, 13)

        with BatchQuery(batch_type=cassandra_BatchType.LOGGED) as b:
            TestMultiKeyModel.batch(b).create(partition=1, cluster=1)
            TestMultiKeyModel.batch(b).create(partition=1, cluster=2)

        obj = TestMultiKeyModel.objects(partition=1)
        self.assertEqual(2, len(obj))

    @execute_count(4)
    def test_cqlengine_batch_type(self):
        """
        Tests the different types of `class: cassandra.cqlengine.query.BatchType`

        @since 3.13
        @jira_ticket PYTHON-88
        @expected_result batch query succeeds and the results
        are correctly readen

        @test_category query
        """
        with BatchQuery(batch_type=cqlengine_BatchType.Unlogged) as b:
            TestMultiKeyModel.batch(b).create(partition=1, cluster=1)
            TestMultiKeyModel.batch(b).create(partition=1, cluster=2)

        obj = TestMultiKeyModel.objects(partition=1)
        self.assertEqual(2, len(obj))

        with BatchQuery(batch_type=cqlengine_BatchType.Counter) as b:
            CounterBatchQueryModel.batch(b).create(k=1, v=1)
            CounterBatchQueryModel.batch(b).create(k=1, v=2)
            CounterBatchQueryModel.batch(b).create(k=1, v=10)

        obj = CounterBatchQueryModel.objects(k=1)
        self.assertEqual(1, len(obj))
        self.assertEqual(obj[0].v, 13)
