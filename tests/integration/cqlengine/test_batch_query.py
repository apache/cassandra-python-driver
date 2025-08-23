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
import warnings

import pytest

from cassandra.cqlengine import columns
from cassandra.cqlengine.management import drop_table, sync_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import BatchQuery
from tests.integration.cqlengine.base import BaseCassEngTestCase

from unittest.mock import patch

class TestMultiKeyModel(Model):
    partition   = columns.Integer(primary_key=True)
    cluster     = columns.Integer(primary_key=True)
    count       = columns.Integer(required=False)
    text        = columns.Text(required=False)


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

    def test_insert_success_case(self):

        b = BatchQuery()
        TestMultiKeyModel.batch(b).create(partition=self.pkey, cluster=2, count=3, text='4')

        with self.assertRaises(TestMultiKeyModel.DoesNotExist):
            TestMultiKeyModel.get(partition=self.pkey, cluster=2)

        b.execute()

        TestMultiKeyModel.get(partition=self.pkey, cluster=2)

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

    def test_delete_success_case(self):

        inst = TestMultiKeyModel.create(partition=self.pkey, cluster=2, count=3, text='4')

        b = BatchQuery()

        inst.batch(b).delete()

        TestMultiKeyModel.get(partition=self.pkey, cluster=2)

        b.execute()

        with self.assertRaises(TestMultiKeyModel.DoesNotExist):
            TestMultiKeyModel.get(partition=self.pkey, cluster=2)

    def test_context_manager(self):

        with BatchQuery() as b:
            for i in range(5):
                TestMultiKeyModel.batch(b).create(partition=self.pkey, cluster=i, count=3, text='4')

            for i in range(5):
                with self.assertRaises(TestMultiKeyModel.DoesNotExist):
                    TestMultiKeyModel.get(partition=self.pkey, cluster=i)

        for i in range(5):
            TestMultiKeyModel.get(partition=self.pkey, cluster=i)

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

    def test_empty_batch(self):
        b = BatchQuery()
        b.execute()

        with BatchQuery() as b:
            pass

class BatchQueryCallbacksTests(BaseCassEngTestCase):

    def test_API_managing_callbacks(self):

        # Callbacks can be added at init and after

        def my_callback(*args, **kwargs):
            pass

        # adding on init:
        batch = BatchQuery()

        batch.add_callback(my_callback)
        batch.add_callback(my_callback, 2, named_arg='value')
        batch.add_callback(my_callback, 1, 3)

        self.assertEqual(batch._callbacks, [
            (my_callback, (), {}),
            (my_callback, (2,), {'named_arg':'value'}),
            (my_callback, (1, 3), {})
        ])

    def test_callbacks_properly_execute_callables_and_tuples(self):

        call_history = []
        def my_callback(*args, **kwargs):
            call_history.append(args)

        # adding on init:
        batch = BatchQuery()

        batch.add_callback(my_callback)
        batch.add_callback(my_callback, 'more', 'args')

        batch.execute()

        self.assertEqual(len(call_history), 2)
        self.assertEqual([(), ('more', 'args')], call_history)

    def test_callbacks_tied_to_execute(self):
        """Batch callbacks should NOT fire if batch is not executed in context manager mode"""

        call_history = []
        def my_callback(*args, **kwargs):
            call_history.append(args)

        with BatchQuery() as batch:
            batch.add_callback(my_callback)

        self.assertEqual(len(call_history), 1)

        class SomeError(Exception):
            pass

        with self.assertRaises(SomeError):
            with BatchQuery() as batch:
                batch.add_callback(my_callback)
                # this error bubbling up through context manager
                # should prevent callback runs (along with b.execute())
                raise SomeError

        # still same call history. Nothing added
        self.assertEqual(len(call_history), 1)

        # but if execute ran, even with an error bubbling through
        # the callbacks also would have fired
        with self.assertRaises(SomeError):
            with BatchQuery(execute_on_exception=True) as batch:
                batch.add_callback(my_callback)
                raise SomeError

        # updated call history
        self.assertEqual(len(call_history), 2)

    def test_callbacks_work_multiple_times(self):
        """
        Tests that multiple executions of execute on a batch statement
        logs a warning, and that we don't encounter an attribute error.
        @since 3.1
        @jira_ticket PYTHON-445
        @expected_result warning message is logged

        @test_category object_mapper
        """
        call_history = []

        def my_callback(*args, **kwargs):
            call_history.append(args)

        with pytest.warns() as w:
            with BatchQuery() as batch:
                batch.add_callback(my_callback)
                batch.execute()
            batch.execute()
        self.assertEqual(len(w), 2)  # package filter setup to warn always
        self.assertRegex(str(w[0].message), r"^Batch.*multiple.*")

    def test_disable_multiple_callback_warning(self):
        """
        Tests that multiple executions of a batch statement
        don't log a warning when warn_multiple_exec flag is set, and
        that we don't encounter an attribute error.
        @since 3.1
        @jira_ticket PYTHON-445
        @expected_result warning message is logged

        @test_category object_mapper
        """
        call_history = []

        def my_callback(*args, **kwargs):
            call_history.append(args)

        with patch('cassandra.cqlengine.query.BatchQuery.warn_multiple_exec', False):
            with warnings.catch_warnings(record=True) as w:
                with BatchQuery() as batch:
                    batch.add_callback(my_callback)
                    batch.execute()
                batch.execute()
            self.assertFalse(w)
