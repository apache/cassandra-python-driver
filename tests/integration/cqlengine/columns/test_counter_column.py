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

from uuid import uuid4

from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.models import Model, ModelDefinitionException
from tests.integration.cqlengine.base import BaseCassEngTestCase


class TestCounterModel(Model):

    partition = columns.UUID(primary_key=True, default=uuid4)
    cluster = columns.UUID(primary_key=True, default=uuid4)
    counter = columns.Counter()


class TestClassConstruction(BaseCassEngTestCase):

    def test_defining_a_non_counter_column_fails(self):
        """ Tests that defining a non counter column field in a model with a counter column fails """
        try:
            class model(Model):
                partition = columns.UUID(primary_key=True, default=uuid4)
                counter = columns.Counter()
                text = columns.Text()
            self.fail("did not raise expected ModelDefinitionException")
        except ModelDefinitionException:
            pass


    def test_defining_a_primary_key_counter_column_fails(self):
        """ Tests that defining primary keys on counter columns fails """
        try:
            class model(Model):
                partition = columns.UUID(primary_key=True, default=uuid4)
                cluster = columns.Counter(primary_ley=True)
                counter = columns.Counter()
            self.fail("did not raise expected TypeError")
        except TypeError:
            pass

        # force it
        try:
            class model(Model):
                partition = columns.UUID(primary_key=True, default=uuid4)
                cluster = columns.Counter()
                cluster.primary_key = True
                counter = columns.Counter()
            self.fail("did not raise expected ModelDefinitionException")
        except ModelDefinitionException:
            pass


class TestCounterColumn(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        drop_table(TestCounterModel)
        sync_table(TestCounterModel)

    @classmethod
    def tearDownClass(cls):
        drop_table(TestCounterModel)

    def test_updates(self):
        """ Tests that counter updates work as intended """
        instance = TestCounterModel.create()
        instance.counter += 5
        instance.save()

        actual = TestCounterModel.get(partition=instance.partition)
        assert actual.counter == 5

    def test_concurrent_updates(self):
        """ Tests updates from multiple queries reaches the correct value """
        instance = TestCounterModel.create()
        new1 = TestCounterModel.get(partition=instance.partition)
        new2 = TestCounterModel.get(partition=instance.partition)

        new1.counter += 5
        new1.save()
        new2.counter += 5
        new2.save()

        actual = TestCounterModel.get(partition=instance.partition)
        assert actual.counter == 10

    def test_update_from_none(self):
        """ Tests that updating from None uses a create statement """
        instance = TestCounterModel()
        instance.counter += 1
        instance.save()

        new = TestCounterModel.get(partition=instance.partition)
        assert new.counter == 1

    def test_new_instance_defaults_to_zero(self):
        """ Tests that instantiating a new model instance will set the counter column to zero """
        instance = TestCounterModel()
        assert instance.counter == 0

    def test_save_after_no_update(self):
        expected_value = 15
        instance = TestCounterModel.create()
        instance.update(counter=expected_value)

        # read back
        instance = TestCounterModel.get(partition=instance.partition)
        self.assertEqual(instance.counter, expected_value)

        # save after doing nothing
        instance.save()
        self.assertEqual(instance.counter, expected_value)

        # make sure there was no increment
        instance = TestCounterModel.get(partition=instance.partition)
        self.assertEqual(instance.counter, expected_value)
