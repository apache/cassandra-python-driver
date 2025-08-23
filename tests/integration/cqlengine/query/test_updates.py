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
from cassandra.cqlengine import ValidationError

from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine import columns

from tests.integration.cqlengine import is_prepend_reversed
from tests.integration.cqlengine.base import BaseCassEngTestCase, TestQueryUpdateModel
from tests.integration.cqlengine import execute_count
from tests.integration import greaterthancass20


class QueryUpdateTests(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(QueryUpdateTests, cls).setUpClass()
        sync_table(TestQueryUpdateModel)

    @classmethod
    def tearDownClass(cls):
        super(QueryUpdateTests, cls).tearDownClass()
        drop_table(TestQueryUpdateModel)

    @execute_count(8)
    def test_update_values(self):
        """ tests calling udpate on a queryset """
        partition = uuid4()
        for i in range(5):
            TestQueryUpdateModel.create(partition=partition, cluster=i, count=i, text=str(i))

        # sanity check
        for i, row in enumerate(TestQueryUpdateModel.objects(partition=partition)):
            self.assertEqual(row.cluster, i)
            self.assertEqual(row.count, i)
            self.assertEqual(row.text, str(i))

        # perform update
        TestQueryUpdateModel.objects(partition=partition, cluster=3).update(count=6)

        for i, row in enumerate(TestQueryUpdateModel.objects(partition=partition)):
            self.assertEqual(row.cluster, i)
            self.assertEqual(row.count, 6 if i == 3 else i)
            self.assertEqual(row.text, str(i))

    @execute_count(6)
    def test_update_values_validation(self):
        """ tests calling udpate on models with values passed in """
        partition = uuid4()
        for i in range(5):
            TestQueryUpdateModel.create(partition=partition, cluster=i, count=i, text=str(i))

        # sanity check
        for i, row in enumerate(TestQueryUpdateModel.objects(partition=partition)):
            self.assertEqual(row.cluster, i)
            self.assertEqual(row.count, i)
            self.assertEqual(row.text, str(i))

        # perform update
        with self.assertRaises(ValidationError):
            TestQueryUpdateModel.objects(partition=partition, cluster=3).update(count='asdf')

    def test_invalid_update_kwarg(self):
        """ tests that passing in a kwarg to the update method that isn't a column will fail """
        with self.assertRaises(ValidationError):
            TestQueryUpdateModel.objects(partition=uuid4(), cluster=3).update(bacon=5000)

    def test_primary_key_update_failure(self):
        """ tests that attempting to update the value of a primary key will fail """
        with self.assertRaises(ValidationError):
            TestQueryUpdateModel.objects(partition=uuid4(), cluster=3).update(cluster=5000)

    @execute_count(8)
    def test_null_update_deletes_column(self):
        """ setting a field to null in the update should issue a delete statement """
        partition = uuid4()
        for i in range(5):
            TestQueryUpdateModel.create(partition=partition, cluster=i, count=i, text=str(i))

        # sanity check
        for i, row in enumerate(TestQueryUpdateModel.objects(partition=partition)):
            self.assertEqual(row.cluster, i)
            self.assertEqual(row.count, i)
            self.assertEqual(row.text, str(i))

        # perform update
        TestQueryUpdateModel.objects(partition=partition, cluster=3).update(text=None)

        for i, row in enumerate(TestQueryUpdateModel.objects(partition=partition)):
            self.assertEqual(row.cluster, i)
            self.assertEqual(row.count, i)
            self.assertEqual(row.text, None if i == 3 else str(i))

    @execute_count(9)
    def test_mixed_value_and_null_update(self):
        """ tests that updating a columns value, and removing another works properly """
        partition = uuid4()
        for i in range(5):
            TestQueryUpdateModel.create(partition=partition, cluster=i, count=i, text=str(i))

        # sanity check
        for i, row in enumerate(TestQueryUpdateModel.objects(partition=partition)):
            self.assertEqual(row.cluster, i)
            self.assertEqual(row.count, i)
            self.assertEqual(row.text, str(i))

        # perform update
        TestQueryUpdateModel.objects(partition=partition, cluster=3).update(count=6, text=None)

        for i, row in enumerate(TestQueryUpdateModel.objects(partition=partition)):
            self.assertEqual(row.cluster, i)
            self.assertEqual(row.count, 6 if i == 3 else i)
            self.assertEqual(row.text, None if i == 3 else str(i))

    @execute_count(3)
    def test_set_add_updates(self):
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects.create(
                partition=partition, cluster=cluster, text_set=set(("foo",)))
        TestQueryUpdateModel.objects(
                partition=partition, cluster=cluster).update(text_set__add=set(('bar',)))
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(obj.text_set, set(("foo", "bar")))

    @execute_count(2)
    def test_set_add_updates_new_record(self):
        """ If the key doesn't exist yet, an update creates the record
        """
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects(
                partition=partition, cluster=cluster).update(text_set__add=set(('bar',)))
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(obj.text_set, set(("bar",)))

    @execute_count(3)
    def test_set_remove_updates(self):
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects.create(
                partition=partition, cluster=cluster, text_set=set(("foo", "baz")))
        TestQueryUpdateModel.objects(
                partition=partition, cluster=cluster).update(
                text_set__remove=set(('foo',)))
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(obj.text_set, set(("baz",)))

    @execute_count(3)
    def test_set_remove_new_record(self):
        """ Removing something not in the set should silently do nothing
        """
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects.create(
                partition=partition, cluster=cluster, text_set=set(("foo",)))
        TestQueryUpdateModel.objects(
                partition=partition, cluster=cluster).update(
                text_set__remove=set(('afsd',)))
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(obj.text_set, set(("foo",)))

    @execute_count(3)
    def test_list_append_updates(self):
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects.create(
                partition=partition, cluster=cluster, text_list=["foo"])
        TestQueryUpdateModel.objects(
                partition=partition, cluster=cluster).update(
                text_list__append=['bar'])
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(obj.text_list, ["foo", "bar"])

    @execute_count(3)
    def test_list_prepend_updates(self):
        """ Prepend two things since order is reversed by default by CQL """
        partition = uuid4()
        cluster = 1
        original = ["foo"]
        TestQueryUpdateModel.objects.create(
                partition=partition, cluster=cluster, text_list=original)
        prepended = ['bar', 'baz']
        TestQueryUpdateModel.objects(
                partition=partition, cluster=cluster).update(
                text_list__prepend=prepended)
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        expected = (prepended[::-1] if is_prepend_reversed() else prepended) + original
        self.assertEqual(obj.text_list, expected)

    @execute_count(3)
    def test_map_update_updates(self):
        """ Merge a dictionary into existing value """
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects.create(
                partition=partition, cluster=cluster,
                text_map={"foo": '1', "bar": '2'})
        TestQueryUpdateModel.objects(
                partition=partition, cluster=cluster).update(
                text_map__update={"bar": '3', "baz": '4'})
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(obj.text_map, {"foo": '1', "bar": '3', "baz": '4'})

    @execute_count(3)
    def test_map_update_none_deletes_key(self):
        """ The CQL behavior is if you set a key in a map to null it deletes
        that key from the map.  Test that this works with __update.
        """
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects.create(
                partition=partition, cluster=cluster,
                text_map={"foo": '1', "bar": '2'})
        TestQueryUpdateModel.objects(
                partition=partition, cluster=cluster).update(
                text_map__update={"bar": None})
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(obj.text_map, {"foo": '1'})

    @greaterthancass20
    @execute_count(5)
    def test_map_update_remove(self):
        """
        Test that map item removal with update(<columnname>__remove=...) works

        @jira_ticket PYTHON-688
        """
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects.create(
            partition=partition,
            cluster=cluster,
            text_map={"foo": '1', "bar": '2'}
        )
        TestQueryUpdateModel.objects(partition=partition, cluster=cluster).update(
            text_map__remove={"bar"},
            text_map__update={"foz": '4', "foo": '2'}
        )
        obj = TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(obj.text_map, {"foo": '2', "foz": '4'})

        TestQueryUpdateModel.objects(partition=partition, cluster=cluster).update(
            text_map__remove={"foo", "foz"}
        )
        self.assertEqual(
            TestQueryUpdateModel.objects.get(partition=partition, cluster=cluster).text_map,
            {}
        )

    def test_map_remove_rejects_non_sets(self):
        """
        Map item removal requires a set to match the CQL API

        @jira_ticket PYTHON-688
        """
        partition = uuid4()
        cluster = 1
        TestQueryUpdateModel.objects.create(
            partition=partition,
            cluster=cluster,
            text_map={"foo": '1', "bar": '2'}
        )
        with self.assertRaises(ValidationError):
            TestQueryUpdateModel.objects(partition=partition, cluster=cluster).update(
                text_map__remove=["bar"]
            )

    @execute_count(3)
    def test_an_extra_delete_is_not_sent(self):
        """
        Test to ensure that an extra DELETE is not sent if an object is read
        from the DB with a None value

        @since 3.9
        @jira_ticket PYTHON-719
        @expected_result only three queries are executed, the first one for
        inserting the object, the second one for reading it, and the third
        one for updating it

        @test_category object_mapper
        """
        partition = uuid4()
        cluster = 1

        TestQueryUpdateModel.objects.create(
            partition=partition, cluster=cluster)

        obj = TestQueryUpdateModel.objects(
            partition=partition, cluster=cluster).first()

        self.assertFalse({k: v for (k, v) in obj._values.items() if v.deleted})

        obj.text = 'foo'
        obj.save()
        #execute_count will check the execution count and
        #assert no more calls than necessary where made

class StaticDeleteModel(Model):
    example_id = columns.Integer(partition_key=True, primary_key=True, default=uuid4)
    example_static1 = columns.Integer(static=True)
    example_static2 = columns.Integer(static=True)
    example_clust = columns.Integer(primary_key=True)


class StaticDeleteTests(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(StaticDeleteTests, cls).setUpClass()
        sync_table(StaticDeleteModel)

    @classmethod
    def tearDownClass(cls):
        super(StaticDeleteTests, cls).tearDownClass()
        drop_table(StaticDeleteModel)

    def test_static_deletion(self):
        """
        Test to ensure that cluster keys are not included when removing only static columns

        @since 3.6
        @jira_ticket PYTHON-608
        @expected_result Server should not throw an exception, and the static column should be deleted

        @test_category object_mapper
        """
        StaticDeleteModel.create(example_id=5, example_clust=5, example_static2=1)
        sdm = StaticDeleteModel.filter(example_id=5).first()
        self.assertEqual(1, sdm.example_static2)
        sdm.update(example_static2=None)
        self.assertIsNone(sdm.example_static2)
