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

from uuid import uuid4, UUID
import random
from datetime import datetime, date, time, timezone
from decimal import Decimal
from operator import itemgetter

import cassandra
from cassandra.cqlengine import columns
from cassandra.cqlengine import CQLEngineException
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.management import drop_table
from cassandra.cqlengine.models import Model
from cassandra.query import SimpleStatement
from cassandra.util import Date, Time, Duration
from cassandra.cqlengine.statements import SelectStatement, DeleteStatement, WhereClause
from cassandra.cqlengine.operators import EqualsOperator

from tests.integration import PROTOCOL_VERSION, greaterthanorequalcass3_10
from tests.integration.cqlengine.base import BaseCassEngTestCase
from tests.integration.cqlengine import DEFAULT_KEYSPACE


class TestModel(Model):

    id = columns.UUID(primary_key=True, default=lambda: uuid4())
    count = columns.Integer()
    text = columns.Text(required=False)
    a_bool = columns.Boolean(default=False)


class TestModelSave(Model):
    partition = columns.UUID(primary_key=True, default=uuid4)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer(required=False)
    text = columns.Text(required=False, index=True)
    text_set = columns.Set(columns.Text, required=False)
    text_list = columns.List(columns.Text, required=False)
    text_map = columns.Map(columns.Text, columns.Text, required=False)


class TestModelIO(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestModelIO, cls).setUpClass()
        sync_table(TestModel)

    @classmethod
    def tearDownClass(cls):
        super(TestModelIO, cls).tearDownClass()
        drop_table(TestModel)

    def test_model_save_and_load(self):
        """
        Tests that models can be saved and retrieved, using the create method.
        """
        tm = TestModel.create(count=8, text='123456789')
        self.assertIsInstance(tm, TestModel)

        tm2 = TestModel.objects(id=tm.pk).first()
        self.assertIsInstance(tm2, TestModel)

        for cname in tm._columns.keys():
            self.assertEqual(getattr(tm, cname), getattr(tm2, cname))

    def test_model_instantiation_save_and_load(self):
        """
        Tests that models can be saved and retrieved, this time using the
        natural model instantiation.
        """
        tm = TestModel(count=8, text='123456789')
        # Tests that values are available on instantiation.
        self.assertIsNotNone(tm['id'])
        self.assertEqual(tm.count, 8)
        self.assertEqual(tm.text, '123456789')
        tm.save()
        tm2 = TestModel.objects(id=tm.id).first()

        for cname in tm._columns.keys():
            self.assertEqual(getattr(tm, cname), getattr(tm2, cname))

    def test_model_read_as_dict(self):
        """
        Tests that columns of an instance can be read as a dict.
        """
        tm = TestModel.create(count=8, text='123456789', a_bool=True)
        column_dict = {
            'id': tm.id,
            'count': tm.count,
            'text': tm.text,
            'a_bool': tm.a_bool,
        }
        self.assertEqual(sorted(tm.keys()), sorted(column_dict.keys()))

        self.assertSetEqual(set(tm.values()), set(column_dict.values()))
        self.assertEqual(
            sorted(tm.items(), key=itemgetter(0)),
            sorted(column_dict.items(), key=itemgetter(0)))
        self.assertEqual(len(tm), len(column_dict))
        for column_id in column_dict.keys():
            self.assertEqual(tm[column_id], column_dict[column_id])

        tm['count'] = 6
        self.assertEqual(tm.count, 6)

    def test_model_updating_works_properly(self):
        """
        Tests that subsequent saves after initial model creation work
        """
        tm = TestModel.objects.create(count=8, text='123456789')

        tm.count = 100
        tm.a_bool = True
        tm.save()

        tm2 = TestModel.objects(id=tm.pk).first()
        self.assertEqual(tm.count, tm2.count)
        self.assertEqual(tm.a_bool, tm2.a_bool)

    def test_model_deleting_works_properly(self):
        """
        Tests that an instance's delete method deletes the instance
        """
        tm = TestModel.create(count=8, text='123456789')
        tm.delete()
        tm2 = TestModel.objects(id=tm.pk).first()
        self.assertIsNone(tm2)

    def test_column_deleting_works_properly(self):
        """
        """
        tm = TestModel.create(count=8, text='123456789')
        tm.text = None
        tm.save()

        tm2 = TestModel.objects(id=tm.pk).first()
        self.assertIsInstance(tm2, TestModel)

        self.assertTrue(tm2.text is None)
        self.assertTrue(tm2._values['text'].previous_value is None)

    def test_a_sensical_error_is_raised_if_you_try_to_create_a_table_twice(self):
        """
        """
        sync_table(TestModel)
        sync_table(TestModel)

    @greaterthanorequalcass3_10
    def test_can_insert_model_with_all_column_types(self):
        """
        Test for inserting all column types into a Model

        test_can_insert_model_with_all_column_types tests that each cqlengine column type can be inserted into a Model.
        It first creates a Model that has each cqlengine column type. It then creates a Model instance where all the fields
        have corresponding data, which performs the insert into the Cassandra table.
        Finally, it verifies that each column read from the Model from Cassandra is the same as the input parameters.

        @since 2.6.0
        @jira_ticket PYTHON-246
        @expected_result The Model is inserted with each column type, and the resulting read yields proper data for each column.

        @test_category data_types:primitive
        """

        class AllDatatypesModel(Model):
            id = columns.Integer(primary_key=True)
            a = columns.Ascii()
            b = columns.BigInt()
            c = columns.Blob()
            d = columns.Boolean()
            e = columns.DateTime()
            f = columns.Decimal()
            g = columns.Double()
            h = columns.Float()
            i = columns.Inet()
            j = columns.Integer()
            k = columns.Text()
            l = columns.TimeUUID()
            m = columns.UUID()
            n = columns.VarInt()
            o = columns.Duration()

        sync_table(AllDatatypesModel)

        input = ['ascii', 2 ** 63 - 1, bytearray(b'hello world'), True, datetime.fromtimestamp(872835240, tz=timezone.utc).replace(tzinfo=None),
                 Decimal('12.3E+7'), 2.39, 3.4028234663852886e+38, '123.123.123.123', 2147483647, 'text',
                 UUID('FE2B4360-28C6-11E2-81C1-0800200C9A66'), UUID('067e6162-3b6f-4ae2-a171-2470b63dff00'),
                 int(str(2147483647) + '000')]

        AllDatatypesModel.create(id=0, a='ascii', b=2 ** 63 - 1, c=bytearray(b'hello world'), d=True,
                                 e=datetime.fromtimestamp(872835240, tz=timezone.utc), f=Decimal('12.3E+7'), g=2.39,
                                 h=3.4028234663852886e+38, i='123.123.123.123', j=2147483647, k='text',
                                 l=UUID('FE2B4360-28C6-11E2-81C1-0800200C9A66'),
                                 m=UUID('067e6162-3b6f-4ae2-a171-2470b63dff00'), n=int(str(2147483647) + '000'),
                                 o=Duration(2, 3, 4))

        self.assertEqual(1, AllDatatypesModel.objects.count())
        output = AllDatatypesModel.objects.first()

        for i, i_char in enumerate(range(ord('a'), ord('a') + 14)):
            self.assertEqual(input[i], output[chr(i_char)])

    def test_can_specify_none_instead_of_default(self):
        self.assertIsNotNone(TestModel.a_bool.column.default)

        # override default
        inst = TestModel.create(a_bool=None)
        self.assertIsNone(inst.a_bool)
        queried = TestModel.objects(id=inst.id).first()
        self.assertIsNone(queried.a_bool)

        # letting default be set
        inst = TestModel.create()
        self.assertEqual(inst.a_bool, TestModel.a_bool.column.default)
        queried = TestModel.objects(id=inst.id).first()
        self.assertEqual(queried.a_bool, TestModel.a_bool.column.default)

    def test_can_insert_model_with_all_protocol_v4_column_types(self):
        """
        Test for inserting all protocol v4 column types into a Model

        test_can_insert_model_with_all_protocol_v4_column_types tests that each cqlengine protocol v4 column type can be
        inserted into a Model. It first creates a Model that has each cqlengine protocol v4 column type. It then creates
        a Model instance where all the fields have corresponding data, which performs the insert into the Cassandra table.
        Finally, it verifies that each column read from the Model from Cassandra is the same as the input parameters.

        @since 2.6.0
        @jira_ticket PYTHON-245
        @expected_result The Model is inserted with each protocol v4 column type, and the resulting read yields proper data for each column.

        @test_category data_types:primitive
        """

        if PROTOCOL_VERSION < 4:
            raise unittest.SkipTest("Protocol v4 datatypes require native protocol 4+, currently using: {0}".format(PROTOCOL_VERSION))

        class v4DatatypesModel(Model):
            id = columns.Integer(primary_key=True)
            a = columns.Date()
            b = columns.SmallInt()
            c = columns.Time()
            d = columns.TinyInt()

        sync_table(v4DatatypesModel)

        input = [Date(date(1970, 1, 1)), 32523, Time(time(16, 47, 25, 7)), 123]

        v4DatatypesModel.create(id=0, a=date(1970, 1, 1), b=32523, c=time(16, 47, 25, 7), d=123)

        self.assertEqual(1, v4DatatypesModel.objects.count())
        output = v4DatatypesModel.objects.first()

        for i, i_char in enumerate(range(ord('a'), ord('a') + 3)):
            self.assertEqual(input[i], output[chr(i_char)])

    def test_can_insert_double_and_float(self):
        """
        Test for inserting single-precision and double-precision values into a Float and Double columns

        @since 2.6.0
        @changed 3.0.0 removed deprecated Float(double_precision) parameter
        @jira_ticket PYTHON-246
        @expected_result Each floating point column type is able to hold their respective precision values.

        @test_category data_types:primitive
        """

        class FloatingPointModel(Model):
            id = columns.Integer(primary_key=True)
            f = columns.Float()
            d = columns.Double()

        sync_table(FloatingPointModel)

        FloatingPointModel.create(id=0, f=2.39)
        output = FloatingPointModel.objects.first()
        self.assertEqual(2.390000104904175, output.f)  # float loses precision

        FloatingPointModel.create(id=0, f=3.4028234663852886e+38, d=2.39)
        output = FloatingPointModel.objects.first()
        self.assertEqual(3.4028234663852886e+38, output.f)
        self.assertEqual(2.39, output.d)  # double retains precision

        FloatingPointModel.create(id=0, d=3.4028234663852886e+38)
        output = FloatingPointModel.objects.first()
        self.assertEqual(3.4028234663852886e+38, output.d)


class TestMultiKeyModel(Model):

    partition = columns.Integer(primary_key=True)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer(required=False)
    text = columns.Text(required=False)


class TestDeleting(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestDeleting, cls).setUpClass()
        drop_table(TestMultiKeyModel)
        sync_table(TestMultiKeyModel)

    @classmethod
    def tearDownClass(cls):
        super(TestDeleting, cls).tearDownClass()
        drop_table(TestMultiKeyModel)

    def test_deleting_only_deletes_one_object(self):
        partition = random.randint(0, 1000)
        for i in range(5):
            TestMultiKeyModel.create(partition=partition, cluster=i, count=i, text=str(i))

        self.assertTrue(TestMultiKeyModel.filter(partition=partition).count() == 5)

        TestMultiKeyModel.get(partition=partition, cluster=0).delete()

        self.assertTrue(TestMultiKeyModel.filter(partition=partition).count() == 4)

        TestMultiKeyModel.filter(partition=partition).delete()


class TestUpdating(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestUpdating, cls).setUpClass()
        drop_table(TestModelSave)
        drop_table(TestMultiKeyModel)
        sync_table(TestModelSave)
        sync_table(TestMultiKeyModel)

    @classmethod
    def tearDownClass(cls):
        super(TestUpdating, cls).tearDownClass()
        drop_table(TestMultiKeyModel)
        drop_table(TestModelSave)

    def setUp(self):
        super(TestUpdating, self).setUp()
        self.instance = TestMultiKeyModel.create(
            partition=random.randint(0, 1000),
            cluster=random.randint(0, 1000),
            count=0,
            text='happy'
        )

    def test_vanilla_update(self):
        self.instance.count = 5
        self.instance.save()

        check = TestMultiKeyModel.get(partition=self.instance.partition, cluster=self.instance.cluster)
        self.assertTrue(check.count == 5)
        self.assertTrue(check.text == 'happy')

    def test_deleting_only(self):
        self.instance.count = None
        self.instance.text = None
        self.instance.save()

        check = TestMultiKeyModel.get(partition=self.instance.partition, cluster=self.instance.cluster)
        self.assertTrue(check.count is None)
        self.assertTrue(check.text is None)

    def test_get_changed_columns(self):
        self.assertTrue(self.instance.get_changed_columns() == [])
        self.instance.count = 1
        changes = self.instance.get_changed_columns()
        self.assertTrue(len(changes) == 1)
        self.assertTrue(changes == ['count'])
        self.instance.save()
        self.assertTrue(self.instance.get_changed_columns() == [])

    def test_previous_value_tracking_of_persisted_instance(self):
        # Check initial internal states.
        self.assertTrue(self.instance.get_changed_columns() == [])
        self.assertTrue(self.instance._values['count'].previous_value == 0)

        # Change value and check internal states.
        self.instance.count = 1
        self.assertTrue(self.instance.get_changed_columns() == ['count'])
        self.assertTrue(self.instance._values['count'].previous_value == 0)

        # Internal states should be updated on save.
        self.instance.save()
        self.assertTrue(self.instance.get_changed_columns() == [])
        self.assertTrue(self.instance._values['count'].previous_value == 1)

        # Change value twice.
        self.instance.count = 2
        self.assertTrue(self.instance.get_changed_columns() == ['count'])
        self.assertTrue(self.instance._values['count'].previous_value == 1)
        self.instance.count = 3
        self.assertTrue(self.instance.get_changed_columns() == ['count'])
        self.assertTrue(self.instance._values['count'].previous_value == 1)

        # Internal states updated on save.
        self.instance.save()
        self.assertTrue(self.instance.get_changed_columns() == [])
        self.assertTrue(self.instance._values['count'].previous_value == 3)

        # Change value and reset it.
        self.instance.count = 2
        self.assertTrue(self.instance.get_changed_columns() == ['count'])
        self.assertTrue(self.instance._values['count'].previous_value == 3)
        self.instance.count = 3
        self.assertTrue(self.instance.get_changed_columns() == [])
        self.assertTrue(self.instance._values['count'].previous_value == 3)

        # Nothing to save: values in initial conditions.
        self.instance.save()
        self.assertTrue(self.instance.get_changed_columns() == [])
        self.assertTrue(self.instance._values['count'].previous_value == 3)

        # Change Multiple values
        self.instance.count = 4
        self.instance.text = "changed"
        self.assertTrue(len(self.instance.get_changed_columns()) == 2)
        self.assertTrue('text' in self.instance.get_changed_columns())
        self.assertTrue('count' in self.instance.get_changed_columns())
        self.instance.save()
        self.assertTrue(self.instance.get_changed_columns() == [])

        # Reset Multiple Values
        self.instance.count = 5
        self.instance.text = "changed"
        self.assertTrue(self.instance.get_changed_columns() == ['count'])
        self.instance.text = "changed2"
        self.assertTrue(len(self.instance.get_changed_columns()) == 2)
        self.assertTrue('text' in self.instance.get_changed_columns())
        self.assertTrue('count' in self.instance.get_changed_columns())
        self.instance.count = 4
        self.instance.text = "changed"
        self.assertTrue(self.instance.get_changed_columns() == [])

    def test_previous_value_tracking_on_instantiation(self):
        self.instance = TestMultiKeyModel(
            partition=random.randint(0, 1000),
            cluster=random.randint(0, 1000),
            count=0,
            text='happy')

        # Columns of instances not persisted yet should be marked as changed.
        self.assertTrue(set(self.instance.get_changed_columns()) == set([
            'partition', 'cluster', 'count', 'text']))
        self.assertTrue(self.instance._values['partition'].previous_value is None)
        self.assertTrue(self.instance._values['cluster'].previous_value is None)
        self.assertTrue(self.instance._values['count'].previous_value is None)
        self.assertTrue(self.instance._values['text'].previous_value is None)

        # Value changes doesn't affect internal states.
        self.instance.count = 1
        self.assertTrue('count' in self.instance.get_changed_columns())
        self.assertTrue(self.instance._values['count'].previous_value is None)
        self.instance.count = 2
        self.assertTrue('count' in self.instance.get_changed_columns())
        self.assertTrue(self.instance._values['count'].previous_value is None)

        # Value reset is properly tracked.
        self.instance.count = None
        self.assertTrue('count' not in self.instance.get_changed_columns())
        self.assertTrue(self.instance._values['count'].previous_value is None)

        self.instance.save()
        self.assertTrue(self.instance.get_changed_columns() == [])
        self.assertTrue(self.instance._values['count'].previous_value is None)
        self.assertTrue(self.instance.count is None)

    def test_previous_value_tracking_on_instantiation_with_default(self):

        class TestDefaultValueTracking(Model):
            id = columns.Integer(partition_key=True)
            int1 = columns.Integer(default=123)
            int2 = columns.Integer(default=456)
            int3 = columns.Integer(default=lambda: random.randint(0, 1000))
            int4 = columns.Integer(default=lambda: random.randint(0, 1000))
            int5 = columns.Integer()
            int6 = columns.Integer()

        instance = TestDefaultValueTracking(
            id=1,
            int1=9999,
            int3=7777,
            int5=5555)

        self.assertEqual(instance.id, 1)
        self.assertEqual(instance.int1, 9999)
        self.assertEqual(instance.int2, 456)
        self.assertEqual(instance.int3, 7777)
        self.assertIsNotNone(instance.int4)
        self.assertIsInstance(instance.int4, int)
        self.assertGreaterEqual(instance.int4, 0)
        self.assertLessEqual(instance.int4, 1000)
        self.assertEqual(instance.int5, 5555)
        self.assertTrue(instance.int6 is None)

        # All previous values are unset as the object hasn't been persisted
        # yet.
        self.assertTrue(instance._values['id'].previous_value is None)
        self.assertTrue(instance._values['int1'].previous_value is None)
        self.assertTrue(instance._values['int2'].previous_value is None)
        self.assertTrue(instance._values['int3'].previous_value is None)
        self.assertTrue(instance._values['int4'].previous_value is None)
        self.assertTrue(instance._values['int5'].previous_value is None)
        self.assertTrue(instance._values['int6'].previous_value is None)

        # All explicitely set columns, and those with default values are
        # flagged has changed.
        self.assertTrue(set(instance.get_changed_columns()) == set([
            'id', 'int1', 'int3', 'int5']))

    def test_save_to_none(self):
        """
        Test update of column value of None with save() function.

        Under specific scenarios calling save on a None value wouldn't update
        previous values. This issue only manifests with a new instantiation of the model,
        if existing model is modified and updated the issue will not occur.

        @since 3.0.0
        @jira_ticket PYTHON-475
        @expected_result column value should be updated to None

        @test_category object_mapper
        """

        partition = uuid4()
        cluster = 1
        text = 'set'
        text_list = ['set']
        text_set = set(("set",))
        text_map = {"set": 'set'}
        initial = TestModelSave(partition=partition, cluster=cluster, text=text, text_list=text_list,
                                text_set=text_set, text_map=text_map)
        initial.save()
        current = TestModelSave.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(current.text, text)
        self.assertEqual(current.text_list, text_list)
        self.assertEqual(current.text_set, text_set)
        self.assertEqual(current.text_map, text_map)

        next = TestModelSave(partition=partition, cluster=cluster, text=None, text_list=None,
                            text_set=None, text_map=None)

        next.save()
        current = TestModelSave.objects.get(partition=partition, cluster=cluster)
        self.assertEqual(current.text, None)
        self.assertEqual(current.text_list, [])
        self.assertEqual(current.text_set, set())
        self.assertEqual(current.text_map, {})


def test_none_filter_fails():
    class NoneFilterModel(Model):

        pk = columns.Integer(primary_key=True)
        v = columns.Integer()
    sync_table(NoneFilterModel)

    try:
        NoneFilterModel.objects(pk=None)
        raise Exception("fail")
    except CQLEngineException as e:
        pass


class TestCanUpdate(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestCanUpdate, cls).setUpClass()
        drop_table(TestModel)
        sync_table(TestModel)

    @classmethod
    def tearDownClass(cls):
        super(TestCanUpdate, cls).tearDownClass()
        drop_table(TestModel)

    def test_success_case(self):
        tm = TestModel(count=8, text='123456789')

        # object hasn't been saved,
        # shouldn't be able to update
        self.assertTrue(not tm._is_persisted)
        self.assertTrue(not tm._can_update())

        tm.save()

        # object has been saved,
        # should be able to update
        self.assertTrue(tm._is_persisted)
        self.assertTrue(tm._can_update())

        tm.count = 200

        # primary keys haven't changed,
        # should still be able to update
        self.assertTrue(tm._can_update())
        tm.save()

        tm.id = uuid4()

        # primary keys have changed,
        # should not be able to update
        self.assertTrue(not tm._can_update())


class IndexDefinitionModel(Model):

    key = columns.UUID(primary_key=True)
    val = columns.Text(index=True)


class TestIndexedColumnDefinition(BaseCassEngTestCase):

    def test_exception_isnt_raised_if_an_index_is_defined_more_than_once(self):
        sync_table(IndexDefinitionModel)
        sync_table(IndexDefinitionModel)


class ReservedWordModel(Model):

    token = columns.Text(primary_key=True)
    insert = columns.Integer(index=True)


class TestQueryQuoting(BaseCassEngTestCase):

    def test_reserved_cql_words_can_be_used_as_column_names(self):
        """
        """
        sync_table(ReservedWordModel)

        model1 = ReservedWordModel.create(token='1', insert=5)

        model2 = ReservedWordModel.filter(token='1')

        self.assertTrue(len(model2) == 1)
        self.assertTrue(model1.token == model2[0].token)
        self.assertTrue(model1.insert == model2[0].insert)


class TestQueryModel(Model):

    test_id = columns.UUID(primary_key=True, default=uuid4)
    date = columns.Date(primary_key=True)
    description = columns.Text()


class TestQuerying(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        if PROTOCOL_VERSION < 4:
            return

        super(TestQuerying, cls).setUpClass()
        drop_table(TestQueryModel)
        sync_table(TestQueryModel)

    @classmethod
    def tearDownClass(cls):
        if PROTOCOL_VERSION < 4:
            return

        super(TestQuerying, cls).tearDownClass()
        drop_table(TestQueryModel)

    def setUp(self):
        if PROTOCOL_VERSION < 4:
            raise unittest.SkipTest("Date query tests require native protocol 4+, currently using: {0}".format(PROTOCOL_VERSION))

    def test_query_with_date(self):
        uid = uuid4()
        day = date(2013, 11, 26)
        obj = TestQueryModel.create(test_id=uid, date=day, description=u'foo')

        self.assertEqual(obj.description, u'foo')

        inst = TestQueryModel.filter(
            TestQueryModel.test_id == uid,
            TestQueryModel.date == day).limit(1).first()

        self.assertTrue(inst.test_id == uid)
        self.assertTrue(inst.date == day)


class BasicModelNoRouting(Model):
    __table_name__ = 'basic_model_no_routing'
    __compute_routing_key__ = False
    k = columns.Integer(primary_key=True)
    v = columns.Integer()


class BasicModel(Model):
    __table_name__ = 'basic_model_routing'
    k = columns.Integer(primary_key=True)
    v = columns.Integer()


class BasicModelMulti(Model):
    __table_name__ = 'basic_model_routing_multi'
    k = columns.Integer(partition_key=True)
    v = columns.Integer(partition_key=True)


class ComplexModelRouting(Model):
    __table_name__ = 'complex_model_routing'
    partition = columns.UUID(partition_key=True, default=uuid4)
    cluster = columns.Integer(partition_key=True)
    count = columns.Integer()
    text = columns.Text(partition_key=True)
    float = columns.Float(partition_key=True)
    text_2 = columns.Text()


class TestModelRoutingKeys(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestModelRoutingKeys, cls).setUpClass()
        sync_table(BasicModelNoRouting)
        sync_table(BasicModel)
        sync_table(BasicModelMulti)
        sync_table(ComplexModelRouting)

    @classmethod
    def tearDownClass(cls):
        super(TestModelRoutingKeys, cls).tearDownClass()
        drop_table(BasicModelNoRouting)
        drop_table(BasicModel)
        drop_table(BasicModelMulti)
        drop_table(ComplexModelRouting)

    def test_routing_key_is_ignored(self):
        """
        Compares the routing key generated by simple partition key using the model with the one generated by the equivalent
        bound statement. It also verifies basic operations work with no routing key
        @since 3.2
        @jira_ticket PYTHON-505
        @expected_result they shouldn't match

        @test_category object_mapper
        """

        prepared = self.session.prepare(
            """
          INSERT INTO {0}.basic_model_no_routing (k, v) VALUES  (?, ?)
          """.format(DEFAULT_KEYSPACE))
        bound = prepared.bind((1, 2))

        mrk = BasicModelNoRouting._routing_key_from_values([1], self.session.cluster.protocol_version)
        simple = SimpleStatement("")
        simple.routing_key = mrk
        self.assertNotEqual(bound.routing_key, simple.routing_key)

        # Verify that basic create, update and delete work with no routing key
        t = BasicModelNoRouting.create(k=2, v=3)
        t.update(v=4).save()
        f = BasicModelNoRouting.objects.filter(k=2).first()
        self.assertEqual(t, f)

        t.delete()
        self.assertEqual(BasicModelNoRouting.objects.count(), 0)


    def test_routing_key_generation_basic(self):
        """
        Compares the routing key generated by simple partition key using the model with the one generated by the equivalent
        bound statement
        @since 3.2
        @jira_ticket PYTHON-535
        @expected_result they should match

        @test_category object_mapper
        """

        prepared = self.session.prepare(
            """
          INSERT INTO {0}.basic_model_routing (k, v) VALUES  (?, ?)
          """.format(DEFAULT_KEYSPACE))
        bound = prepared.bind((1, 2))

        mrk = BasicModel._routing_key_from_values([1], self.session.cluster.protocol_version)
        simple = SimpleStatement("")
        simple.routing_key = mrk
        self.assertEqual(bound.routing_key, simple.routing_key)

    def test_routing_key_generation_multi(self):
        """
        Compares the routing key generated by composite partition key using the model with the one generated by the equivalent
        bound statement
        @since 3.2
        @jira_ticket PYTHON-535
        @expected_result they should match

        @test_category object_mapper
        """

        prepared = self.session.prepare(
            """
          INSERT INTO {0}.basic_model_routing_multi (k, v) VALUES  (?, ?)
          """.format(DEFAULT_KEYSPACE))
        bound = prepared.bind((1, 2))
        mrk = BasicModelMulti._routing_key_from_values([1, 2], self.session.cluster.protocol_version)
        simple = SimpleStatement("")
        simple.routing_key = mrk
        self.assertEqual(bound.routing_key, simple.routing_key)

    def test_routing_key_generation_complex(self):
        """
        Compares the routing key generated by complex composite partition key using the model with the one generated by the equivalent
        bound statement
        @since 3.2
        @jira_ticket PYTHON-535
        @expected_result they should match

        @test_category object_mapper
        """
        prepared = self.session.prepare(
            """
          INSERT INTO {0}.complex_model_routing (partition, cluster, count, text, float, text_2) VALUES  (?, ?, ?, ?, ?, ?)
          """.format(DEFAULT_KEYSPACE))
        partition = uuid4()
        cluster = 1
        count = 2
        text = "text"
        float = 1.2
        text_2 = "text_2"
        bound = prepared.bind((partition, cluster, count, text, float, text_2))
        mrk = ComplexModelRouting._routing_key_from_values([partition, cluster, text, float], self.session.cluster.protocol_version)
        simple = SimpleStatement("")
        simple.routing_key = mrk
        self.assertEqual(bound.routing_key, simple.routing_key)

    def test_partition_key_index(self):
        """
        Test to ensure that statement partition key generation is in the correct order
        @since 3.2
        @jira_ticket PYTHON-535
        @expected_result .

        @test_category object_mapper
        """
        self._check_partition_value_generation(BasicModel, SelectStatement(BasicModel.__table_name__))
        self._check_partition_value_generation(BasicModel, DeleteStatement(BasicModel.__table_name__))
        self._check_partition_value_generation(BasicModelMulti, SelectStatement(BasicModelMulti.__table_name__))
        self._check_partition_value_generation(BasicModelMulti, DeleteStatement(BasicModelMulti.__table_name__))
        self._check_partition_value_generation(ComplexModelRouting, SelectStatement(ComplexModelRouting.__table_name__))
        self._check_partition_value_generation(ComplexModelRouting, DeleteStatement(ComplexModelRouting.__table_name__))
        self._check_partition_value_generation(BasicModel, SelectStatement(BasicModel.__table_name__), reverse=True)
        self._check_partition_value_generation(BasicModel, DeleteStatement(BasicModel.__table_name__), reverse=True)
        self._check_partition_value_generation(BasicModelMulti, SelectStatement(BasicModelMulti.__table_name__), reverse=True)
        self._check_partition_value_generation(BasicModelMulti, DeleteStatement(BasicModelMulti.__table_name__), reverse=True)
        self._check_partition_value_generation(ComplexModelRouting, SelectStatement(ComplexModelRouting.__table_name__), reverse=True)
        self._check_partition_value_generation(ComplexModelRouting, DeleteStatement(ComplexModelRouting.__table_name__), reverse=True)

    def _check_partition_value_generation(self, model, state, reverse=False):
        """
        This generates a some statements based on the partition_key_index of the model.
        It then validates that order of the partition key values in the statement matches the index
        specified in the models partition_key_index
        """
        # Setup some unique values for statement generation
        uuid = uuid4()
        values = {'k': 5, 'v': 3, 'partition': uuid, 'cluster': 6, 'count': 42, 'text': 'text', 'float': 3.1415, 'text_2': 'text_2'}
        res = dict((v, k) for k, v in values.items())
        items = list(model._partition_key_index.items())
        if(reverse):
            items.reverse()
        # Add where clauses for each partition key
        for partition_key, position in items:
            wc = WhereClause(partition_key, EqualsOperator(), values.get(partition_key))
            state._add_where_clause(wc)

        # Iterate over the partition key values check to see that their index matches
        # Those specified in the models partition field
        for indx, value in enumerate(state.partition_key_values(model._partition_key_index)):
            name = res.get(value)
            self.assertEqual(indx, model._partition_key_index.get(name))


def test_none_filter_fails():
    class NoneFilterModel(Model):

        pk = columns.Integer(primary_key=True)
        v = columns.Integer()
    sync_table(NoneFilterModel)

    try:
        NoneFilterModel.objects(pk=None)
        raise Exception("fail")
    except CQLEngineException as e:
        pass
