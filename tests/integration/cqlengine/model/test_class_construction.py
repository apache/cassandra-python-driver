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
import warnings

from cassandra.cqlengine import columns, CQLEngineException
from cassandra.cqlengine.models import Model, ModelException, ModelDefinitionException, ColumnQueryEvaluator
from cassandra.cqlengine.query import ModelQuerySet, DMLQuery

from tests.integration.cqlengine.base import BaseCassEngTestCase


class TestModelClassFunction(BaseCassEngTestCase):
    """
    Tests verifying the behavior of the Model metaclass
    """

    def test_column_attributes_handled_correctly(self):
        """
        Tests that column attributes are moved to a _columns dict
        and replaced with simple value attributes
        """

        class TestModel(Model):

            id = columns.UUID(primary_key=True, default=lambda:uuid4())
            text = columns.Text()

        # check class attibutes
        self.assertHasAttr(TestModel, '_columns')
        self.assertHasAttr(TestModel, 'id')
        self.assertHasAttr(TestModel, 'text')

        # check instance attributes
        inst = TestModel()
        self.assertHasAttr(inst, 'id')
        self.assertHasAttr(inst, 'text')
        self.assertIsNotNone(inst.id)
        self.assertIsNone(inst.text)

    def test_values_on_instantiation(self):
        """
        Tests defaults and user-provided values on instantiation.
        """

        class TestPerson(Model):
            first_name = columns.Text(primary_key=True, default='kevin')
            last_name = columns.Text(default='deldycke')

        # Check that defaults are available at instantiation.
        inst1 = TestPerson()
        self.assertHasAttr(inst1, 'first_name')
        self.assertHasAttr(inst1, 'last_name')
        self.assertEqual(inst1.first_name, 'kevin')
        self.assertEqual(inst1.last_name, 'deldycke')

        # Check that values on instantiation overrides defaults.
        inst2 = TestPerson(first_name='bob', last_name='joe')
        self.assertEqual(inst2.first_name, 'bob')
        self.assertEqual(inst2.last_name, 'joe')

    def test_db_map(self):
        """
        Tests that the db_map is properly defined
        -the db_map allows columns
        """
        class WildDBNames(Model):

            id = columns.UUID(primary_key=True, default=lambda:uuid4())
            content = columns.Text(db_field='words_and_whatnot')
            numbers = columns.Integer(db_field='integers_etc')

        db_map = WildDBNames._db_map
        self.assertEqual(db_map['words_and_whatnot'], 'content')
        self.assertEqual(db_map['integers_etc'], 'numbers')

    def test_attempting_to_make_duplicate_column_names_fails(self):
        """
        Tests that trying to create conflicting db column names will fail
        """

        with self.assertRaisesRegex(ModelException, r".*more than once$"):
            class BadNames(Model):
                words = columns.Text(primary_key=True)
                content = columns.Text(db_field='words')

    def test_column_ordering_is_preserved(self):
        """
        Tests that the _columns dics retains the ordering of the class definition
        """

        class Stuff(Model):

            id  = columns.UUID(primary_key=True, default=lambda:uuid4())
            words = columns.Text()
            content = columns.Text()
            numbers = columns.Integer()

        self.assertEqual([x for x in Stuff._columns.keys()], ['id', 'words', 'content', 'numbers'])

    def test_exception_raised_when_creating_class_without_pk(self):
        with self.assertRaises(ModelDefinitionException):
            class TestModel(Model):

                count = columns.Integer()
                text = columns.Text(required=False)

    def test_value_managers_are_keeping_model_instances_isolated(self):
        """
        Tests that instance value managers are isolated from other instances
        """
        class Stuff(Model):

            id = columns.UUID(primary_key=True, default=lambda:uuid4())
            num = columns.Integer()

        inst1 = Stuff(num=5)
        inst2 = Stuff(num=7)

        self.assertNotEqual(inst1.num, inst2.num)
        self.assertEqual(inst1.num, 5)
        self.assertEqual(inst2.num, 7)

    def test_superclass_fields_are_inherited(self):
        """
        Tests that fields defined on the super class are inherited properly
        """
        class TestModel(Model):

            id = columns.UUID(primary_key=True, default=lambda:uuid4())
            text = columns.Text()

        class InheritedModel(TestModel):
            numbers = columns.Integer()

        assert 'text' in InheritedModel._columns
        assert 'numbers' in InheritedModel._columns

    def test_column_family_name_generation(self):
        """ Tests that auto column family name generation works as expected """
        class TestModel(Model):

            id = columns.UUID(primary_key=True, default=lambda:uuid4())
            text = columns.Text()

        assert TestModel.column_family_name(include_keyspace=False) == 'test_model'

    def test_partition_keys(self):
        """
        Test compound partition key definition
        """
        class ModelWithPartitionKeys(Model):

            id = columns.UUID(primary_key=True, default=lambda:uuid4())
            c1 = columns.Text(primary_key=True)
            p1 = columns.Text(partition_key=True)
            p2 = columns.Text(partition_key=True)

        cols = ModelWithPartitionKeys._columns

        self.assertTrue(cols['c1'].primary_key)
        self.assertFalse(cols['c1'].partition_key)

        self.assertTrue(cols['p1'].primary_key)
        self.assertTrue(cols['p1'].partition_key)
        self.assertTrue(cols['p2'].primary_key)
        self.assertTrue(cols['p2'].partition_key)

        obj = ModelWithPartitionKeys(p1='a', p2='b')
        self.assertEqual(obj.pk, ('a', 'b'))

    def test_del_attribute_is_assigned_properly(self):
        """ Tests that columns that can be deleted have the del attribute """
        class DelModel(Model):

            id = columns.UUID(primary_key=True, default=lambda:uuid4())
            key = columns.Integer(primary_key=True)
            data = columns.Integer(required=False)

        model = DelModel(key=4, data=5)
        del model.data
        with self.assertRaises(AttributeError):
            del model.key

    def test_does_not_exist_exceptions_are_not_shared_between_model(self):
        """ Tests that DoesNotExist exceptions are not the same exception between models """

        class Model1(Model):

            id = columns.UUID(primary_key=True, default=lambda:uuid4())

        class Model2(Model):

            id = columns.UUID(primary_key=True, default=lambda:uuid4())

        try:
            raise Model1.DoesNotExist
        except Model2.DoesNotExist:
            assert False, "Model1 exception should not be caught by Model2"
        except Model1.DoesNotExist:
            # expected
            pass

    def test_does_not_exist_inherits_from_superclass(self):
        """ Tests that a DoesNotExist exception can be caught by it's parent class DoesNotExist """
        class Model1(Model):

            id = columns.UUID(primary_key=True, default=lambda:uuid4())

        class Model2(Model1):
            pass

        try:
            raise Model2.DoesNotExist
        except Model1.DoesNotExist:
            # expected
            pass
        except Exception:
            assert False, "Model2 exception should not be caught by Model1"

    def test_abstract_model_keyspace_warning_is_skipped(self):
        with warnings.catch_warnings(record=True) as warn:
            class NoKeyspace(Model):
                __abstract__ = True
                key = columns.UUID(primary_key=True)

        self.assertEqual(len(warn), 0)


class TestManualTableNaming(BaseCassEngTestCase):

    class RenamedTest(Model):
        __keyspace__ = 'whatever'
        __table_name__ = 'manual_name'

        id = columns.UUID(primary_key=True)
        data = columns.Text()

    def test_proper_table_naming(self):
        assert self.RenamedTest.column_family_name(include_keyspace=False) == 'manual_name'
        assert self.RenamedTest.column_family_name(include_keyspace=True) == 'whatever.manual_name'


class TestManualTableNamingCaseSensitive(BaseCassEngTestCase):

    class RenamedCaseInsensitiveTest(Model):
        __keyspace__ = 'whatever'
        __table_name__ = 'Manual_Name'

        id = columns.UUID(primary_key=True)

    class RenamedCaseSensitiveTest(Model):
        __keyspace__ = 'whatever'
        __table_name__ = 'Manual_Name'
        __table_name_case_sensitive__ = True

        id = columns.UUID(primary_key=True)

    def test_proper_table_naming_case_insensitive(self):
        """
        Test to ensure case senstivity is not honored by default honored

        @since 3.1
        @jira_ticket PYTHON-337
        @expected_result table_names arel lowercase

        @test_category object_mapper
        """
        self.assertEqual(self.RenamedCaseInsensitiveTest.column_family_name(include_keyspace=False), 'manual_name')
        self.assertEqual(self.RenamedCaseInsensitiveTest.column_family_name(include_keyspace=True), 'whatever.manual_name')

    def test_proper_table_naming_case_sensitive(self):
        """
        Test to ensure case is honored when the flag is correctly set.

        @since 3.1
        @jira_ticket PYTHON-337
        @expected_result table_name case is honored.

        @test_category object_mapper
        """

        self.assertEqual(self.RenamedCaseSensitiveTest.column_family_name(include_keyspace=False), '"Manual_Name"')
        self.assertEqual(self.RenamedCaseSensitiveTest.column_family_name(include_keyspace=True), 'whatever."Manual_Name"')


class AbstractModel(Model):
    __abstract__ = True


class ConcreteModel(AbstractModel):
    pkey = columns.Integer(primary_key=True)
    data = columns.Integer()


class AbstractModelWithCol(Model):

    __abstract__ = True
    pkey = columns.Integer(primary_key=True)


class ConcreteModelWithCol(AbstractModelWithCol):
    data = columns.Integer()


class AbstractModelWithFullCols(Model):
    __abstract__ = True

    pkey = columns.Integer(primary_key=True)
    data = columns.Integer()


class TestAbstractModelClasses(BaseCassEngTestCase):

    def test_id_field_is_not_created(self):
        """ Tests that an id field is not automatically generated on abstract classes """
        assert not hasattr(AbstractModel, 'id')
        assert not hasattr(AbstractModelWithCol, 'id')

    def test_id_field_is_not_created_on_subclass(self):
        assert not hasattr(ConcreteModel, 'id')

    def test_abstract_attribute_is_not_inherited(self):
        """ Tests that __abstract__ attribute is not inherited """
        assert not ConcreteModel.__abstract__
        assert not ConcreteModelWithCol.__abstract__

    def test_attempting_to_save_abstract_model_fails(self):
        """ Attempting to save a model from an abstract model should fail """
        with self.assertRaises(CQLEngineException):
            AbstractModelWithFullCols.create(pkey=1, data=2)

    def test_attempting_to_create_abstract_table_fails(self):
        """ Attempting to create a table from an abstract model should fail """
        from cassandra.cqlengine.management import sync_table
        with self.assertRaises(CQLEngineException):
            sync_table(AbstractModelWithFullCols)

    def test_attempting_query_on_abstract_model_fails(self):
        """ Tests attempting to execute query with an abstract model fails """
        with self.assertRaises(CQLEngineException):
            iter(AbstractModelWithFullCols.objects(pkey=5)).next()

    def test_abstract_columns_are_inherited(self):
        """ Tests that columns defined in the abstract class are inherited into the concrete class """
        assert hasattr(ConcreteModelWithCol, 'pkey')
        assert isinstance(ConcreteModelWithCol.pkey, ColumnQueryEvaluator)
        assert isinstance(ConcreteModelWithCol._columns['pkey'], columns.Column)

    def test_concrete_class_table_creation_cycle(self):
        """ Tests that models with inherited abstract classes can be created, and have io performed """
        from cassandra.cqlengine.management import sync_table, drop_table
        sync_table(ConcreteModelWithCol)

        w1 = ConcreteModelWithCol.create(pkey=5, data=6)
        w2 = ConcreteModelWithCol.create(pkey=6, data=7)

        r1 = ConcreteModelWithCol.get(pkey=5)
        r2 = ConcreteModelWithCol.get(pkey=6)

        assert w1.pkey == r1.pkey
        assert w1.data == r1.data
        assert w2.pkey == r2.pkey
        assert w2.data == r2.data

        drop_table(ConcreteModelWithCol)


class TestCustomQuerySet(BaseCassEngTestCase):
    """ Tests overriding the default queryset class """

    class TestException(Exception): pass

    def test_overriding_queryset(self):

        class QSet(ModelQuerySet):
            def create(iself, **kwargs):
                raise self.TestException

        class CQModel(Model):
            __queryset__ = QSet

            part = columns.UUID(primary_key=True)
            data = columns.Text()

        with self.assertRaises(self.TestException):
            CQModel.create(part=uuid4(), data='s')

    def test_overriding_dmlqueryset(self):

        class DMLQ(DMLQuery):
            def save(iself):
                raise self.TestException

        class CDQModel(Model):

            __dmlquery__ = DMLQ
            part = columns.UUID(primary_key=True)
            data = columns.Text()

        with self.assertRaises(self.TestException):
            CDQModel().save()


class TestCachedLengthIsNotCarriedToSubclasses(BaseCassEngTestCase):
    def test_subclassing(self):

        length = len(ConcreteModelWithCol())

        class AlreadyLoadedTest(ConcreteModelWithCol):
            new_field = columns.Integer()

        self.assertGreater(len(AlreadyLoadedTest()), length)
