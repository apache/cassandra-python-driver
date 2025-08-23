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

import uuid
from unittest import mock

from cassandra.cqlengine import columns
from cassandra.cqlengine import models
from cassandra.cqlengine.connection import get_session
from tests.integration.cqlengine.base import BaseCassEngTestCase
from cassandra.cqlengine import management


class TestInheritanceClassConstruction(BaseCassEngTestCase):

    def test_multiple_discriminator_value_failure(self):
        """ Tests that defining a model with more than one discriminator column fails """
        with self.assertRaises(models.ModelDefinitionException):
            class M(models.Model):
                partition = columns.Integer(primary_key=True)
                type1 = columns.Integer(discriminator_column=True)
                type2 = columns.Integer(discriminator_column=True)

    def test_no_discriminator_column_failure(self):
        with self.assertRaises(models.ModelDefinitionException):
            class M(models.Model):
                __discriminator_value__ = 1

    def test_discriminator_value_inheritance(self):
        """ Tests that discriminator_column attribute is not inherited """
        class Base(models.Model):

            partition = columns.Integer(primary_key=True)
            type1 = columns.Integer(discriminator_column=True)

        class M1(Base):
            __discriminator_value__ = 1

        class M2(M1):
            pass

        assert M2.__discriminator_value__ is None

    def test_inheritance_metaclass(self):
        """ Tests that the model meta class configures inherited models properly """
        class Base(models.Model):

            partition = columns.Integer(primary_key=True)
            type1 = columns.Integer(discriminator_column=True)

        class M1(Base):
            __discriminator_value__ = 1

        assert Base._is_polymorphic
        assert M1._is_polymorphic

        assert Base._is_polymorphic_base
        assert not M1._is_polymorphic_base

        assert Base._discriminator_column is Base._columns['type1']
        assert M1._discriminator_column is M1._columns['type1']

        assert Base._discriminator_column_name == 'type1'
        assert M1._discriminator_column_name == 'type1'

    def test_table_names_are_inherited_from_base(self):
        class Base(models.Model):

            partition = columns.Integer(primary_key=True)
            type1 = columns.Integer(discriminator_column=True)

        class M1(Base):
            __discriminator_value__ = 1

        assert Base.column_family_name() == M1.column_family_name()

    def test_collection_columns_cant_be_discriminator_column(self):
        with self.assertRaises(models.ModelDefinitionException):
            class Base(models.Model):

                partition = columns.Integer(primary_key=True)
                type1 = columns.Set(columns.Integer, discriminator_column=True)


class InheritBase(models.Model):

    partition = columns.UUID(primary_key=True, default=uuid.uuid4)
    row_type = columns.Integer(discriminator_column=True)


class Inherit1(InheritBase):
    __discriminator_value__ = 1
    data1 = columns.Text()


class Inherit2(InheritBase):
    __discriminator_value__ = 2
    data2 = columns.Text()


class TestInheritanceModel(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestInheritanceModel, cls).setUpClass()
        management.sync_table(Inherit1)
        management.sync_table(Inherit2)

    @classmethod
    def tearDownClass(cls):
        super(TestInheritanceModel, cls).tearDownClass()
        management.drop_table(Inherit1)
        management.drop_table(Inherit2)

    def test_saving_base_model_fails(self):
        with self.assertRaises(models.PolymorphicModelException):
            InheritBase.create()

    def test_saving_subclass_saves_disc_value(self):
        p1 = Inherit1.create(data1='pickle')
        p2 = Inherit2.create(data2='bacon')

        assert p1.row_type == Inherit1.__discriminator_value__
        assert p2.row_type == Inherit2.__discriminator_value__

    def test_query_deserialization(self):
        p1 = Inherit1.create(data1='pickle')
        p2 = Inherit2.create(data2='bacon')

        p1r = InheritBase.get(partition=p1.partition)
        p2r = InheritBase.get(partition=p2.partition)

        assert isinstance(p1r, Inherit1)
        assert isinstance(p2r, Inherit2)

    def test_delete_on_subclass_does_not_include_disc_value(self):
        p1 = Inherit1.create()
        session = get_session()
        with mock.patch.object(session, 'execute') as m:
            Inherit1.objects(partition=p1.partition).delete()

        # make sure our discriminator value isn't in the CQL
        # not sure how we would even get here if it was in there
        # since the CQL would fail.

        self.assertNotIn("row_type", m.call_args[0][0].query_string)


class UnindexedInheritBase(models.Model):

    partition = columns.UUID(primary_key=True, default=uuid.uuid4)
    cluster = columns.UUID(primary_key=True, default=uuid.uuid4)
    row_type = columns.Integer(discriminator_column=True)


class UnindexedInherit1(UnindexedInheritBase):
    __discriminator_value__ = 1
    data1 = columns.Text()


class UnindexedInherit2(UnindexedInheritBase):
    __discriminator_value__ = 2
    data2 = columns.Text()


class UnindexedInherit3(UnindexedInherit2):
    __discriminator_value__ = 3
    data3 = columns.Text()


class TestUnindexedInheritanceQuery(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestUnindexedInheritanceQuery, cls).setUpClass()
        management.sync_table(UnindexedInherit1)
        management.sync_table(UnindexedInherit2)
        management.sync_table(UnindexedInherit3)

        cls.p1 = UnindexedInherit1.create(data1='pickle')
        cls.p2 = UnindexedInherit2.create(partition=cls.p1.partition, data2='bacon')
        cls.p3 = UnindexedInherit3.create(partition=cls.p1.partition, data3='turkey')

    @classmethod
    def tearDownClass(cls):
        super(TestUnindexedInheritanceQuery, cls).tearDownClass()
        management.drop_table(UnindexedInherit1)
        management.drop_table(UnindexedInherit2)
        management.drop_table(UnindexedInherit3)

    def test_non_conflicting_type_results_work(self):
        p1, p2, p3 = self.p1, self.p2, self.p3
        assert len(list(UnindexedInherit1.objects(partition=p1.partition, cluster=p1.cluster))) == 1
        assert len(list(UnindexedInherit2.objects(partition=p1.partition, cluster=p2.cluster))) == 1
        assert len(list(UnindexedInherit3.objects(partition=p1.partition, cluster=p3.cluster))) == 1

    def test_subclassed_model_results_work_properly(self):
        p1, p2, p3 = self.p1, self.p2, self.p3
        assert len(list(UnindexedInherit2.objects(partition=p1.partition, cluster__in=[p2.cluster, p3.cluster]))) == 2

    def test_conflicting_type_results(self):
        with self.assertRaises(models.PolymorphicModelException):
            list(UnindexedInherit1.objects(partition=self.p1.partition))
        with self.assertRaises(models.PolymorphicModelException):
            list(UnindexedInherit2.objects(partition=self.p1.partition))


class IndexedInheritBase(models.Model):

    partition = columns.UUID(primary_key=True, default=uuid.uuid4)
    cluster = columns.UUID(primary_key=True, default=uuid.uuid4)
    row_type = columns.Integer(discriminator_column=True, index=True)


class IndexedInherit1(IndexedInheritBase):
    __discriminator_value__ = 1
    data1 = columns.Text()


class IndexedInherit2(IndexedInheritBase):
    __discriminator_value__ = 2
    data2 = columns.Text()


class TestIndexedInheritanceQuery(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestIndexedInheritanceQuery, cls).setUpClass()
        management.sync_table(IndexedInherit1)
        management.sync_table(IndexedInherit2)

        cls.p1 = IndexedInherit1.create(data1='pickle')
        cls.p2 = IndexedInherit2.create(partition=cls.p1.partition, data2='bacon')

    @classmethod
    def tearDownClass(cls):
        super(TestIndexedInheritanceQuery, cls).tearDownClass()
        management.drop_table(IndexedInherit1)
        management.drop_table(IndexedInherit2)

    def test_success_case(self):
        self.assertEqual(len(list(IndexedInherit1.objects(partition=self.p1.partition))), 1)
        self.assertEqual(len(list(IndexedInherit2.objects(partition=self.p1.partition))), 1)
