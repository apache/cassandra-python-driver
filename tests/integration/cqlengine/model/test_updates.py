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

from unittest.mock import patch
from uuid import uuid4

from cassandra.cqlengine import ValidationError

from tests.integration import greaterthancass21
from tests.integration.cqlengine.base import BaseCassEngTestCase
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.usertype import UserType

class TestUpdateModel(Model):

    partition   = columns.UUID(primary_key=True, default=uuid4)
    cluster     = columns.UUID(primary_key=True, default=uuid4)
    count       = columns.Integer(required=False)
    text        = columns.Text(required=False, index=True)


class ModelUpdateTests(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(ModelUpdateTests, cls).setUpClass()
        sync_table(TestUpdateModel)

    @classmethod
    def tearDownClass(cls):
        super(ModelUpdateTests, cls).tearDownClass()
        drop_table(TestUpdateModel)

    def test_update_model(self):
        """ tests calling udpate on models with no values passed in """
        m0 = TestUpdateModel.create(count=5, text='monkey')

        # independently save over a new count value, unknown to original instance
        m1 = TestUpdateModel.get(partition=m0.partition, cluster=m0.cluster)
        m1.count = 6
        m1.save()

        # update the text, and call update
        m0.text = 'monkey land'
        m0.update()

        # database should reflect both updates
        m2 = TestUpdateModel.get(partition=m0.partition, cluster=m0.cluster)
        self.assertEqual(m2.count, m1.count)
        self.assertEqual(m2.text, m0.text)

        #This shouldn't raise a Validation error as the PR is not changing
        m0.update(partition=m0.partition, cluster=m0.cluster)

        #Assert a ValidationError is risen if the PR changes
        with self.assertRaises(ValidationError):
            m0.update(partition=m0.partition, cluster=20)

        # Assert a ValidationError is risen if the columns doesn't exist
        with self.assertRaises(ValidationError):
            m0.update(invalid_column=20)

    def test_update_values(self):
        """ tests calling update on models with values passed in """
        m0 = TestUpdateModel.create(count=5, text='monkey')

        # independently save over a new count value, unknown to original instance
        m1 = TestUpdateModel.get(partition=m0.partition, cluster=m0.cluster)
        m1.count = 6
        m1.save()

        # update the text, and call update
        m0.update(text='monkey land')
        self.assertEqual(m0.text, 'monkey land')

        # database should reflect both updates
        m2 = TestUpdateModel.get(partition=m0.partition, cluster=m0.cluster)
        self.assertEqual(m2.count, m1.count)
        self.assertEqual(m2.text, m0.text)

    def test_noop_model_direct_update(self):
        """ Tests that calling update on a model with no changes will do nothing. """
        m0 = TestUpdateModel.create(count=5, text='monkey')

        with patch.object(self.session, 'execute') as execute:
            m0.update()
        assert execute.call_count == 0

        with patch.object(self.session, 'execute') as execute:
            m0.update(count=5)
        assert execute.call_count == 0

        with patch.object(self.session, 'execute') as execute:
            m0.update(partition=m0.partition)

        with patch.object(self.session, 'execute') as execute:
            m0.update(cluster=m0.cluster)

    def test_noop_model_assignation_update(self):
        """ Tests that assigning the same value on a model will do nothing. """
        # Create object and fetch it back to eliminate any hidden variable
        # cache effect.
        m0 = TestUpdateModel.create(count=5, text='monkey')
        m1 = TestUpdateModel.get(partition=m0.partition, cluster=m0.cluster)

        with patch.object(self.session, 'execute') as execute:
            m1.save()
        assert execute.call_count == 0

        with patch.object(self.session, 'execute') as execute:
            m1.count = 5
            m1.save()
        assert execute.call_count == 0

        with patch.object(self.session, 'execute') as execute:
            m1.partition = m0.partition
            m1.save()
        assert execute.call_count == 0

        with patch.object(self.session, 'execute') as execute:
            m1.cluster = m0.cluster
            m1.save()
        assert execute.call_count == 0

    def test_invalid_update_kwarg(self):
        """ tests that passing in a kwarg to the update method that isn't a column will fail """
        m0 = TestUpdateModel.create(count=5, text='monkey')
        with self.assertRaises(ValidationError):
            m0.update(numbers=20)

    def test_primary_key_update_failure(self):
        """ tests that attempting to update the value of a primary key will fail """
        m0 = TestUpdateModel.create(count=5, text='monkey')
        with self.assertRaises(ValidationError):
            m0.update(partition=uuid4())


class UDT(UserType):
    age = columns.Integer()
    mf = columns.Map(columns.Integer, columns.Integer)
    dummy_udt = columns.Integer(default=42)
    time_col = columns.Time()


class ModelWithDefault(Model):
    id = columns.Integer(primary_key=True)
    mf = columns.Map(columns.Integer, columns.Integer)
    dummy = columns.Integer(default=42)
    udt = columns.UserDefinedType(UDT)
    udt_default = columns.UserDefinedType(UDT, default=UDT(age=1, mf={2:2}))


class UDTWithDefault(UserType):
    age = columns.Integer()
    mf = columns.Map(columns.Integer, columns.Integer, default={2:2})
    dummy_udt = columns.Integer(default=42)


class ModelWithDefaultCollection(Model):
    id = columns.Integer(primary_key=True)
    mf = columns.Map(columns.Integer, columns.Integer, default={2:2})
    dummy = columns.Integer(default=42)
    udt = columns.UserDefinedType(UDT)
    udt_default = columns.UserDefinedType(UDT, default=UDT(age=1, mf={2: 2}))

@greaterthancass21
class ModelWithDefaultTests(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        cls.udt_default = UDT(age=1, mf={2:2}, dummy_udt=42)

    def setUp(self):
        sync_table(ModelWithDefault)
        sync_table(ModelWithDefaultCollection)

    def tearDown(self):
        drop_table(ModelWithDefault)
        drop_table(ModelWithDefaultCollection)

    def test_value_override_with_default(self):
        """
        Updating a row with a new Model instance shouldn't set columns to defaults

        @since 3.9
        @jira_ticket PYTHON-657
        @expected_result column value should not change

        @test_category object_mapper
        """
        first_udt = UDT(age=1, mf={2:2}, dummy_udt=0)
        initial = ModelWithDefault(id=1, mf={0: 0}, dummy=0, udt=first_udt, udt_default=first_udt)
        initial.save()

        self.assertEqual(ModelWithDefault.get()._as_dict(),
                         {'id': 1, 'dummy': 0, 'mf': {0: 0}, "udt": first_udt, "udt_default": first_udt})

        second_udt = UDT(age=1, mf={3: 3}, dummy_udt=12)
        second = ModelWithDefault(id=1)
        second.update(mf={0: 1}, udt=second_udt)

        self.assertEqual(ModelWithDefault.get()._as_dict(),
                         {'id': 1, 'dummy': 0, 'mf': {0: 1}, "udt": second_udt, "udt_default": first_udt})

    def test_value_is_written_if_is_default(self):
        """
        Check if the we try to update with the default value, the update
        happens correctly
        @since 3.9
        @jira_ticket PYTHON-657
        @expected_result column value should be updated
        :return:
        """
        initial = ModelWithDefault(id=1)
        initial.mf = {0: 0}
        initial.dummy = 42
        initial.udt_default = self.udt_default
        initial.update()

        self.assertEqual(ModelWithDefault.get()._as_dict(),
                         {'id': 1, 'dummy': 42, 'mf': {0: 0}, "udt": None, "udt_default": self.udt_default})

    def test_null_update_is_respected(self):
        """
        Check if the we try to update with None under particular
        circumstances, it works correctly
        @since 3.9
        @jira_ticket PYTHON-657
        @expected_result column value should be updated to None

        @test_category object_mapper
        :return:
        """
        ModelWithDefault.create(id=1, mf={0: 0}).save()

        q = ModelWithDefault.objects.all().allow_filtering()
        obj = q.filter(id=1).get()

        updated_udt = UDT(age=1, mf={2:2}, dummy_udt=None)
        obj.update(dummy=None, udt_default=updated_udt)

        self.assertEqual(ModelWithDefault.get()._as_dict(),
                         {'id': 1, 'dummy': None, 'mf': {0: 0}, "udt": None, "udt_default": updated_udt})

    def test_only_set_values_is_updated(self):
        """
        Test the updates work as expected when an object is deleted
        @since 3.9
        @jira_ticket PYTHON-657
        @expected_result the non updated column is None and the
        updated column has the set value

        @test_category object_mapper
        """

        ModelWithDefault.create(id=1, mf={1: 1}, dummy=1).save()

        item = ModelWithDefault.filter(id=1).first()
        ModelWithDefault.objects(id=1).delete()
        item.mf = {1: 2}
        udt, udt_default = UDT(age=1, mf={2:3}), UDT(age=1, mf={2:3})
        item.udt, item.udt_default = udt, udt_default
        item.save()

        self.assertEqual(ModelWithDefault.get()._as_dict(),
                         {'id': 1, 'dummy': None, 'mf': {1: 2}, "udt": udt, "udt_default": udt_default})

    def test_collections(self):
        """
        Test the updates work as expected on Map objects
        @since 3.9
        @jira_ticket PYTHON-657
        @expected_result the row is updated when the Map object is
        reduced

        @test_category object_mapper
        """
        udt, udt_default = UDT(age=1, mf={1: 1, 2: 1}), UDT(age=1, mf={1: 1, 2: 1})

        ModelWithDefault.create(id=1, mf={1: 1, 2: 1}, dummy=1, udt=udt, udt_default=udt_default).save()
        item = ModelWithDefault.filter(id=1).first()

        udt, udt_default = UDT(age=1, mf={2: 1}), UDT(age=1, mf={2: 1})
        item.update(mf={2:1}, udt=udt, udt_default=udt_default)
        self.assertEqual(ModelWithDefault.get()._as_dict(),
                         {'id': 1, 'dummy': 1, 'mf': {2: 1}, "udt": udt, "udt_default": udt_default})

    def test_collection_with_default(self):
        """
        Test the updates work as expected when an object is deleted
        @since 3.9
        @jira_ticket PYTHON-657
        @expected_result the non updated column is None and the
        updated column has the set value

        @test_category object_mapper
        """
        sync_table(ModelWithDefaultCollection)

        udt, udt_default = UDT(age=1, mf={6: 6}), UDT(age=1, mf={6: 6})

        item = ModelWithDefaultCollection.create(id=1, mf={1: 1}, dummy=1, udt=udt, udt_default=udt_default).save()
        self.assertEqual(ModelWithDefaultCollection.objects.get(id=1)._as_dict(),
                         {'id': 1, 'dummy': 1, 'mf': {1: 1}, "udt": udt, "udt_default": udt_default})

        udt, udt_default = UDT(age=1, mf={5: 5}), UDT(age=1, mf={5: 5})
        item.update(mf={2: 2}, udt=udt, udt_default=udt_default)
        self.assertEqual(ModelWithDefaultCollection.objects.get(id=1)._as_dict(),
                         {'id': 1, 'dummy': 1, 'mf': {2: 2}, "udt": udt, "udt_default": udt_default})

        udt, udt_default = UDT(age=1, mf=None), UDT(age=1, mf=None)
        expected_udt, expected_udt_default = UDT(age=1, mf={}), UDT(age=1, mf={})
        item.update(mf=None, udt=udt, udt_default=udt_default)
        self.assertEqual(ModelWithDefaultCollection.objects.get(id=1)._as_dict(),
                         {'id': 1, 'dummy': 1, 'mf': {}, "udt": expected_udt, "udt_default": expected_udt_default})

        udt_default = UDT(age=1, mf={2:2}, dummy_udt=42)
        item = ModelWithDefaultCollection.create(id=2, dummy=2)
        self.assertEqual(ModelWithDefaultCollection.objects.get(id=2)._as_dict(),
                         {'id': 2, 'dummy': 2, 'mf': {2: 2}, "udt": None, "udt_default": udt_default})

        udt, udt_default = UDT(age=1, mf={1: 1, 6: 6}), UDT(age=1, mf={1: 1, 6: 6})
        item.update(mf={1: 1, 4: 4}, udt=udt, udt_default=udt_default)
        self.assertEqual(ModelWithDefaultCollection.objects.get(id=2)._as_dict(),
                         {'id': 2, 'dummy': 2, 'mf': {1: 1, 4: 4}, "udt": udt, "udt_default": udt_default})

        item.update(udt_default=None)
        self.assertEqual(ModelWithDefaultCollection.objects.get(id=2)._as_dict(),
                         {'id': 2, 'dummy': 2, 'mf': {1: 1, 4: 4}, "udt": udt, "udt_default": None})

        udt_default = UDT(age=1, mf={2:2})
        item.update(udt_default=udt_default)
        self.assertEqual(ModelWithDefaultCollection.objects.get(id=2)._as_dict(),
                         {'id': 2, 'dummy': 2, 'mf': {1: 1, 4: 4}, "udt": udt, "udt_default": udt_default})


    def test_udt_to_python(self):
        """
        Test the to_python and to_database are correctly called on UDTs
        @since 3.10
        @jira_ticket PYTHON-743
        @expected_result the int value is correctly converted to utils.Time
        and written to C*

        @test_category object_mapper
        """
        item = ModelWithDefault(id=1)
        item.save()

        # We update time_col this way because we want to hit
        # the to_python method from UserDefinedType, otherwise to_python
        # would be called in UDT.__init__
        user_to_update = UDT()
        user_to_update.time_col = 10

        item.update(udt=user_to_update)

        udt, udt_default = UDT(time_col=10), UDT(age=1, mf={2:2})
        self.assertEqual(ModelWithDefault.objects.get(id=1)._as_dict(),
                         {'id': 1, 'dummy': 42, 'mf': {}, "udt": udt, "udt_default": udt_default})
