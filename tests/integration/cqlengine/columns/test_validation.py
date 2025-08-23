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

import sys
from datetime import datetime, timedelta, date, tzinfo, time, timezone
from decimal import Decimal as D
from uuid import uuid4, uuid1
from packaging.version import Version

from cassandra import InvalidRequest
from cassandra.cqlengine.columns import (TimeUUID, Ascii, Text, Integer, BigInt,
                                         VarInt, DateTime, Date, UUID, Boolean,
                                         Decimal, Inet, Time, UserDefinedType,
                                         Map, List, Set, Tuple, Double, Duration)
from cassandra.cqlengine.connection import execute
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.models import Model, ValidationError
from cassandra.cqlengine.usertype import UserType
from cassandra import util

from tests.integration import PROTOCOL_VERSION, CASSANDRA_VERSION, greaterthanorequalcass30, greaterthanorequalcass3_11
from tests.integration.cqlengine.base import BaseCassEngTestCase


class TestDatetime(BaseCassEngTestCase):
    class DatetimeTest(Model):

        test_id = Integer(primary_key=True)
        created_at = DateTime()

    @classmethod
    def setUpClass(cls):
        sync_table(cls.DatetimeTest)

    @classmethod
    def tearDownClass(cls):
        drop_table(cls.DatetimeTest)

    def test_datetime_io(self):
        now = datetime.now()
        self.DatetimeTest.objects.create(test_id=0, created_at=now)
        dt2 = self.DatetimeTest.objects(test_id=0).first()
        self.assertEqual(dt2.created_at.timetuple()[:6], now.timetuple()[:6])

    def test_datetime_tzinfo_io(self):
        class TZ(tzinfo):
            def utcoffset(self, date_time):
                return timedelta(hours=-1)
            def dst(self, date_time):
                return None

        now = datetime(1982, 1, 1, tzinfo=TZ())
        dt = self.DatetimeTest.objects.create(test_id=1, created_at=now)
        dt2 = self.DatetimeTest.objects(test_id=1).first()
        self.assertEqual(dt2.created_at.timetuple()[:6], (now + timedelta(hours=1)).timetuple()[:6])

    @greaterthanorequalcass30
    def test_datetime_date_support(self):
        today = date.today()
        self.DatetimeTest.objects.create(test_id=2, created_at=today)
        dt2 = self.DatetimeTest.objects(test_id=2).first()
        self.assertEqual(dt2.created_at.isoformat(), datetime(today.year, today.month, today.day).isoformat())

        result = self.DatetimeTest.objects.all().allow_filtering().filter(test_id=2).first()
        self.assertEqual(result.created_at, datetime.combine(today, datetime.min.time()))

        result = self.DatetimeTest.objects.all().allow_filtering().filter(test_id=2, created_at=today).first()
        self.assertEqual(result.created_at, datetime.combine(today, datetime.min.time()))

    def test_datetime_none(self):
        dt = self.DatetimeTest.objects.create(test_id=3, created_at=None)
        dt2 = self.DatetimeTest.objects(test_id=3).first()
        self.assertIsNone(dt2.created_at)

        dts = self.DatetimeTest.objects.filter(test_id=3).values_list('created_at')
        self.assertIsNone(dts[0][0])

    def test_datetime_invalid(self):
        dt_value= 'INVALID'
        with self.assertRaises(TypeError):
            self.DatetimeTest.objects.create(test_id=4, created_at=dt_value)

    def test_datetime_timestamp(self):
        dt_value = 1454520554
        self.DatetimeTest.objects.create(test_id=5, created_at=dt_value)
        dt2 = self.DatetimeTest.objects(test_id=5).first()
        self.assertEqual(dt2.created_at, datetime.fromtimestamp(dt_value, tz=timezone.utc).replace(tzinfo=None))

    def test_datetime_large(self):
        dt_value = datetime(2038, 12, 31, 10, 10, 10, 123000)
        self.DatetimeTest.objects.create(test_id=6, created_at=dt_value)
        dt2 = self.DatetimeTest.objects(test_id=6).first()
        self.assertEqual(dt2.created_at, dt_value)

    def test_datetime_truncate_microseconds(self):
        """
        Test to ensure that truncate microseconds works as expected.
        This will be default behavior in the future and we will need to modify the tests to comply
        with new behavior

        @since 3.2
        @jira_ticket PYTHON-273
        @expected_result microseconds should be to the nearest thousand when truncate is set.

        @test_category object_mapper
        """
        DateTime.truncate_microseconds = True
        try:
            dt_value = datetime(2024, 12, 31, 10, 10, 10, 923567)
            dt_truncated = datetime(2024, 12, 31, 10, 10, 10, 923000)
            self.DatetimeTest.objects.create(test_id=6, created_at=dt_value)
            dt2 = self.DatetimeTest.objects(test_id=6).first()
            self.assertEqual(dt2.created_at,dt_truncated)
        finally:
            # We need to always return behavior to default
            DateTime.truncate_microseconds = False


class TestBoolDefault(BaseCassEngTestCase):
    class BoolDefaultValueTest(Model):

        test_id = Integer(primary_key=True)
        stuff = Boolean(default=True)

    @classmethod
    def setUpClass(cls):
        sync_table(cls.BoolDefaultValueTest)

    def test_default_is_set(self):
        tmp = self.BoolDefaultValueTest.create(test_id=1)
        self.assertEqual(True, tmp.stuff)
        tmp2 = self.BoolDefaultValueTest.get(test_id=1)
        self.assertEqual(True, tmp2.stuff)


class TestBoolValidation(BaseCassEngTestCase):
    class BoolValidationTest(Model):

        test_id = Integer(primary_key=True)
        bool_column = Boolean()

    @classmethod
    def setUpClass(cls):
        sync_table(cls.BoolValidationTest)

    def test_validation_preserves_none(self):
        test_obj = self.BoolValidationTest(test_id=1)

        test_obj.validate()
        self.assertIsNone(test_obj.bool_column)


class TestVarInt(BaseCassEngTestCase):
    class VarIntTest(Model):

        test_id = Integer(primary_key=True)
        bignum = VarInt(primary_key=True)

    @classmethod
    def setUpClass(cls):
        sync_table(cls.VarIntTest)

    @classmethod
    def tearDownClass(cls):
        sync_table(cls.VarIntTest)

    def test_varint_io(self):
        # TODO: this is a weird test.  i changed the number from sys.maxint (which doesn't exist in python 3)
        # to the giant number below and it broken between runs.
        long_int = 92834902384092834092384028340283048239048203480234823048230482304820348239
        int1 = self.VarIntTest.objects.create(test_id=0, bignum=long_int)
        int2 = self.VarIntTest.objects(test_id=0).first()
        self.assertEqual(int1.bignum, int2.bignum)

        with self.assertRaises(ValidationError):
            self.VarIntTest.objects.create(test_id=0, bignum="not_a_number")


class DataType():
    @classmethod
    def setUpClass(cls):
        if PROTOCOL_VERSION < 4 or CASSANDRA_VERSION < Version("3.0"):
            return

        class DataTypeTest(Model):
            test_id = Integer(primary_key=True)
            class_param = cls.db_klass()

        cls.model_class = DataTypeTest
        sync_table(cls.model_class)

    @classmethod
    def tearDownClass(cls):
        if PROTOCOL_VERSION < 4 or CASSANDRA_VERSION < Version("3.0"):
            return
        drop_table(cls.model_class)

    def setUp(self):
        if PROTOCOL_VERSION < 4 or CASSANDRA_VERSION < Version("3.0"):
            raise unittest.SkipTest("Protocol v4 datatypes "
                                    "require native protocol 4+ and C* version >=3.0, "
                                    "currently using protocol {0} and C* version {1}".
                                    format(PROTOCOL_VERSION, CASSANDRA_VERSION))

    def _check_value_is_correct_in_db(self, value):
        """
         Check that different ways of reading the value
         from the model class give the same expected result
        """
        if value is None:
            result = self.model_class.objects.all().allow_filtering().filter(test_id=0).first()
            self.assertIsNone(result.class_param)

            result = self.model_class.objects(test_id=0).first()
            self.assertIsNone(result.class_param)

        else:
            if not isinstance(value, self.python_klass):
                value_to_compare = self.python_klass(value)
            else:
                value_to_compare = value

            result = self.model_class.objects(test_id=0).first()
            self.assertIsInstance(result.class_param, self.python_klass)
            self.assertEqual(result.class_param, value_to_compare)

            result = self.model_class.objects.all().allow_filtering().filter(test_id=0).first()
            self.assertIsInstance(result.class_param, self.python_klass)
            self.assertEqual(result.class_param, value_to_compare)

            result = self.model_class.objects.all().allow_filtering().filter(test_id=0, class_param=value).first()
            self.assertIsInstance(result.class_param, self.python_klass)
            self.assertEqual(result.class_param, value_to_compare)

        return result

    def test_param_io(self):
        first_value = self.first_value
        second_value = self.second_value
        third_value = self.third_value

        # Check value is correctly written/read from the DB
        self.model_class.objects.create(test_id=0, class_param=first_value)
        result = self._check_value_is_correct_in_db(first_value)
        result.delete()

        # Check the previous value has been correctly deleted and write a new value
        self.model_class.objects.create(test_id=0, class_param=second_value)
        result = self._check_value_is_correct_in_db(second_value)

        # Check the value can be correctly updated from the Model class
        result.update(class_param=third_value).save()
        result = self._check_value_is_correct_in_db(third_value)

        # Check None is correctly written to the DB
        result.update(class_param=None).save()
        self._check_value_is_correct_in_db(None)

    def test_param_none(self):
        """
         Test that None value is correctly written to the db
         and then is correctly read
        """
        self.model_class.objects.create(test_id=1, class_param=None)
        dt2 = self.model_class.objects(test_id=1).first()
        self.assertIsNone(dt2.class_param)

        dts = self.model_class.objects(test_id=1).values_list('class_param')
        self.assertIsNone(dts[0][0])


class TestDate(DataType, BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_klass, cls.python_klass = (
            Date,
            util.Date
        )

        cls.first_value, cls.second_value, cls.third_value = (
            datetime.utcnow(),
            util.Date(datetime(1, 1, 1)),
            datetime(1, 1, 2)
        )
        super(TestDate, cls).setUpClass()


class TestTime(DataType, BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_klass, cls.python_klass = (
            Time,
            util.Time
        )
        cls.first_value, cls.second_value, cls.third_value = (
            None,
            util.Time(time(2, 12, 7, 49)),
            time(2, 12, 7, 50)
        )
        super(TestTime, cls).setUpClass()


class TestDateTime(DataType, BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_klass, cls.python_klass = (
            DateTime,
            datetime
        )
        cls.first_value, cls.second_value, cls.third_value = (
            datetime(2017, 4, 13, 18, 34, 24, 317000),
            datetime(1, 1, 1),
            datetime(1, 1, 2)
        )
        super(TestDateTime, cls).setUpClass()


class TestBoolean(DataType, BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_klass, cls.python_klass = (
            Boolean,
            bool
        )
        cls.first_value, cls.second_value, cls.third_value = (
            None,
            False,
            True
        )
        super(TestBoolean, cls).setUpClass()

@greaterthanorequalcass3_11
class TestDuration(DataType, BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        # setUpClass is executed despite the whole class being skipped
        if CASSANDRA_VERSION >= Version("3.10"):
            cls.db_klass, cls.python_klass = (
                Duration,
                util.Duration
            )
            cls.first_value, cls.second_value, cls.third_value = (
                util.Duration(0, 0, 0),
                util.Duration(1, 2, 3),
                util.Duration(0, 0, 0)
            )
            super(TestDuration, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        if CASSANDRA_VERSION >= Version("3.10"):
            super(TestDuration, cls).tearDownClass()


class User(UserType):
    # We use Date and Time to ensure to_python
    # is called for these columns
    age = Integer()
    date_param = Date()
    map_param = Map(Integer, Time)
    list_param = List(Date)
    set_param = Set(Date)
    tuple_param = Tuple(Date, Decimal, Boolean, VarInt, Double, UUID)


class UserModel(Model):
    test_id = Integer(primary_key=True)
    class_param = UserDefinedType(User)


class TestUDT(DataType, BaseCassEngTestCase):
    @classmethod
    def setUpClass(cls):
        if PROTOCOL_VERSION < 4 or CASSANDRA_VERSION < Version("3.0"):
            return
        
        cls.db_klass, cls.python_klass = UserDefinedType, User
        cls.first_value = User(
            age=1,
            date_param=datetime.utcnow(),
            map_param={1: time(2, 12, 7, 50), 2: util.Time(time(2, 12, 7, 49))},
            list_param=[datetime(1, 1, 2), datetime(1, 1, 3)],
            set_param=set((datetime(1, 1, 3), util.Date(datetime(1, 1, 1)))),
            tuple_param=(datetime(1, 1, 3), 2, False, 1, 2.324, uuid4())
        )

        cls.second_value = User(
            age=1,
            date_param=datetime.utcnow(),
            map_param={1: time(2, 12, 7, 50), 2: util.Time(time(2, 12, 7, 49))},
            list_param=[datetime(1, 1, 2), datetime(1, 2, 3)],
            set_param=None,
            tuple_param=(datetime(1, 1, 2), 2, False, 1, 2.324, uuid4())
        )

        cls.third_value = User(
            age=2,
            date_param=None,
            map_param={1: time(2, 12, 7, 51), 2: util.Time(time(2, 12, 7, 49))},
            list_param=[datetime(1, 1, 2), datetime(1, 1, 4)],
            set_param=set((datetime(1, 1, 3), util.Date(datetime(1, 1, 2)))),
            tuple_param=(None, 3, False, None, 2.3214, uuid4())
        )

        cls.model_class = UserModel
        sync_table(cls.model_class)


class TestDecimal(BaseCassEngTestCase):
    class DecimalTest(Model):

        test_id = Integer(primary_key=True)
        dec_val = Decimal()

    @classmethod
    def setUpClass(cls):
        sync_table(cls.DecimalTest)

    @classmethod
    def tearDownClass(cls):
        drop_table(cls.DecimalTest)

    def test_decimal_io(self):
        dt = self.DecimalTest.objects.create(test_id=0, dec_val=D('0.00'))
        dt2 = self.DecimalTest.objects(test_id=0).first()
        assert dt2.dec_val == dt.dec_val

        dt = self.DecimalTest.objects.create(test_id=0, dec_val=5)
        dt2 = self.DecimalTest.objects(test_id=0).first()
        assert dt2.dec_val == D('5')


class TestUUID(BaseCassEngTestCase):
    class UUIDTest(Model):

        test_id = Integer(primary_key=True)
        a_uuid = UUID(default=uuid4())

    @classmethod
    def setUpClass(cls):
        sync_table(cls.UUIDTest)

    @classmethod
    def tearDownClass(cls):
        drop_table(cls.UUIDTest)

    def test_uuid_str_with_dashes(self):
        a_uuid = uuid4()
        t0 = self.UUIDTest.create(test_id=0, a_uuid=str(a_uuid))
        t1 = self.UUIDTest.get(test_id=0)
        assert a_uuid == t1.a_uuid

    def test_uuid_str_no_dashes(self):
        a_uuid = uuid4()
        t0 = self.UUIDTest.create(test_id=1, a_uuid=a_uuid.hex)
        t1 = self.UUIDTest.get(test_id=1)
        assert a_uuid == t1.a_uuid

    def test_uuid_with_upcase(self):
        a_uuid = uuid4()
        val = str(a_uuid).upper()
        t0 = self.UUIDTest.create(test_id=0, a_uuid=val)
        t1 = self.UUIDTest.get(test_id=0)
        assert a_uuid == t1.a_uuid


class TestTimeUUID(BaseCassEngTestCase):
    class TimeUUIDTest(Model):

        test_id = Integer(primary_key=True)
        timeuuid = TimeUUID(default=uuid1())

    @classmethod
    def setUpClass(cls):
        sync_table(cls.TimeUUIDTest)

    @classmethod
    def tearDownClass(cls):
        drop_table(cls.TimeUUIDTest)

    def test_timeuuid_io(self):
        """
        ensures that
        :return:
        """
        t0 = self.TimeUUIDTest.create(test_id=0)
        t1 = self.TimeUUIDTest.get(test_id=0)

        assert t1.timeuuid.time == t1.timeuuid.time


class TestInteger(BaseCassEngTestCase):
    class IntegerTest(Model):

        test_id = UUID(primary_key=True, default=lambda:uuid4())
        value = Integer(default=0, required=True)

    def test_default_zero_fields_validate(self):
        """ Tests that integer columns with a default value of 0 validate """
        it = self.IntegerTest()
        it.validate()


class TestBigInt(BaseCassEngTestCase):
    class BigIntTest(Model):

        test_id = UUID(primary_key=True, default=lambda:uuid4())
        value   = BigInt(default=0, required=True)

    def test_default_zero_fields_validate(self):
        """ Tests that bigint columns with a default value of 0 validate """
        it = self.BigIntTest()
        it.validate()


class TestAscii(BaseCassEngTestCase):
    def test_min_length(self):
        """ Test arbitrary minimal lengths requirements. """

        Ascii(min_length=0).validate('')
        Ascii(min_length=0, required=True).validate('')

        Ascii(min_length=0).validate(None)
        Ascii(min_length=0).validate('kevin')

        Ascii(min_length=1).validate('k')

        Ascii(min_length=5).validate('kevin')
        Ascii(min_length=5).validate('kevintastic')

        with self.assertRaises(ValidationError):
            Ascii(min_length=1).validate('')

        with self.assertRaises(ValidationError):
            Ascii(min_length=1).validate(None)

        with self.assertRaises(ValidationError):
            Ascii(min_length=6).validate('')

        with self.assertRaises(ValidationError):
            Ascii(min_length=6).validate(None)

        with self.assertRaises(ValidationError):
            Ascii(min_length=6).validate('kevin')

        with self.assertRaises(ValueError):
            Ascii(min_length=-1)

    def test_max_length(self):
        """ Test arbitrary maximal lengths requirements. """
        Ascii(max_length=0).validate('')
        Ascii(max_length=0).validate(None)

        Ascii(max_length=1).validate('')
        Ascii(max_length=1).validate(None)
        Ascii(max_length=1).validate('b')

        Ascii(max_length=5).validate('')
        Ascii(max_length=5).validate(None)
        Ascii(max_length=5).validate('b')
        Ascii(max_length=5).validate('blake')

        with self.assertRaises(ValidationError):
            Ascii(max_length=0).validate('b')

        with self.assertRaises(ValidationError):
            Ascii(max_length=5).validate('blaketastic')

        with self.assertRaises(ValueError):
            Ascii(max_length=-1)

    def test_length_range(self):
        Ascii(min_length=0, max_length=0)
        Ascii(min_length=0, max_length=1)
        Ascii(min_length=10, max_length=10)
        Ascii(min_length=10, max_length=11)

        with self.assertRaises(ValueError):
            Ascii(min_length=10, max_length=9)

        with self.assertRaises(ValueError):
            Ascii(min_length=1, max_length=0)

    def test_type_checking(self):
        Ascii().validate('string')
        Ascii().validate(u'unicode')
        Ascii().validate(bytearray('bytearray', encoding='ascii'))

        with self.assertRaises(ValidationError):
            Ascii().validate(5)

        with self.assertRaises(ValidationError):
            Ascii().validate(True)

        Ascii().validate("!#$%&\'()*+,-./")

        with self.assertRaises(ValidationError):
            Ascii().validate('Beyonc' + chr(233))

        if sys.version_info < (3, 1):
            with self.assertRaises(ValidationError):
                Ascii().validate(u'Beyonc' + unichr(233))

    def test_unaltering_validation(self):
        """ Test the validation step doesn't re-interpret values. """
        self.assertEqual(Ascii().validate(''), '')
        self.assertEqual(Ascii().validate(None), None)
        self.assertEqual(Ascii().validate('yo'), 'yo')

    def test_non_required_validation(self):
        """ Tests that validation is ok on none and blank values if required is False. """
        Ascii().validate('')
        Ascii().validate(None)

    def test_required_validation(self):
        """ Tests that validation raise on none and blank values if value required. """
        Ascii(required=True).validate('k')

        with self.assertRaises(ValidationError):
            Ascii(required=True).validate('')

        with self.assertRaises(ValidationError):
            Ascii(required=True).validate(None)

        # With min_length set.
        Ascii(required=True, min_length=0).validate('k')
        Ascii(required=True, min_length=1).validate('k')

        with self.assertRaises(ValidationError):
            Ascii(required=True, min_length=2).validate('k')

        # With max_length set.
        Ascii(required=True, max_length=1).validate('k')

        with self.assertRaises(ValidationError):
            Ascii(required=True, max_length=2).validate('kevin')

        with self.assertRaises(ValueError):
            Ascii(required=True, max_length=0)


class TestText(BaseCassEngTestCase):

    def test_min_length(self):
        """ Test arbitrary minimal lengths requirements. """

        Text(min_length=0).validate('')
        Text(min_length=0, required=True).validate('')

        Text(min_length=0).validate(None)
        Text(min_length=0).validate('blake')

        Text(min_length=1).validate('b')

        Text(min_length=5).validate('blake')
        Text(min_length=5).validate('blaketastic')

        with self.assertRaises(ValidationError):
            Text(min_length=1).validate('')

        with self.assertRaises(ValidationError):
            Text(min_length=1).validate(None)

        with self.assertRaises(ValidationError):
            Text(min_length=6).validate('')

        with self.assertRaises(ValidationError):
            Text(min_length=6).validate(None)

        with self.assertRaises(ValidationError):
            Text(min_length=6).validate('blake')

        with self.assertRaises(ValueError):
            Text(min_length=-1)

    def test_max_length(self):
        """ Test arbitrary maximal lengths requirements. """
        Text(max_length=0).validate('')
        Text(max_length=0).validate(None)

        Text(max_length=1).validate('')
        Text(max_length=1).validate(None)
        Text(max_length=1).validate('b')

        Text(max_length=5).validate('')
        Text(max_length=5).validate(None)
        Text(max_length=5).validate('b')
        Text(max_length=5).validate('blake')

        with self.assertRaises(ValidationError):
            Text(max_length=0).validate('b')

        with self.assertRaises(ValidationError):
            Text(max_length=5).validate('blaketastic')

        with self.assertRaises(ValueError):
            Text(max_length=-1)

    def test_length_range(self):
        Text(min_length=0, max_length=0)
        Text(min_length=0, max_length=1)
        Text(min_length=10, max_length=10)
        Text(min_length=10, max_length=11)

        with self.assertRaises(ValueError):
            Text(min_length=10, max_length=9)

        with self.assertRaises(ValueError):
            Text(min_length=1, max_length=0)

    def test_type_checking(self):
        Text().validate('string')
        Text().validate(u'unicode')
        Text().validate(bytearray('bytearray', encoding='ascii'))

        with self.assertRaises(ValidationError):
            Text().validate(5)

        with self.assertRaises(ValidationError):
            Text().validate(True)

        Text().validate("!#$%&\'()*+,-./")
        Text().validate('Beyonc' + chr(233))
        if sys.version_info < (3, 1):
            Text().validate(u'Beyonc' + unichr(233))

    def test_unaltering_validation(self):
        """ Test the validation step doesn't re-interpret values. """
        self.assertEqual(Text().validate(''), '')
        self.assertEqual(Text().validate(None), None)
        self.assertEqual(Text().validate('yo'), 'yo')

    def test_non_required_validation(self):
        """ Tests that validation is ok on none and blank values if required is False """
        Text().validate('')
        Text().validate(None)

    def test_required_validation(self):
        """ Tests that validation raise on none and blank values if value required. """
        Text(required=True).validate('b')

        with self.assertRaises(ValidationError):
            Text(required=True).validate('')

        with self.assertRaises(ValidationError):
            Text(required=True).validate(None)

        # With min_length set.
        Text(required=True, min_length=0).validate('b')
        Text(required=True, min_length=1).validate('b')

        with self.assertRaises(ValidationError):
            Text(required=True, min_length=2).validate('b')

        # With max_length set.
        Text(required=True, max_length=1).validate('b')

        with self.assertRaises(ValidationError):
            Text(required=True, max_length=2).validate('blake')

        with self.assertRaises(ValueError):
            Text(required=True, max_length=0)


class TestExtraFieldsRaiseException(BaseCassEngTestCase):
    class TestModel(Model):

        id = UUID(primary_key=True, default=uuid4)

    def test_extra_field(self):
        with self.assertRaises(ValidationError):
            self.TestModel.create(bacon=5000)


class TestPythonDoesntDieWhenExtraFieldIsInCassandra(BaseCassEngTestCase):
    class TestModel(Model):

        __table_name__ = 'alter_doesnt_break_running_app'
        id = UUID(primary_key=True, default=uuid4)

    def test_extra_field(self):
        drop_table(self.TestModel)
        sync_table(self.TestModel)
        self.TestModel.create()
        execute("ALTER TABLE {0} add blah int".format(self.TestModel.column_family_name(include_keyspace=True)))
        self.TestModel.objects.all()


class TestTimeUUIDFromDatetime(BaseCassEngTestCase):
    def test_conversion_specific_date(self):
        dt = datetime(1981, 7, 11, microsecond=555000)

        uuid = util.uuid_from_time(dt)

        from uuid import UUID
        assert isinstance(uuid, UUID)

        ts = (uuid.time - 0x01b21dd213814000) / 1e7 # back to a timestamp
        new_dt = datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)

        # checks that we created a UUID1 with the proper timestamp
        assert new_dt == dt


class TestInet(BaseCassEngTestCase):

    class InetTestModel(Model):
        id = UUID(primary_key=True, default=uuid4)
        address = Inet()

    def setUp(self):
        drop_table(self.InetTestModel)
        sync_table(self.InetTestModel)

    def test_inet_saves(self):
        tmp = self.InetTestModel.create(address="192.168.1.1")

        m = self.InetTestModel.get(id=tmp.id)

        assert m.address == "192.168.1.1"

    def test_non_address_fails(self):
        # TODO: presently this only tests that the server blows it up. Is there supposed to be local validation?
        with self.assertRaises(InvalidRequest):
            self.InetTestModel.create(address="what is going on here?")
