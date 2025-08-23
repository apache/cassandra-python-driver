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

import datetime
import tempfile
import time
import uuid
from binascii import unhexlify

import cassandra
from cassandra import util
from cassandra.cqltypes import (
    CassandraType, DateRangeType, DateType, DecimalType,
    EmptyValue, LongType, SetType, UTF8Type,
    cql_typename, int8_pack, int64_pack, lookup_casstype,
    lookup_casstype_simple, parse_casstype_args,
    int32_pack, Int32Type, ListType, MapType, VectorType,
    FloatType
)
from cassandra.encoder import cql_quote
from cassandra.pool import Host
from cassandra.metadata import Token
from cassandra.policies import ConvictionPolicy, SimpleConvictionPolicy
from cassandra.protocol import (
    read_inet, read_longstring, read_string,
    read_stringmap, write_inet, write_longstring,
    write_string, write_stringmap
)
from cassandra.query import named_tuple_factory
from cassandra.util import (
    OPEN_BOUND, Date, DateRange, DateRangeBound,
    DateRangePrecision, Time, ms_timestamp_from_datetime,
    datetime_from_timestamp
)
from tests.unit.util import check_sequence_consistency


class TypeTests(unittest.TestCase):

    def test_lookup_casstype_simple(self):
        """
        Ensure lookup_casstype_simple returns the correct classes
        """

        self.assertEqual(lookup_casstype_simple('AsciiType'), cassandra.cqltypes.AsciiType)
        self.assertEqual(lookup_casstype_simple('LongType'), cassandra.cqltypes.LongType)
        self.assertEqual(lookup_casstype_simple('BytesType'), cassandra.cqltypes.BytesType)
        self.assertEqual(lookup_casstype_simple('BooleanType'), cassandra.cqltypes.BooleanType)
        self.assertEqual(lookup_casstype_simple('CounterColumnType'), cassandra.cqltypes.CounterColumnType)
        self.assertEqual(lookup_casstype_simple('DecimalType'), cassandra.cqltypes.DecimalType)
        self.assertEqual(lookup_casstype_simple('DoubleType'), cassandra.cqltypes.DoubleType)
        self.assertEqual(lookup_casstype_simple('FloatType'), cassandra.cqltypes.FloatType)
        self.assertEqual(lookup_casstype_simple('InetAddressType'), cassandra.cqltypes.InetAddressType)
        self.assertEqual(lookup_casstype_simple('Int32Type'), cassandra.cqltypes.Int32Type)
        self.assertEqual(lookup_casstype_simple('UTF8Type'), cassandra.cqltypes.UTF8Type)
        self.assertEqual(lookup_casstype_simple('DateType'), cassandra.cqltypes.DateType)
        self.assertEqual(lookup_casstype_simple('SimpleDateType'), cassandra.cqltypes.SimpleDateType)
        self.assertEqual(lookup_casstype_simple('ByteType'), cassandra.cqltypes.ByteType)
        self.assertEqual(lookup_casstype_simple('ShortType'), cassandra.cqltypes.ShortType)
        self.assertEqual(lookup_casstype_simple('TimeUUIDType'), cassandra.cqltypes.TimeUUIDType)
        self.assertEqual(lookup_casstype_simple('TimeType'), cassandra.cqltypes.TimeType)
        self.assertEqual(lookup_casstype_simple('UUIDType'), cassandra.cqltypes.UUIDType)
        self.assertEqual(lookup_casstype_simple('IntegerType'), cassandra.cqltypes.IntegerType)
        self.assertEqual(lookup_casstype_simple('MapType'), cassandra.cqltypes.MapType)
        self.assertEqual(lookup_casstype_simple('ListType'), cassandra.cqltypes.ListType)
        self.assertEqual(lookup_casstype_simple('SetType'), cassandra.cqltypes.SetType)
        self.assertEqual(lookup_casstype_simple('CompositeType'), cassandra.cqltypes.CompositeType)
        self.assertEqual(lookup_casstype_simple('ColumnToCollectionType'), cassandra.cqltypes.ColumnToCollectionType)
        self.assertEqual(lookup_casstype_simple('ReversedType'), cassandra.cqltypes.ReversedType)
        self.assertEqual(lookup_casstype_simple('DurationType'), cassandra.cqltypes.DurationType)
        self.assertEqual(lookup_casstype_simple('DateRangeType'), cassandra.cqltypes.DateRangeType)

        self.assertEqual(str(lookup_casstype_simple('unknown')), str(cassandra.cqltypes.mkUnrecognizedType('unknown')))

    def test_lookup_casstype(self):
        """
        Ensure lookup_casstype returns the correct classes
        """

        self.assertEqual(lookup_casstype('AsciiType'), cassandra.cqltypes.AsciiType)
        self.assertEqual(lookup_casstype('LongType'), cassandra.cqltypes.LongType)
        self.assertEqual(lookup_casstype('BytesType'), cassandra.cqltypes.BytesType)
        self.assertEqual(lookup_casstype('BooleanType'), cassandra.cqltypes.BooleanType)
        self.assertEqual(lookup_casstype('CounterColumnType'), cassandra.cqltypes.CounterColumnType)
        self.assertEqual(lookup_casstype('DateType'), cassandra.cqltypes.DateType)
        self.assertEqual(lookup_casstype('DecimalType'), cassandra.cqltypes.DecimalType)
        self.assertEqual(lookup_casstype('DoubleType'), cassandra.cqltypes.DoubleType)
        self.assertEqual(lookup_casstype('FloatType'), cassandra.cqltypes.FloatType)
        self.assertEqual(lookup_casstype('InetAddressType'), cassandra.cqltypes.InetAddressType)
        self.assertEqual(lookup_casstype('Int32Type'), cassandra.cqltypes.Int32Type)
        self.assertEqual(lookup_casstype('UTF8Type'), cassandra.cqltypes.UTF8Type)
        self.assertEqual(lookup_casstype('DateType'), cassandra.cqltypes.DateType)
        self.assertEqual(lookup_casstype('TimeType'), cassandra.cqltypes.TimeType)
        self.assertEqual(lookup_casstype('ByteType'), cassandra.cqltypes.ByteType)
        self.assertEqual(lookup_casstype('ShortType'), cassandra.cqltypes.ShortType)
        self.assertEqual(lookup_casstype('TimeUUIDType'), cassandra.cqltypes.TimeUUIDType)
        self.assertEqual(lookup_casstype('UUIDType'), cassandra.cqltypes.UUIDType)
        self.assertEqual(lookup_casstype('IntegerType'), cassandra.cqltypes.IntegerType)
        self.assertEqual(lookup_casstype('MapType'), cassandra.cqltypes.MapType)
        self.assertEqual(lookup_casstype('ListType'), cassandra.cqltypes.ListType)
        self.assertEqual(lookup_casstype('SetType'), cassandra.cqltypes.SetType)
        self.assertEqual(lookup_casstype('CompositeType'), cassandra.cqltypes.CompositeType)
        self.assertEqual(lookup_casstype('ColumnToCollectionType'), cassandra.cqltypes.ColumnToCollectionType)
        self.assertEqual(lookup_casstype('ReversedType'), cassandra.cqltypes.ReversedType)
        self.assertEqual(lookup_casstype('DurationType'), cassandra.cqltypes.DurationType)
        self.assertEqual(lookup_casstype('DateRangeType'), cassandra.cqltypes.DateRangeType)

        self.assertEqual(str(lookup_casstype('unknown')), str(cassandra.cqltypes.mkUnrecognizedType('unknown')))

        self.assertRaises(ValueError, lookup_casstype, 'AsciiType~')

    def test_casstype_parameterized(self):
        self.assertEqual(LongType.cass_parameterized_type_with(()), 'LongType')
        self.assertEqual(LongType.cass_parameterized_type_with((), full=True), 'org.apache.cassandra.db.marshal.LongType')
        self.assertEqual(SetType.cass_parameterized_type_with([DecimalType], full=True), 'org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.DecimalType)')

        self.assertEqual(LongType.cql_parameterized_type(), 'bigint')

        subtypes = (cassandra.cqltypes.UTF8Type, cassandra.cqltypes.UTF8Type)
        self.assertEqual('map<text, text>',
                         cassandra.cqltypes.MapType.apply_parameters(subtypes).cql_parameterized_type())

    def test_datetype_from_string(self):
        # Ensure all formats can be parsed, without exception
        for format in cassandra.cqltypes.cql_timestamp_formats:
            date_string = str(datetime.datetime.now().strftime(format))
            cassandra.cqltypes.DateType.interpret_datestring(date_string)

    def test_cql_typename(self):
        """
        Smoke test cql_typename
        """

        self.assertEqual(cql_typename('DateType'), 'timestamp')
        self.assertEqual(cql_typename('org.apache.cassandra.db.marshal.ListType(IntegerType)'), 'list<varint>')

    def test_named_tuple_colname_substitution(self):
        colnames = ("func(abc)", "[applied]", "func(func(abc))", "foo_bar", "foo_bar_")
        rows = [(1, 2, 3, 4, 5)]
        result = named_tuple_factory(colnames, rows)[0]
        self.assertEqual(result[0], result.func_abc)
        self.assertEqual(result[1], result.applied)
        self.assertEqual(result[2], result.func_func_abc)
        self.assertEqual(result[3], result.foo_bar)
        self.assertEqual(result[4], result.foo_bar_)

    def test_parse_casstype_args(self):
        class FooType(CassandraType):
            typename = 'org.apache.cassandra.db.marshal.FooType'

            def __init__(self, subtypes, names):
                self.subtypes = subtypes
                self.names = names

            @classmethod
            def apply_parameters(cls, subtypes, names):
                return cls(subtypes, [unhexlify(name.encode()) if name is not None else name for name in names])

        class BarType(FooType):
            typename = 'org.apache.cassandra.db.marshal.BarType'

        ctype = parse_casstype_args(''.join((
            'org.apache.cassandra.db.marshal.FooType(',
            '63697479:org.apache.cassandra.db.marshal.UTF8Type,',
            'BarType(61646472657373:org.apache.cassandra.db.marshal.UTF8Type),',
            '7a6970:org.apache.cassandra.db.marshal.UTF8Type',
            ')')))

        self.assertEqual(FooType, ctype.__class__)

        self.assertEqual(UTF8Type, ctype.subtypes[0])

        # middle subtype should be a BarType instance with its own subtypes and names
        self.assertIsInstance(ctype.subtypes[1], BarType)
        self.assertEqual([UTF8Type], ctype.subtypes[1].subtypes)
        self.assertEqual([b"address"], ctype.subtypes[1].names)

        self.assertEqual(UTF8Type, ctype.subtypes[2])
        self.assertEqual([b'city', None, b'zip'], ctype.names)

    def test_parse_casstype_vector(self):
        ctype = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 3)")
        self.assertTrue(issubclass(ctype, VectorType))
        self.assertEqual(3, ctype.vector_size)
        self.assertEqual(FloatType, ctype.subtype)

    def test_parse_casstype_vector_of_vectors(self):
        inner_type = "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 4)"
        ctype = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(%s, 3)" % (inner_type))
        self.assertTrue(issubclass(ctype, VectorType))
        self.assertEqual(3, ctype.vector_size)
        sub_ctype = ctype.subtype
        self.assertTrue(issubclass(sub_ctype, VectorType))
        self.assertEqual(4, sub_ctype.vector_size)
        self.assertEqual(FloatType, sub_ctype.subtype)

    def test_empty_value(self):
        self.assertEqual(str(EmptyValue()), 'EMPTY')

    def test_datetype(self):
        now_time_seconds = time.time()
        now_datetime = datetime.datetime.fromtimestamp(now_time_seconds, tz=datetime.timezone.utc)

        # Cassandra timestamps in millis
        now_timestamp = now_time_seconds * 1e3

        # same results serialized
        self.assertEqual(DateType.serialize(now_datetime, 0), DateType.serialize(now_timestamp, 0))

        # deserialize
        # epoc
        expected = 0
        self.assertEqual(DateType.deserialize(int64_pack(1000 * expected), 0), datetime.datetime.fromtimestamp(expected, tz=datetime.timezone.utc).replace(tzinfo=None))

        # beyond 32b
        expected = 2 ** 33
        self.assertEqual(DateType.deserialize(int64_pack(1000 * expected), 0), datetime.datetime(2242, 3, 16, 12, 56, 32, tzinfo=datetime.timezone.utc).replace(tzinfo=None))

        # less than epoc (PYTHON-119)
        expected = -770172256
        self.assertEqual(DateType.deserialize(int64_pack(1000 * expected), 0), datetime.datetime(1945, 8, 5, 23, 15, 44, tzinfo=datetime.timezone.utc).replace(tzinfo=None))

        # work around rounding difference among Python versions (PYTHON-230)
        expected = 1424817268.274
        self.assertEqual(DateType.deserialize(int64_pack(int(1000 * expected)), 0), datetime.datetime(2015, 2, 24, 22, 34, 28, 274000, tzinfo=datetime.timezone.utc).replace(tzinfo=None))

        # Large date overflow (PYTHON-452)
        expected = 2177403010.123
        self.assertEqual(DateType.deserialize(int64_pack(int(1000 * expected)), 0), datetime.datetime(2038, 12, 31, 10, 10, 10, 123000, tzinfo=datetime.timezone.utc).replace(tzinfo=None))

    def test_collection_null_support(self):
        """
        Test that null values in collection are decoded properly.

        @jira_ticket PYTHON-1123
        """
        int_list = ListType.apply_parameters([Int32Type])
        value = (
                int32_pack(2) +  # num items
                int32_pack(-1) +  # size of item1
                int32_pack(4) +  # size of item2
                int32_pack(42)  # item2
        )
        self.assertEqual(
            [None, 42],
            int_list.deserialize(value, 3)
        )

        set_list = SetType.apply_parameters([Int32Type])
        self.assertEqual(
            {None, 42},
            set(set_list.deserialize(value, 3))
        )

        value = (
                int32_pack(2) +  # num items
                int32_pack(4) +  # key size of item1
                int32_pack(42) +  # key item1
                int32_pack(-1) +  # value size of item1
                int32_pack(-1) +  # key size of item2
                int32_pack(4) +  # value size of item2
                int32_pack(42)  # value of item2
        )

        map_list = MapType.apply_parameters([Int32Type, Int32Type])
        self.assertEqual(
            [(42, None), (None, 42)],
            map_list.deserialize(value, 3)._items  # OrderedMapSerializedKey
        )

    def test_write_read_string(self):
        with tempfile.TemporaryFile() as f:
            value = u'test'
            write_string(f, value)
            f.seek(0)
            self.assertEqual(read_string(f), value)

    def test_write_read_longstring(self):
        with tempfile.TemporaryFile() as f:
            value = u'test'
            write_longstring(f, value)
            f.seek(0)
            self.assertEqual(read_longstring(f), value)

    def test_write_read_stringmap(self):
        with tempfile.TemporaryFile() as f:
            value = {'key': 'value'}
            write_stringmap(f, value)
            f.seek(0)
            self.assertEqual(read_stringmap(f), value)

    def test_write_read_inet(self):
        with tempfile.TemporaryFile() as f:
            value = ('192.168.1.1', 9042)
            write_inet(f, value)
            f.seek(0)
            self.assertEqual(read_inet(f), value)

        with tempfile.TemporaryFile() as f:
            value = ('2001:db8:0:f101::1', 9042)
            write_inet(f, value)
            f.seek(0)
            self.assertEqual(read_inet(f), value)

    def test_cql_quote(self):
        self.assertEqual(cql_quote(u'test'), "'test'")
        self.assertEqual(cql_quote('test'), "'test'")
        self.assertEqual(cql_quote(0), '0')


class VectorTests(unittest.TestCase):
    def _normalize_set(self, val):
        if isinstance(val, set) or isinstance(val, util.SortedSet):
            return frozenset([self._normalize_set(v) for v in val])
        return val

    def _round_trip_compare_fn(self, first, second):
        if isinstance(first, float):
            self.assertAlmostEqual(first, second, places=5)
        elif isinstance(first, list):
            self.assertEqual(len(first), len(second))
            for (felem, selem) in zip(first, second):
                self._round_trip_compare_fn(felem, selem)
        elif isinstance(first, set) or isinstance(first, frozenset):
            self.assertEqual(len(first), len(second))
            first_norm = self._normalize_set(first)
            second_norm = self._normalize_set(second)
            self.assertEqual(first_norm, second_norm)
        elif isinstance(first, dict):
            for ((fk,fv), (sk,sv)) in zip(first.items(), second.items()):
                self._round_trip_compare_fn(fk, sk)
                self._round_trip_compare_fn(fv, sv)
        else:
            self.assertEqual(first,second)

    def _round_trip_test(self, data, ctype_str):
        ctype = parse_casstype_args(ctype_str)
        data_bytes = ctype.serialize(data, 0)
        serialized_size = ctype.subtype.serial_size()
        if serialized_size:
            self.assertEqual(serialized_size * len(data), len(data_bytes))
        result = ctype.deserialize(data_bytes, 0)
        self.assertEqual(len(data), len(result))
        for idx in range(0,len(data)):
            self._round_trip_compare_fn(data[idx], result[idx])

    def test_round_trip_basic_types_with_fixed_serialized_size(self):
        self._round_trip_test([True, False, False, True], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.BooleanType, 4)")
        self._round_trip_test([3.4, 2.9, 41.6, 12.0], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 4)")
        self._round_trip_test([3.4, 2.9, 41.6, 12.0], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.DoubleType, 4)")
        self._round_trip_test([3, 2, 41, 12], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.LongType, 4)")
        self._round_trip_test([3, 2, 41, 12], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.Int32Type, 4)")
        self._round_trip_test([uuid.uuid1(), uuid.uuid1(), uuid.uuid1(), uuid.uuid1()], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.TimeUUIDType, 4)")
        self._round_trip_test([3, 2, 41, 12], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.ShortType, 4)")

    def test_round_trip_basic_types_without_fixed_serialized_size(self):
        # Varints
        self._round_trip_test([3, 2, 41, 12], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.IntegerType, 4)")
        # ASCII text
        self._round_trip_test(["abc", "def", "ghi", "jkl"], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.AsciiType, 4)")
        # UTF8 text
        self._round_trip_test(["abc", "def", "ghi", "jkl"], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.UTF8Type, 4)")
        # Time is something of a weird one.  By rights, it should be a fixed size type but C* code marks it as variable
        # size.  We're forced to follow the C* code base (since that's who'll be providing the data we're parsing) so
        # we match what they're doing.
        self._round_trip_test([datetime.time(1,1,1), datetime.time(2,2,2), datetime.time(3,3,3)], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.TimeType, 3)")
        # Duration (contains varints)
        self._round_trip_test([util.Duration(1,1,1), util.Duration(2,2,2), util.Duration(3,3,3)], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.DurationType, 3)")

    def test_round_trip_collection_types(self):
        # List (subtype of fixed size)
        self._round_trip_test([[1, 2, 3, 4], [5, 6], [7, 8, 9, 10], [11, 12]], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.ListType \
                (org.apache.cassandra.db.marshal.Int32Type), 4)")
        # Set (subtype of fixed size)
        self._round_trip_test([set([1, 2, 3, 4]), set([5, 6]), set([7, 8, 9, 10]), set([11, 12])], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.SetType \
                (org.apache.cassandra.db.marshal.Int32Type), 4)")
        # Map (subtype of fixed size)
        self._round_trip_test([{1:1.2}, {2:3.4}, {3:5.6}, {4:7.8}], \
                "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.MapType \
                    (org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.FloatType), 4)")
        # List (subtype without fixed size)
        self._round_trip_test([["one","two"], ["three","four"], ["five","six"], ["seven","eight"]], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.ListType \
                (org.apache.cassandra.db.marshal.AsciiType), 4)")
        # Set (subtype without fixed size)
        self._round_trip_test([set(["one","two"]), set(["three","four"]), set(["five","six"]), set(["seven","eight"])], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.SetType \
                (org.apache.cassandra.db.marshal.AsciiType), 4)")
        # Map (subtype without fixed size)
        self._round_trip_test([{1:"one"}, {2:"two"}, {3:"three"}, {4:"four"}], \
                "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.MapType \
                    (org.apache.cassandra.db.marshal.IntegerType,org.apache.cassandra.db.marshal.AsciiType), 4)")
        # List of lists (subtype without fixed size)
        data = [[["one","two"],["three"]], [["four"],["five"]], [["six","seven","eight"]], [["nine"]]]
        ctype = "org.apache.cassandra.db.marshal.VectorType\
                    (org.apache.cassandra.db.marshal.ListType\
                        (org.apache.cassandra.db.marshal.ListType\
                            (org.apache.cassandra.db.marshal.AsciiType)), 4)"
        self._round_trip_test(data, ctype)
        # Set of sets (subtype without fixed size)
        data = [set([frozenset(["one","two"]),frozenset(["three"])]),\
                set([frozenset(["four"]),frozenset(["five"])]),\
                set([frozenset(["six","seven","eight"])]),
                set([frozenset(["nine"])])]
        ctype = "org.apache.cassandra.db.marshal.VectorType\
                    (org.apache.cassandra.db.marshal.SetType\
                        (org.apache.cassandra.db.marshal.SetType\
                            (org.apache.cassandra.db.marshal.AsciiType)), 4)"
        self._round_trip_test(data, ctype)
        # Map of maps (subtype without fixed size)
        data = [{100:{1:"one",2:"two",3:"three"}},\
                {200:{4:"four",5:"five"}},\
                {300:{}},\
                {400:{6:"six"}}]
        ctype = "org.apache.cassandra.db.marshal.VectorType\
                    (org.apache.cassandra.db.marshal.MapType\
                        (org.apache.cassandra.db.marshal.Int32Type,\
                        org.apache.cassandra.db.marshal.MapType \
                            (org.apache.cassandra.db.marshal.IntegerType,org.apache.cassandra.db.marshal.AsciiType)), 4)"
        self._round_trip_test(data, ctype)

    def test_round_trip_vector_of_vectors(self):
        # Subytpes of subtypes with a fixed size
        self._round_trip_test([[1.2, 3.4], [5.6, 7.8], [9.10, 11.12], [13.14, 15.16]], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.VectorType \
                (org.apache.cassandra.db.marshal.FloatType,2), 4)")

        # Subytpes of subtypes without a fixed size
        self._round_trip_test([["one", "two"], ["three", "four"], ["five", "six"], ["seven", "eight"]], \
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.VectorType \
                (org.apache.cassandra.db.marshal.AsciiType,2), 4)")

    # parse_casstype_args() is tested above... we're explicitly concerned about cql_parameterized_type() output here
    def test_cql_parameterized_type(self):
        # Base vector functionality
        ctype = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 4)")
        self.assertEqual(ctype.cql_parameterized_type(), "org.apache.cassandra.db.marshal.VectorType<float, 4>")

        # Test vector-of-vectors
        inner_type = "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 4)"
        ctype = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(%s, 3)" % (inner_type))
        inner_parsed_type = "org.apache.cassandra.db.marshal.VectorType<float, 4>"
        self.assertEqual(ctype.cql_parameterized_type(), "org.apache.cassandra.db.marshal.VectorType<%s, 3>" % (inner_parsed_type))

    def test_serialization_fixed_size_too_small(self):
        ctype = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 5)")
        with self.assertRaisesRegex(ValueError, "Expected sequence of size 5 for vector of type float and dimension 5, observed sequence of length 4"):
            ctype.serialize([1.2, 3.4, 5.6, 7.8], 0)

    def test_serialization_fixed_size_too_big(self):
        ctype = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 4)")
        with self.assertRaisesRegex(ValueError, "Expected sequence of size 4 for vector of type float and dimension 4, observed sequence of length 5"):
            ctype.serialize([1.2, 3.4, 5.6, 7.8, 9.10], 0)

    def test_serialization_variable_size_too_small(self):
        ctype = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.IntegerType, 5)")
        with self.assertRaisesRegex(ValueError, "Expected sequence of size 5 for vector of type varint and dimension 5, observed sequence of length 4"):
            ctype.serialize([1, 2, 3, 4], 0)

    def test_serialization_variable_size_too_big(self):
        ctype = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.IntegerType, 4)")
        with self.assertRaisesRegex(ValueError, "Expected sequence of size 4 for vector of type varint and dimension 4, observed sequence of length 5"):
            ctype.serialize([1, 2, 3, 4, 5], 0)

    def test_deserialization_fixed_size_too_small(self):
        ctype_four = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 4)")
        ctype_four_bytes = ctype_four.serialize([1.2, 3.4, 5.6, 7.8], 0)
        ctype_five = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 5)")
        with self.assertRaisesRegex(ValueError, "Expected vector of type float and dimension 5 to have serialized size 20; observed serialized size of 16 instead"):
            ctype_five.deserialize(ctype_four_bytes, 0)

    def test_deserialization_fixed_size_too_big(self):
        ctype_five = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 5)")
        ctype_five_bytes = ctype_five.serialize([1.2, 3.4, 5.6, 7.8, 9.10], 0)
        ctype_four = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType, 4)")
        with self.assertRaisesRegex(ValueError, "Expected vector of type float and dimension 4 to have serialized size 16; observed serialized size of 20 instead"):
            ctype_four.deserialize(ctype_five_bytes, 0)

    def test_deserialization_variable_size_too_small(self):
        ctype_four = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.IntegerType, 4)")
        ctype_four_bytes = ctype_four.serialize([1, 2, 3, 4], 0)
        ctype_five = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.IntegerType, 5)")
        with self.assertRaisesRegex(ValueError, "Error reading additional data during vector deserialization after successfully adding 4 elements"):
            ctype_five.deserialize(ctype_four_bytes, 0)

    def test_deserialization_variable_size_too_big(self):
        ctype_five = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.IntegerType, 5)")
        ctype_five_bytes = ctype_five.serialize([1, 2, 3, 4, 5], 0)
        ctype_four = parse_casstype_args("org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.IntegerType, 4)")
        with self.assertRaisesRegex(ValueError, "Additional bytes remaining after vector deserialization completed"):
            ctype_four.deserialize(ctype_five_bytes, 0)


ZERO = datetime.timedelta(0)


class UTC(datetime.tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO


try:
    utc_timezone = datetime.timezone.utc
except AttributeError:
    utc_timezone = UTC()


class DateRangeTypeTests(unittest.TestCase):
    dt = datetime.datetime(1990, 2, 3, 13, 58, 45, 777777)
    timestamp = 1485963732404

    def test_month_rounding_creation_failure(self):
        """
        @jira_ticket PYTHON-912
        """
        feb_stamp = ms_timestamp_from_datetime(
            datetime.datetime(2018, 2, 25, 18, 59, 59, 0)
        )
        dr = DateRange(OPEN_BOUND,
                  DateRangeBound(feb_stamp, DateRangePrecision.MONTH))
        dt = datetime_from_timestamp(dr.upper_bound.milliseconds / 1000)
        self.assertEqual(dt.day, 28)

        # Leap year
        feb_stamp_leap_year = ms_timestamp_from_datetime(
            datetime.datetime(2016, 2, 25, 18, 59, 59, 0)
        )
        dr = DateRange(OPEN_BOUND,
                       DateRangeBound(feb_stamp_leap_year, DateRangePrecision.MONTH))
        dt = datetime_from_timestamp(dr.upper_bound.milliseconds / 1000)
        self.assertEqual(dt.day, 29)

    def test_decode_precision(self):
        self.assertEqual(DateRangeType._decode_precision(6), 'MILLISECOND')

    def test_decode_precision_error(self):
        with self.assertRaises(ValueError):
            DateRangeType._decode_precision(-1)

    def test_encode_precision(self):
        self.assertEqual(DateRangeType._encode_precision('SECOND'), 5)

    def test_encode_precision_error(self):
        with self.assertRaises(ValueError):
            DateRangeType._encode_precision('INVALID')

    def test_deserialize_single_value(self):
        serialized = (int8_pack(0) +
                      int64_pack(self.timestamp) +
                      int8_pack(3))
        self.assertEqual(
            DateRangeType.deserialize(serialized, 5),
            util.DateRange(value=util.DateRangeBound(
                value=datetime.datetime(2017, 2, 1, 15, 42, 12, 404000),
                precision='HOUR')
            )
        )

    def test_deserialize_closed_range(self):
        serialized = (int8_pack(1) +
                      int64_pack(self.timestamp) +
                      int8_pack(2) +
                      int64_pack(self.timestamp) +
                      int8_pack(6))
        self.assertEqual(
            DateRangeType.deserialize(serialized, 5),
            util.DateRange(
                lower_bound=util.DateRangeBound(
                    value=datetime.datetime(2017, 2, 1, 0, 0),
                    precision='DAY'
                ),
                upper_bound=util.DateRangeBound(
                    value=datetime.datetime(2017, 2, 1, 15, 42, 12, 404000),
                    precision='MILLISECOND'
                )
            )
        )

    def test_deserialize_open_high(self):
        serialized = (int8_pack(2) +
                      int64_pack(self.timestamp) +
                      int8_pack(3))
        deserialized = DateRangeType.deserialize(serialized, 5)
        self.assertEqual(
            deserialized,
            util.DateRange(
                lower_bound=util.DateRangeBound(
                    value=datetime.datetime(2017, 2, 1, 15, 0),
                    precision='HOUR'
                ),
                upper_bound=util.OPEN_BOUND
            )
        )

    def test_deserialize_open_low(self):
        serialized = (int8_pack(3) +
                      int64_pack(self.timestamp) +
                      int8_pack(4))
        deserialized = DateRangeType.deserialize(serialized, 5)
        self.assertEqual(
            deserialized,
            util.DateRange(
                lower_bound=util.OPEN_BOUND,
                upper_bound=util.DateRangeBound(
                    value=datetime.datetime(2017, 2, 1, 15, 42, 20, 1000),
                    precision='MINUTE'
                )
            )
        )

    def test_deserialize_single_open(self):
        self.assertEqual(
            util.DateRange(value=util.OPEN_BOUND),
            DateRangeType.deserialize(int8_pack(5), 5)
        )

    def test_serialize_single_value(self):
        serialized = (int8_pack(0) +
                      int64_pack(self.timestamp) +
                      int8_pack(5))
        deserialized = DateRangeType.deserialize(serialized, 5)
        self.assertEqual(
            deserialized,
            util.DateRange(
                value=util.DateRangeBound(
                    value=datetime.datetime(2017, 2, 1, 15, 42, 12),
                    precision='SECOND'
                )
            )
        )

    def test_serialize_closed_range(self):
        serialized = (int8_pack(1) +
                      int64_pack(self.timestamp) +
                      int8_pack(5) +
                      int64_pack(self.timestamp) +
                      int8_pack(0))
        deserialized = DateRangeType.deserialize(serialized, 5)
        self.assertEqual(
            deserialized,
            util.DateRange(
                lower_bound=util.DateRangeBound(
                    value=datetime.datetime(2017, 2, 1, 15, 42, 12),
                    precision='SECOND'
                ),
                upper_bound=util.DateRangeBound(
                    value=datetime.datetime(2017, 12, 31),
                    precision='YEAR'
                )
            )
        )

    def test_serialize_open_high(self):
        serialized = (int8_pack(2) +
                      int64_pack(self.timestamp) +
                      int8_pack(2))
        deserialized = DateRangeType.deserialize(serialized, 5)
        self.assertEqual(
            deserialized,
            util.DateRange(
                lower_bound=util.DateRangeBound(
                    value=datetime.datetime(2017, 2, 1),
                    precision='DAY'
                ),
                upper_bound=util.OPEN_BOUND
            )
        )

    def test_serialize_open_low(self):
        serialized = (int8_pack(2) +
                      int64_pack(self.timestamp) +
                      int8_pack(3))
        deserialized = DateRangeType.deserialize(serialized, 5)
        self.assertEqual(
            deserialized,
            util.DateRange(
                lower_bound=util.DateRangeBound(
                    value=datetime.datetime(2017, 2, 1, 15),
                    precision='HOUR'
                ),
                upper_bound=util.OPEN_BOUND
            )
        )

    def test_deserialize_both_open(self):
        serialized = (int8_pack(4))
        deserialized = DateRangeType.deserialize(serialized, 5)
        self.assertEqual(
            deserialized,
            util.DateRange(
                lower_bound=util.OPEN_BOUND,
                upper_bound=util.OPEN_BOUND
            )
        )

    def test_serialize_single_open(self):
        serialized = DateRangeType.serialize(util.DateRange(
            value=util.OPEN_BOUND,
        ), 5)
        self.assertEqual(int8_pack(5), serialized)

    def test_serialize_both_open(self):
        serialized = DateRangeType.serialize(util.DateRange(
            lower_bound=util.OPEN_BOUND,
            upper_bound=util.OPEN_BOUND
        ), 5)
        self.assertEqual(int8_pack(4), serialized)

    def test_failure_to_serialize_no_value_object(self):
        self.assertRaises(ValueError, DateRangeType.serialize, object(), 5)

    def test_failure_to_serialize_no_bounds_object(self):
        class no_bounds_object(object):
            value = lower_bound = None
        self.assertRaises(ValueError, DateRangeType.serialize, no_bounds_object, 5)

    def test_serialized_value_round_trip(self):
        vals = [b'\x01\x00\x00\x01%\xe9a\xf9\xd1\x06\x00\x00\x01v\xbb>o\xff\x00',
                b'\x01\x00\x00\x00\xdcm\x03-\xd1\x06\x00\x00\x01v\xbb>o\xff\x00']
        for serialized in vals:
            self.assertEqual(
                serialized,
                DateRangeType.serialize(DateRangeType.deserialize(serialized, 0), 0)
            )

    def test_serialize_zero_datetime(self):
        """
        Test serialization where timestamp = 0

        Companion test for test_deserialize_zero_datetime

        @since 2.0.0
        @jira_ticket PYTHON-729
        @expected_result serialization doesn't raise an error

        @test_category data_types
        """
        DateRangeType.serialize(util.DateRange(
            lower_bound=(datetime.datetime(1970, 1, 1), 'YEAR'),
            upper_bound=(datetime.datetime(1970, 1, 1), 'YEAR')
        ), 5)

    def test_deserialize_zero_datetime(self):
        """
        Test deserialization where timestamp = 0

        Reproduces PYTHON-729

        @since 2.0.0
        @jira_ticket PYTHON-729
        @expected_result deserialization doesn't raise an error

        @test_category data_types
        """
        DateRangeType.deserialize(
            (int8_pack(1) +
             int64_pack(0) + int8_pack(0) +
             int64_pack(0) + int8_pack(0)),
            5
        )


class DateRangeDeserializationTests(unittest.TestCase):
    """
    These tests iterate over different timestamp values
    and assert deserialization gives the expected value
    """

    starting_lower_value = 1514744108923
    """
    Sample starting value for the lower bound for DateRange
    """
    starting_upper_value = 2148761288922
    """
    Sample starting value for the upper bound for DateRange
    """

    epoch = datetime.datetime(1970, 1, 1, tzinfo=utc_timezone)

    def test_deserialize_date_range_milliseconds(self):
        """
        Test rounding from DateRange for milliseconds

        @since 2.0.0
        @jira_ticket PYTHON-898
        @expected_result

        @test_category data_types
        """
        for i in range(1000):
            lower_value = self.starting_lower_value + i
            upper_value = self.starting_upper_value + i
            dr = DateRange(DateRangeBound(lower_value, DateRangePrecision.MILLISECOND),
                           DateRangeBound(upper_value, DateRangePrecision.MILLISECOND))
            self.assertEqual(lower_value, dr.lower_bound.milliseconds)
            self.assertEqual(upper_value, dr.upper_bound.milliseconds)

    def test_deserialize_date_range_seconds(self):
        """
        Test rounding from DateRange for milliseconds

        @since 2.0.0
        @jira_ticket PYTHON-898
        @expected_result

        @test_category data_types
        """

        def truncate_last_figures(number, n=3):
            """
            Truncates last n digits of a number
            """
            return int(str(number)[:-n] + '0' * n)

        for i in range(1000):
            lower_value = self.starting_lower_value + i * 900
            upper_value = self.starting_upper_value + i * 900
            dr = DateRange(DateRangeBound(lower_value, DateRangePrecision.SECOND),
                           DateRangeBound(upper_value, DateRangePrecision.SECOND))

            self.assertEqual(truncate_last_figures(lower_value), dr.lower_bound.milliseconds)
            upper_value = truncate_last_figures(upper_value) + 999
            self.assertEqual(upper_value, dr.upper_bound.milliseconds)

    def test_deserialize_date_range_minutes(self):
        """
        Test rounding from DateRange for seconds

        @since 2.4.0
        @jira_ticket PYTHON-898
        @expected_result

        @test_category data_types
        """
        self._deserialize_date_range({"second": 0, "microsecond": 0},
                                     DateRangePrecision.MINUTE,
                                     # This lambda function given a truncated date adds
                                     # one day minus one microsecond in microseconds
                                     lambda x: x + 59 * 1000 + 999,
                                     lambda original_value, i: original_value + i * 900 * 50)

    def test_deserialize_date_range_hours(self):
        """
        Test rounding from DateRange for hours

        @since 2.4.0
        @jira_ticket PYTHON-898
        @expected_result

        @test_category data_types
        """
        self._deserialize_date_range({"minute": 0, "second": 0, "microsecond": 0},
                                     DateRangePrecision.HOUR,
                                     # This lambda function given a truncated date adds
                                     # one hour minus one microsecond in microseconds
                                     lambda x: x +
                                               59 * 60 * 1000 +
                                               59 * 1000 +
                                               999,
                                     lambda original_value, i: original_value + i * 900 * 50 * 60)

    def test_deserialize_date_range_day(self):
        """
        Test rounding from DateRange for hours

        @since 2.4.0
        @jira_ticket PYTHON-898
        @expected_result

        @test_category data_types
        """
        self._deserialize_date_range({"hour": 0, "minute": 0, "second": 0, "microsecond": 0},
                                     DateRangePrecision.DAY,
                                     # This lambda function given a truncated date adds
                                     # one day minus one microsecond in microseconds
                                     lambda x: x +
                                               23 * 60 * 60 * 1000 +
                                               59 * 60 * 1000 +
                                               59 * 1000 +
                                               999,
                                     lambda original_value, i: original_value + i * 900 * 50 * 60 * 24)

    @unittest.skip("This is currently failing, see PYTHON-912")
    def test_deserialize_date_range_month(self):
        """
        Test rounding from DateRange for months

        @since 2.4.0
        @jira_ticket PYTHON-898
        @expected_result

        @test_category data_types
        """
        def get_upper_bound(seconds):
            """
            function that given a truncated date in seconds from the epoch returns that same date
            but with the microseconds set to 999999, seconds to 59, minutes to 59, hours to 23
            and days 28, 29, 30 or 31 depending on the month.
            The way to do this is to add one month and leave the date at YEAR-MONTH-01 00:00:00 000000.
            Then subtract one millisecond.
            """
            dt = datetime.datetime.fromtimestamp(seconds / 1000.0, tz=utc_timezone)
            dt = dt + datetime.timedelta(days=32)
            dt = dt.replace(day=1) - datetime.timedelta(microseconds=1)
            return int((dt - self.epoch).total_seconds() * 1000)
        self._deserialize_date_range({"day": 1, "hour": 0, "minute": 0, "second": 0, "microsecond": 0},
                                     DateRangePrecision.MONTH,
                                     get_upper_bound,
                                     lambda original_value, i: original_value + i * 900 * 50 * 60 * 24 * 30)

    def test_deserialize_date_range_year(self):
        """
        Test rounding from DateRange for year

        @since 2.4.0
        @jira_ticket PYTHON-898
        @expected_result

        @test_category data_types
        """
        def get_upper_bound(seconds):
            """
            function that given a truncated date in seconds from the epoch returns that same date
            but with the microseconds set to 999999, seconds to 59, minutes to 59, hours to 23
            days 28, 29, 30 or 31 depending on the month and months to 12.
            The way to do this is to add one year and leave the date at YEAR-01-01 00:00:00 000000.
            Then subtract one millisecond.
            """
            dt = datetime.datetime.fromtimestamp(seconds / 1000.0, tz=utc_timezone)
            dt = dt + datetime.timedelta(days=370)
            dt = dt.replace(day=1) - datetime.timedelta(microseconds=1)

            diff = time.mktime(dt.timetuple()) - time.mktime(self.epoch.timetuple())
            return diff * 1000 + 999
            # This doesn't work for big values because it loses precision
            #return int((dt - self.epoch).total_seconds() * 1000)
        self._deserialize_date_range({"month": 1, "day": 1, "hour": 0, "minute": 0, "second": 0, "microsecond": 0},
                                     DateRangePrecision.YEAR,
                                     get_upper_bound,
                                     lambda original_value, i: original_value + i * 900 * 50 * 60 * 24 * 30 * 12 * 7)

    def _deserialize_date_range(self, truncate_kwargs, precision,
                                round_up_truncated_upper_value, increment_loop_variable):
        """
        This functions iterates over several DateRange objects determined by
        lower_value upper_value which are given as a value that represents seconds since the epoch.
        We want to make sure the lower_value is correctly rounded down and the upper value is correctly rounded up.
        In the case of rounding down we verify that the rounded down value
        has the appropriate fields set to the minimum they could possibly have. That is
        1 for months, 1 for days, 0 for hours, 0 for minutes, 0 for seconds, 0 for microseconds.
        We use the generic function truncate_date which depends on truncate_kwargs for this

        In the case of rounding up we verify that the rounded up value has the appropriate fields set
        to the maximum they could possibly have. This is calculated by round_up_truncated_upper_value
        which input is the truncated value from before. It is passed as an argument as the way
        of calculating this is different for every precision.

        :param truncate_kwargs: determine what values to truncate in truncate_date
        :param precision: :class:`~util.DateRangePrecision`
        :param round_up_truncated_upper_value: this is a function that gets a truncated date and
        returns a new date with some fields set to the maximum possible value
        :param increment_loop_variable: this is a function that given a starting value and the iteration
        value returns a new date to serve as lower_bound/upper_bound. We need this because the value by which
        dates are incremented depends on if the precision is seconds, minutes, hours, days and months
        :return:
        """

        def truncate_date(number):
            """
            Given a date in seconds since the epoch truncates ups to a certain precision depending on
            truncate_kwargs.
            The return is the truncated date in seconds since the epoch.
            For example if truncate_kwargs = {"hour": 0, "minute": 0, "second": 0, "microsecond": 0} the returned
            value will be the original given date but with the hours, minutes, seconds and microseconds set to 0
            """
            dt = datetime.datetime.fromtimestamp(number / 1000.0, tz=utc_timezone)
            dt = dt.replace(**truncate_kwargs)
            return round((dt - self.epoch).total_seconds() * 1000.0)

        for i in range(1000):
            # We increment the lower_value and upper_value according to increment_loop_variable
            lower_value = increment_loop_variable(self.starting_lower_value, i)
            upper_value = increment_loop_variable(self.starting_upper_value, i)

            # Inside the __init__ for DateRange the rounding up and down should happen
            dr = DateRange(DateRangeBound(lower_value, precision),
                           DateRangeBound(upper_value, precision))

            # We verify that rounded value corresponds with what we would expect
            self.assertEqual(truncate_date(lower_value), dr.lower_bound.milliseconds)
            upper_value = round_up_truncated_upper_value(truncate_date(upper_value))
            self.assertEqual(upper_value, dr.upper_bound.milliseconds)


class TestOrdering(unittest.TestCase):
    def _shuffle_lists(self, *args):
        return [item for sublist in zip(*args) for item in sublist]

    def test_host_order(self):
        """
        Test Host class is ordered consistently

        @since 3.9
        @jira_ticket PYTHON-714
        @expected_result the hosts are ordered correctly

        @test_category data_types
        """
        hosts = [Host(addr, SimpleConvictionPolicy) for addr in
                 ("127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4")]
        hosts_equal = [Host(addr, SimpleConvictionPolicy) for addr in
                       ("127.0.0.1", "127.0.0.1")]
        hosts_equal_conviction = [Host("127.0.0.1", SimpleConvictionPolicy), Host("127.0.0.1", ConvictionPolicy)]
        check_sequence_consistency(self, hosts)
        check_sequence_consistency(self, hosts_equal, equal=True)
        check_sequence_consistency(self, hosts_equal_conviction, equal=True)

    def test_date_order(self):
        """
        Test Date class is ordered consistently

        @since 3.9
        @jira_ticket PYTHON-714
        @expected_result the dates are ordered correctly

        @test_category data_types
        """
        dates_from_string = [Date("2017-01-01"), Date("2017-01-05"), Date("2017-01-09"), Date("2017-01-13")]
        dates_from_string_equal = [Date("2017-01-01"), Date("2017-01-01")]
        check_sequence_consistency(self, dates_from_string)
        check_sequence_consistency(self, dates_from_string_equal, equal=True)

        date_format = "%Y-%m-%d"

        dates_from_value = [
            Date((datetime.datetime.strptime(dtstr, date_format) -
                  datetime.datetime(1970, 1, 1)).days)
            for dtstr in ("2017-01-02", "2017-01-06", "2017-01-10", "2017-01-14")
        ]
        dates_from_value_equal = [Date(1), Date(1)]
        check_sequence_consistency(self, dates_from_value)
        check_sequence_consistency(self, dates_from_value_equal, equal=True)

        dates_from_datetime = [Date(datetime.datetime.strptime(dtstr, date_format))
                               for dtstr in ("2017-01-03", "2017-01-07", "2017-01-11", "2017-01-15")]
        dates_from_datetime_equal = [Date(datetime.datetime.strptime("2017-01-01", date_format)),
                               Date(datetime.datetime.strptime("2017-01-01", date_format))]
        check_sequence_consistency(self, dates_from_datetime)
        check_sequence_consistency(self, dates_from_datetime_equal, equal=True)

        dates_from_date = [
            Date(datetime.datetime.strptime(dtstr, date_format).date()) for dtstr in
            ("2017-01-04", "2017-01-08", "2017-01-12", "2017-01-16")
        ]
        dates_from_date_equal = [datetime.datetime.strptime(dtstr, date_format) for dtstr in
                                 ("2017-01-09", "2017-01-9")]

        check_sequence_consistency(self, dates_from_date)
        check_sequence_consistency(self, dates_from_date_equal, equal=True)

        check_sequence_consistency(self, self._shuffle_lists(dates_from_string, dates_from_value,
                                                             dates_from_datetime, dates_from_date))

    def test_timer_order(self):
        """
        Test Time class is ordered consistently

        @since 3.9
        @jira_ticket PYTHON-714
        @expected_result the times are ordered correctly

        @test_category data_types
        """
        time_from_int = [Time(1000), Time(4000), Time(7000), Time(10000)]
        time_from_int_equal = [Time(1), Time(1)]
        check_sequence_consistency(self, time_from_int)
        check_sequence_consistency(self, time_from_int_equal, equal=True)

        time_from_datetime = [Time(datetime.time(hour=0, minute=0, second=0, microsecond=us))
                              for us in (2, 5, 8, 11)]
        time_from_datetime_equal = [Time(datetime.time(hour=0, minute=0, second=0, microsecond=us))
                                    for us in (1, 1)]
        check_sequence_consistency(self, time_from_datetime)
        check_sequence_consistency(self, time_from_datetime_equal, equal=True)

        time_from_string = [Time("00:00:00.000003000"), Time("00:00:00.000006000"),
                            Time("00:00:00.000009000"), Time("00:00:00.000012000")]
        time_from_string_equal = [Time("00:00:00.000004000"), Time("00:00:00.000004000")]
        check_sequence_consistency(self, time_from_string)
        check_sequence_consistency(self, time_from_string_equal, equal=True)

        check_sequence_consistency(self, self._shuffle_lists(time_from_int, time_from_datetime, time_from_string))

    def test_token_order(self):
        """
        Test Token class is ordered consistently

        @since 3.9
        @jira_ticket PYTHON-714
        @expected_result the tokens are ordered correctly

        @test_category data_types
        """
        tokens = [Token(1), Token(2), Token(3), Token(4)]
        tokens_equal = [Token(1), Token(1)]
        check_sequence_consistency(self, tokens)
        check_sequence_consistency(self, tokens_equal, equal=True)
