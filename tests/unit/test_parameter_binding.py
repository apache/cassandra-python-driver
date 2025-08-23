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

from cassandra.encoder import Encoder
from cassandra.protocol import ColumnMetadata
from cassandra.query import (bind_params, ValueSequence, PreparedStatement,
                             BoundStatement, UNSET_VALUE)
from cassandra.cqltypes import Int32Type
from cassandra.util import OrderedDict


class ParamBindingTest(unittest.TestCase):

    def test_bind_sequence(self):
        result = bind_params("%s %s %s", (1, "a", 2.0), Encoder())
        self.assertEqual(result, "1 'a' 2.0")

    def test_bind_map(self):
        result = bind_params("%(a)s %(b)s %(c)s", dict(a=1, b="a", c=2.0), Encoder())
        self.assertEqual(result, "1 'a' 2.0")

    def test_sequence_param(self):
        result = bind_params("%s", (ValueSequence((1, "a", 2.0)),), Encoder())
        self.assertEqual(result, "(1, 'a', 2.0)")

    def test_generator_param(self):
        result = bind_params("%s", ((i for i in range(3)),), Encoder())
        self.assertEqual(result, "[0, 1, 2]")

    def test_none_param(self):
        result = bind_params("%s", (None,), Encoder())
        self.assertEqual(result, "NULL")

    def test_list_collection(self):
        result = bind_params("%s", (['a', 'b', 'c'],), Encoder())
        self.assertEqual(result, "['a', 'b', 'c']")

    def test_set_collection(self):
        result = bind_params("%s", (set(['a', 'b']),), Encoder())
        self.assertIn(result, ("{'a', 'b'}", "{'b', 'a'}"))

    def test_map_collection(self):
        vals = OrderedDict()
        vals['a'] = 'a'
        vals['b'] = 'b'
        vals['c'] = 'c'
        result = bind_params("%s", (vals,), Encoder())
        self.assertEqual(result, "{'a': 'a', 'b': 'b', 'c': 'c'}")

    def test_quote_escaping(self):
        result = bind_params("%s", ("""'ef''ef"ef""ef'""",), Encoder())
        self.assertEqual(result, """'''ef''''ef"ef""ef'''""")

    def test_float_precision(self):
        f = 3.4028234663852886e+38
        self.assertEqual(float(bind_params("%s", (f,), Encoder())), f)


class BoundStatementTestV1(unittest.TestCase):

    protocol_version = 1

    @classmethod
    def setUpClass(cls):
        column_metadata = [ColumnMetadata('keyspace', 'cf', 'rk0', Int32Type),
                           ColumnMetadata('keyspace', 'cf', 'rk1', Int32Type),
                           ColumnMetadata('keyspace', 'cf', 'ck0', Int32Type),
                           ColumnMetadata('keyspace', 'cf', 'v0', Int32Type)]
        cls.prepared = PreparedStatement(column_metadata=column_metadata,
                                         query_id=None,
                                         routing_key_indexes=[1, 0],
                                         query=None,
                                         keyspace='keyspace',
                                         protocol_version=cls.protocol_version, result_metadata=None,
                                         result_metadata_id=None)
        cls.bound = BoundStatement(prepared_statement=cls.prepared)

    def test_invalid_argument_type(self):
        values = (0, 0, 0, 'string not int')
        try:
            self.bound.bind(values)
        except TypeError as e:
            self.assertIn('v0', str(e))
            self.assertIn('Int32Type', str(e))
            self.assertIn('str', str(e))
        else:
            self.fail('Passed invalid type but exception was not thrown')

        values = (['1', '2'], 0, 0, 0)

        try:
            self.bound.bind(values)
        except TypeError as e:
            self.assertIn('rk0', str(e))
            self.assertIn('Int32Type', str(e))
            self.assertIn('list', str(e))
        else:
            self.fail('Passed invalid type but exception was not thrown')

    def test_inherit_fetch_size(self):
        keyspace = 'keyspace1'
        column_family = 'cf1'

        column_metadata = [
            ColumnMetadata(keyspace, column_family, 'foo1', Int32Type),
            ColumnMetadata(keyspace, column_family, 'foo2', Int32Type)
        ]

        prepared_statement = PreparedStatement(column_metadata=column_metadata,
                                               query_id=None,
                                               routing_key_indexes=[],
                                               query=None,
                                               keyspace=keyspace,
                                               protocol_version=self.protocol_version,
                                               result_metadata=None,
                                               result_metadata_id=None)
        prepared_statement.fetch_size = 1234
        bound_statement = BoundStatement(prepared_statement=prepared_statement)
        self.assertEqual(1234, bound_statement.fetch_size)

    def test_too_few_parameters_for_routing_key(self):
        self.assertRaises(ValueError, self.prepared.bind, (1,))

        bound = self.prepared.bind((1, 2))
        self.assertEqual(bound.keyspace, 'keyspace')

    def test_dict_missing_routing_key(self):
        self.assertRaises(KeyError, self.bound.bind, {'rk0': 0, 'ck0': 0, 'v0': 0})
        self.assertRaises(KeyError, self.bound.bind, {'rk1': 0, 'ck0': 0, 'v0': 0})

    def test_missing_value(self):
        self.assertRaises(KeyError, self.bound.bind, {'rk0': 0, 'rk1': 0, 'ck0': 0})

    def test_extra_value(self):
        self.bound.bind({'rk0': 0, 'rk1': 0, 'ck0': 0, 'v0': 0, 'should_not_be_here': 123})  # okay to have extra keys in dict
        self.assertEqual(self.bound.values, [b'\x00' * 4] * 4)  # four encoded zeros
        self.assertRaises(ValueError, self.bound.bind, (0, 0, 0, 0, 123))

    def test_values_none(self):
        # should have values
        self.assertRaises(ValueError, self.bound.bind, None)

        # prepared statement with no values
        prepared_statement = PreparedStatement(column_metadata=[],
                                               query_id=None,
                                               routing_key_indexes=[],
                                               query=None,
                                               keyspace='whatever',
                                               protocol_version=self.protocol_version,
                                               result_metadata=None,
                                               result_metadata_id=None)
        bound = prepared_statement.bind(None)
        self.assertListEqual(bound.values, [])

    def test_bind_none(self):
        self.bound.bind({'rk0': 0, 'rk1': 0, 'ck0': 0, 'v0': None})
        self.assertEqual(self.bound.values[-1], None)

        old_values = self.bound.values
        self.bound.bind((0, 0, 0, None))
        self.assertIsNot(self.bound.values, old_values)
        self.assertEqual(self.bound.values[-1], None)

    def test_unset_value(self):
        self.assertRaises(ValueError, self.bound.bind, {'rk0': 0, 'rk1': 0, 'ck0': 0, 'v0': UNSET_VALUE})
        self.assertRaises(ValueError, self.bound.bind, (0, 0, 0, UNSET_VALUE))


class BoundStatementTestV2(BoundStatementTestV1):
    protocol_version = 2


class BoundStatementTestV3(BoundStatementTestV1):
    protocol_version = 3


class BoundStatementTestV4(BoundStatementTestV1):
    protocol_version = 4

    def test_dict_missing_routing_key(self):
        # in v4 it implicitly binds UNSET_VALUE for missing items,
        # UNSET_VALUE is ValueError for routing keys
        self.assertRaises(ValueError, self.bound.bind, {'rk0': 0, 'ck0': 0, 'v0': 0})
        self.assertRaises(ValueError, self.bound.bind, {'rk1': 0, 'ck0': 0, 'v0': 0})

    def test_missing_value(self):
        # in v4 missing values are UNSET_VALUE
        self.bound.bind({'rk0': 0, 'rk1': 0, 'ck0': 0})
        self.assertEqual(self.bound.values[-1], UNSET_VALUE)

        old_values = self.bound.values
        self.bound.bind((0, 0, 0))
        self.assertIsNot(self.bound.values, old_values)
        self.assertEqual(self.bound.values[-1], UNSET_VALUE)

    def test_unset_value(self):
        self.bound.bind({'rk0': 0, 'rk1': 0, 'ck0': 0, 'v0': UNSET_VALUE})
        self.assertEqual(self.bound.values[-1], UNSET_VALUE)

        self.bound.bind((0, 0, 0, UNSET_VALUE))
        self.assertEqual(self.bound.values[-1], UNSET_VALUE)


class BoundStatementTestV5(BoundStatementTestV4):
    protocol_version = 5
