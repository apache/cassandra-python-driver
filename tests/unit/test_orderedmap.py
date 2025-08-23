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

from cassandra.util import OrderedMap, OrderedMapSerializedKey
from cassandra.cqltypes import EMPTY, UTF8Type, lookup_casstype

class OrderedMapTest(unittest.TestCase):
    def test_init(self):
        a = OrderedMap(zip(['one', 'three', 'two'], [1, 3, 2]))
        b = OrderedMap([('one', 1), ('three', 3), ('two', 2)])
        c = OrderedMap(a)
        builtin = {'one': 1, 'two': 2, 'three': 3}
        self.assertEqual(a, b)
        self.assertEqual(a, c)
        self.assertEqual(a, builtin)
        self.assertEqual(OrderedMap([(1, 1), (1, 2)]), {1: 2})

        d = OrderedMap({'': 3}, key1='v1', key2='v2')
        self.assertEqual(d[''], 3)
        self.assertEqual(d['key1'], 'v1')
        self.assertEqual(d['key2'], 'v2')

        with self.assertRaises(TypeError):
            OrderedMap('too', 'many', 'args')

    def test_contains(self):
        keys = ['first', 'middle', 'last']

        om = OrderedMap()

        om = OrderedMap(zip(keys, range(len(keys))))

        for k in keys:
            self.assertTrue(k in om)
            self.assertFalse(k not in om)

        self.assertTrue('notthere' not in om)
        self.assertFalse('notthere' in om)

    def test_keys(self):
        keys = ['first', 'middle', 'last']
        om = OrderedMap(zip(keys, range(len(keys))))

        self.assertListEqual(list(om.keys()), keys)

    def test_values(self):
        keys = ['first', 'middle', 'last']
        values = list(range(len(keys)))
        om = OrderedMap(zip(keys, values))

        self.assertListEqual(list(om.values()), values)

    def test_items(self):
        keys = ['first', 'middle', 'last']
        items = list(zip(keys, range(len(keys))))
        om = OrderedMap(items)

        self.assertListEqual(list(om.items()), items)

    def test_get(self):
        keys = ['first', 'middle', 'last']
        om = OrderedMap(zip(keys, range(len(keys))))

        for v, k in enumerate(keys):
            self.assertEqual(om.get(k), v)
        
        self.assertEqual(om.get('notthere', 'default'), 'default')
        self.assertIsNone(om.get('notthere'))

    def test_equal(self):
        d1 = {'one': 1}
        d12 = {'one': 1, 'two': 2}
        om1 = OrderedMap({'one': 1})
        om12 = OrderedMap([('one', 1), ('two', 2)])
        om21 = OrderedMap([('two', 2), ('one', 1)])

        self.assertEqual(om1, d1)
        self.assertEqual(om12, d12)
        self.assertEqual(om21, d12)
        self.assertNotEqual(om1, om12)
        self.assertNotEqual(om12, om1)
        self.assertNotEqual(om12, om21)
        self.assertNotEqual(om1, d12)
        self.assertNotEqual(om12, d1)
        self.assertNotEqual(om1, EMPTY)

        self.assertFalse(OrderedMap([('three', 3), ('four', 4)]) == d12)

    def test_getitem(self):
        keys = ['first', 'middle', 'last']
        om = OrderedMap(zip(keys, range(len(keys))))

        for v, k in enumerate(keys):
            self.assertEqual(om[k], v)
        
        with self.assertRaises(KeyError):
            om['notthere']

    def test_iter(self):
        keys = ['first', 'middle', 'last']
        values = list(range(len(keys)))
        items = list(zip(keys, values))
        om = OrderedMap(items)

        itr = iter(om)
        self.assertEqual(sum([1 for _ in itr]), len(keys))
        self.assertRaises(StopIteration, next, itr)

        self.assertEqual(list(iter(om)), keys)
        self.assertEqual(list(om.items()), items)
        self.assertEqual(list(om.values()), values)

    def test_len(self):
        self.assertEqual(len(OrderedMap()), 0)
        self.assertEqual(len(OrderedMap([(1, 1)])), 1)

    def test_mutable_keys(self):
        d = {'1': 1}
        s = set([1, 2, 3])
        om = OrderedMap([(d, 'dict'), (s, 'set')])

    def test_strings(self):
        # changes in 3.x
        d = {'map': 'inner'}
        s = set([1, 2, 3])
        self.assertEqual(repr(OrderedMap([('two', 2), ('one', 1), (d, 'value'), (s, 'another')])),
                         "OrderedMap([('two', 2), ('one', 1), (%r, 'value'), (%r, 'another')])" % (d, s))

        self.assertEqual(str(OrderedMap([('two', 2), ('one', 1), (d, 'value'), (s, 'another')])),
                         "{'two': 2, 'one': 1, %r: 'value', %r: 'another'}" % (d, s))

    def test_popitem(self):
        item = (1, 2)
        om = OrderedMap((item,))
        self.assertEqual(om.popitem(), item)
        self.assertRaises(KeyError, om.popitem)

    def test_delitem(self):
        om = OrderedMap({1: 1, 2: 2})

        self.assertRaises(KeyError, om.__delitem__, 3)

        del om[1]
        self.assertEqual(om, {2: 2})
        del om[2]
        self.assertFalse(om)

        self.assertRaises(KeyError, om.__delitem__, 1)


class OrderedMapSerializedKeyTest(unittest.TestCase):
    def test_init(self):
        om = OrderedMapSerializedKey(UTF8Type, 2)
        self.assertEqual(om, {})

    def test_normalized_lookup(self):
        key_type = lookup_casstype('MapType(UTF8Type, Int32Type)')
        protocol_version = 3
        om = OrderedMapSerializedKey(key_type, protocol_version)
        key_ascii = {'one': 1}
        key_unicode = {u'two': 2}
        om._insert_unchecked(key_ascii, key_type.serialize(key_ascii, protocol_version), object())
        om._insert_unchecked(key_unicode, key_type.serialize(key_unicode, protocol_version), object())

        # type lookup is normalized by key_type
        # PYTHON-231
        self.assertIs(om[{'one': 1}], om[{u'one': 1}])
        self.assertIs(om[{'two': 2}], om[{u'two': 2}])
        self.assertIsNot(om[{'one': 1}], om[{'two': 2}])
