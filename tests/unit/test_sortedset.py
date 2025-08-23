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

from cassandra.util import sortedset
from cassandra.cqltypes import EMPTY

from datetime import datetime
from itertools import permutations

class SortedSetTest(unittest.TestCase):
    def test_init(self):
        input = [5, 4, 3, 2, 1, 1, 1]
        expected = sorted(set(input))
        ss = sortedset(input)
        self.assertEqual(len(ss), len(expected))
        self.assertEqual(list(ss), expected)

    def test_repr(self):
        self.assertEqual(repr(sortedset([1, 2, 3, 4])), "SortedSet([1, 2, 3, 4])")

    def test_contains(self):
        input = [5, 4, 3, 2, 1, 1, 1]
        expected = sorted(set(input))
        ss = sortedset(input)

        for i in expected:
            self.assertTrue(i in ss)
            self.assertFalse(i not in ss)

        hi = max(expected)+1
        lo = min(expected)-1

        self.assertFalse(hi in ss)
        self.assertFalse(lo in ss)

    def test_mutable_contents(self):
        ba = bytearray(b'some data here')
        ss = sortedset([ba, ba])
        self.assertEqual(list(ss), [ba])

    def test_clear(self):
        ss = sortedset([1, 2, 3])
        ss.clear()
        self.assertEqual(len(ss), 0)

    def test_equal(self):
        s1 = set([1])
        s12 = set([1, 2])
        ss1 = sortedset(s1)
        ss12 = sortedset(s12)

        self.assertEqual(ss1, s1)
        self.assertEqual(ss12, s12)
        self.assertEqual(ss12, s12)
        self.assertEqual(ss1.__eq__(None), NotImplemented)
        self.assertNotEqual(ss1, ss12)
        self.assertNotEqual(ss12, ss1)
        self.assertNotEqual(ss1, s12)
        self.assertNotEqual(ss12, s1)
        self.assertNotEqual(ss1, EMPTY)

    def test_copy(self):
        class comparable(object):
            def __lt__(self, other):
                return id(self) < id(other)

        o = comparable()
        ss = sortedset([comparable(), o])
        ss2 = ss.copy()
        self.assertNotEqual(id(ss), id(ss2))
        self.assertTrue(o in ss)
        self.assertTrue(o in ss2)

    def test_isdisjoint(self):
        # set, ss
        s12 = set([1, 2])
        s2 = set([2])
        ss1 = sortedset([1])
        ss13 = sortedset([1, 3])
        ss3 = sortedset([3])
        # s ss disjoint
        self.assertTrue(s2.isdisjoint(ss1))
        self.assertTrue(s2.isdisjoint(ss13))
        # s ss not disjoint
        self.assertFalse(s12.isdisjoint(ss1))
        self.assertFalse(s12.isdisjoint(ss13))
        # ss s disjoint
        self.assertTrue(ss1.isdisjoint(s2))
        self.assertTrue(ss13.isdisjoint(s2))
        # ss s not disjoint
        self.assertFalse(ss1.isdisjoint(s12))
        self.assertFalse(ss13.isdisjoint(s12))
        # ss ss disjoint
        self.assertTrue(ss1.isdisjoint(ss3))
        self.assertTrue(ss3.isdisjoint(ss1))
        # ss ss not disjoint
        self.assertFalse(ss1.isdisjoint(ss13))
        self.assertFalse(ss13.isdisjoint(ss1))
        self.assertFalse(ss3.isdisjoint(ss13))
        self.assertFalse(ss13.isdisjoint(ss3))

    def test_issubset(self):
        s12 = set([1, 2])
        ss1 = sortedset([1])
        ss13 = sortedset([1, 3])
        ss3 = sortedset([3])

        self.assertTrue(ss1.issubset(s12))
        self.assertTrue(ss1.issubset(ss13))

        self.assertFalse(ss1.issubset(ss3))
        self.assertFalse(ss13.issubset(ss3))
        self.assertFalse(ss13.issubset(ss1))
        self.assertFalse(ss13.issubset(s12))

    def test_issuperset(self):
        s12 = set([1, 2])
        ss1 = sortedset([1])
        ss13 = sortedset([1, 3])
        ss3 = sortedset([3])

        self.assertTrue(s12.issuperset(ss1))
        self.assertTrue(ss13.issuperset(ss3))
        self.assertTrue(ss13.issuperset(ss13))

        self.assertFalse(s12.issuperset(ss13))
        self.assertFalse(ss1.issuperset(ss3))
        self.assertFalse(ss1.issuperset(ss13))

    def test_union(self):
        s1 = set([1])
        ss12 = sortedset([1, 2])
        ss23 = sortedset([2, 3])

        self.assertEqual(sortedset().union(s1), sortedset([1]))
        self.assertEqual(ss12.union(s1), sortedset([1, 2]))
        self.assertEqual(ss12.union(ss23), sortedset([1, 2, 3]))
        self.assertEqual(ss23.union(ss12), sortedset([1, 2, 3]))
        self.assertEqual(ss23.union(s1), sortedset([1, 2, 3]))

    def test_intersection(self):
        s12 = set([1, 2])
        ss23 = sortedset([2, 3])
        self.assertEqual(s12.intersection(ss23), set([2]))
        self.assertEqual(ss23.intersection(s12), sortedset([2]))
        self.assertEqual(ss23.intersection(s12, [2], (2,)), sortedset([2]))
        self.assertEqual(ss23.intersection(s12, [900], (2,)), sortedset())

    def test_difference(self):
        s1 = set([1])
        ss12 = sortedset([1, 2])
        ss23 = sortedset([2, 3])

        self.assertEqual(sortedset().difference(s1), sortedset())
        self.assertEqual(ss12.difference(s1), sortedset([2]))
        self.assertEqual(ss12.difference(ss23), sortedset([1]))
        self.assertEqual(ss23.difference(ss12), sortedset([3]))
        self.assertEqual(ss23.difference(s1), sortedset([2, 3]))

    def test_symmetric_difference(self):
        s = set([1, 3, 5])
        ss = sortedset([2, 3, 4])
        ss2 = sortedset([5, 6, 7])

        self.assertEqual(ss.symmetric_difference(s), sortedset([1, 2, 4, 5]))
        self.assertFalse(ss.symmetric_difference(ss))
        self.assertEqual(ss.symmetric_difference(s), sortedset([1, 2, 4, 5]))
        self.assertEqual(ss2.symmetric_difference(ss), sortedset([2, 3, 4, 5, 6, 7]))

    def test_pop(self):
        ss = sortedset([2, 1])
        self.assertEqual(ss.pop(), 2)
        self.assertEqual(ss.pop(), 1)
        try:
            ss.pop()
            self.fail("Error not thrown")
        except (KeyError, IndexError) as e:
            pass

    def test_remove(self):
        ss = sortedset([2, 1])
        self.assertEqual(len(ss), 2)
        self.assertRaises(KeyError, ss.remove, 3)
        self.assertEqual(len(ss), 2)
        ss.remove(1)
        self.assertEqual(len(ss), 1)
        ss.remove(2)
        self.assertFalse(ss)
        self.assertRaises(KeyError, ss.remove, 2)
        self.assertFalse(ss)

    def test_getitem(self):
        ss = sortedset(range(3))
        for i in range(len(ss)):
            self.assertEqual(ss[i], i)
        with self.assertRaises(IndexError):
            ss[len(ss)]

    def test_delitem(self):
        expected = [1,2,3,4]
        ss = sortedset(expected)
        for i in range(len(ss)):
            self.assertListEqual(list(ss), expected[i:])
            del ss[0]
        with self.assertRaises(IndexError):
            ss[0]

    def test_delslice(self):
        expected = [1, 2, 3, 4, 5]
        ss = sortedset(expected)
        del ss[1:3]
        self.assertListEqual(list(ss), [1, 4, 5])
        del ss[-1:]
        self.assertListEqual(list(ss), [1, 4])
        del ss[1:]
        self.assertListEqual(list(ss), [1])
        del ss[:]
        self.assertFalse(ss)
        with self.assertRaises(IndexError):
            del ss[0]

    def test_reversed(self):
        expected = range(10)
        self.assertListEqual(list(reversed(sortedset(expected))), list(reversed(expected)))

    def test_operators(self):

        ss1 = sortedset([1])
        ss12 = sortedset([1, 2])
        # __ne__
        self.assertFalse(ss12 != ss12)
        self.assertFalse(ss12 != sortedset([1, 2]))
        self.assertTrue(ss12 != sortedset())

        # __le__
        self.assertTrue(ss1 <= ss12)
        self.assertTrue(ss12 <= ss12)
        self.assertFalse(ss12 <= ss1)

        # __lt__
        self.assertTrue(ss1 < ss12)
        self.assertFalse(ss12 < ss12)
        self.assertFalse(ss12 < ss1)

        # __ge__
        self.assertFalse(ss1 >= ss12)
        self.assertTrue(ss12 >= ss12)
        self.assertTrue(ss12 >= ss1)

        # __gt__
        self.assertFalse(ss1 > ss12)
        self.assertFalse(ss12 > ss12)
        self.assertTrue(ss12 > ss1)

        # __and__
        self.assertEqual(ss1 & ss12, ss1)
        self.assertEqual(ss12 & ss12, ss12)
        self.assertEqual(ss12 & set(), sortedset())

        # __iand__
        tmp = sortedset(ss12)
        tmp &= ss1
        self.assertEqual(tmp, ss1)
        tmp = sortedset(ss1)
        tmp &= ss12
        self.assertEqual(tmp, ss1)
        tmp = sortedset(ss12)
        tmp &= ss12
        self.assertEqual(tmp, ss12)
        tmp = sortedset(ss12)
        tmp &= set()
        self.assertEqual(tmp, sortedset())

        # __rand__
        self.assertEqual(set([1]) & ss12, ss1)

        # __or__
        self.assertEqual(ss1 | ss12, ss12)
        self.assertEqual(ss12 | ss12, ss12)
        self.assertEqual(ss12 | set(), ss12)
        self.assertEqual(sortedset() | ss1 | ss12, ss12)

        # __ior__
        tmp = sortedset(ss1)
        tmp |= ss12
        self.assertEqual(tmp, ss12)
        tmp = sortedset(ss12)
        tmp |= ss12
        self.assertEqual(tmp, ss12)
        tmp = sortedset(ss12)
        tmp |= set()
        self.assertEqual(tmp, ss12)
        tmp = sortedset()
        tmp |= ss1
        tmp |= ss12
        self.assertEqual(tmp, ss12)

        # __ror__
        self.assertEqual(set([1]) | ss12, ss12)

        # __sub__
        self.assertEqual(ss1 - ss12, set())
        self.assertEqual(ss12 - ss12, set())
        self.assertEqual(ss12 - set(), ss12)
        self.assertEqual(ss12 - ss1, sortedset([2]))

        # __isub__
        tmp = sortedset(ss1)
        tmp -= ss12
        self.assertEqual(tmp, set())
        tmp = sortedset(ss12)
        tmp -= ss12
        self.assertEqual(tmp, set())
        tmp = sortedset(ss12)
        tmp -= set()
        self.assertEqual(tmp, ss12)
        tmp = sortedset(ss12)
        tmp -= ss1
        self.assertEqual(tmp, sortedset([2]))

        # __rsub__
        self.assertEqual(set((1,2,3)) - ss12, set((3,)))

        # __xor__
        self.assertEqual(ss1 ^ ss12, set([2]))
        self.assertEqual(ss12 ^ ss1, set([2]))
        self.assertEqual(ss12 ^ ss12, set())
        self.assertEqual(ss12 ^ set(), ss12)

        # __ixor__
        tmp = sortedset(ss1)
        tmp ^= ss12
        self.assertEqual(tmp, set([2]))
        tmp = sortedset(ss12)
        tmp ^= ss1
        self.assertEqual(tmp, set([2]))
        tmp = sortedset(ss12)
        tmp ^= ss12
        self.assertEqual(tmp, set())
        tmp = sortedset(ss12)
        tmp ^= set()
        self.assertEqual(tmp, ss12)

        # __rxor__
        self.assertEqual(set([1, 2]) ^ ss1, (set([2])))

    def test_reduce_pickle(self):
        ss = sortedset((4,3,2,1))
        import pickle
        s = pickle.dumps(ss)
        self.assertEqual(pickle.loads(s), ss)

    def _test_uncomparable_types(self, items):
        for perm in permutations(items):
            ss = sortedset(perm)
            s = set(perm)
            self.assertEqual(s, ss)
            self.assertEqual(ss, ss.union(s))
            for x in range(len(ss)):
                subset = set(s)
                for _ in range(x):
                    subset.pop()
                self.assertEqual(ss.difference(subset), s.difference(subset))
                self.assertEqual(ss.intersection(subset), s.intersection(subset))
            for x in ss:
                self.assertIn(x, ss)
                ss.remove(x)
                self.assertNotIn(x, ss)

    def test_uncomparable_types_with_tuples(self):
        # PYTHON-1087 - make set handle uncomparable types
        dt = datetime(2019, 5, 16)
        items = (('samekey', 3, 1),
                 ('samekey', None, 0),
                 ('samekey', dt),
                 ("samekey", None, 2),
                 ("samekey", None, 1),
                 ('samekey', dt),
                 ('samekey', None, 0),
                 ("samekey", datetime.now()))

        self._test_uncomparable_types(items)

    def test_uncomparable_types_with_integers(self):
        # PYTHON-1087 - make set handle uncomparable types
        items = (None, 1, 2, 6, None, None, 92)
        self._test_uncomparable_types(items)
