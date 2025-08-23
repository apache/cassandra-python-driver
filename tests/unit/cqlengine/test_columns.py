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

from cassandra.cqlengine.columns import Column


class ColumnTest(unittest.TestCase):

    def test_comparisons(self):
        c0 = Column()
        c1 = Column()
        self.assertEqual(c1.position - c0.position, 1)

        # __ne__
        self.assertNotEqual(c0, c1)
        self.assertNotEqual(c0, object())

        # __eq__
        self.assertEqual(c0, c0)
        self.assertFalse(c0 == object())

        # __lt__
        self.assertLess(c0, c1)
        try:
            c0 < object()  # this raises for Python 3
        except TypeError:
            pass

        # __le__
        self.assertLessEqual(c0, c1)
        self.assertLessEqual(c0, c0)
        try:
            c0 <= object()  # this raises for Python 3
        except TypeError:
            pass

        # __gt__
        self.assertGreater(c1, c0)
        try:
            c1 > object()  # this raises for Python 3
        except TypeError:
            pass

        # __ge__
        self.assertGreaterEqual(c1, c0)
        self.assertGreaterEqual(c1, c1)
        try:
            c1 >= object()  # this raises for Python 3
        except TypeError:
            pass

    def test_hash(self):
        c0 = Column()
        self.assertEqual(id(c0), c0.__hash__())

