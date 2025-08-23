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
from unittest.mock import Mock, PropertyMock, patch

from cassandra.cluster import ResultSet
from cassandra.query import named_tuple_factory, dict_factory, tuple_factory


class ResultSetTests(unittest.TestCase):

    def test_iter_non_paged(self):
        expected = list(range(10))
        rs = ResultSet(Mock(has_more_pages=False), expected)
        itr = iter(rs)
        self.assertListEqual(list(itr), expected)

    def test_iter_paged(self):
        expected = list(range(10))
        response_future = Mock(has_more_pages=True, _continuous_paging_session=None)
        response_future.result.side_effect = (ResultSet(Mock(), expected[-5:]), )  # ResultSet is iterable, so it must be protected in order to be returned whole by the Mock
        rs = ResultSet(response_future, expected[:5])
        itr = iter(rs)
        # this is brittle, depends on internal impl details. Would like to find a better way
        type(response_future).has_more_pages = PropertyMock(side_effect=(True, True, False))  # after init to avoid side effects being consumed by init
        self.assertListEqual(list(itr), expected)

    def test_iter_paged_with_empty_pages(self):
        expected = list(range(10))
        response_future = Mock(has_more_pages=True, _continuous_paging_session=None)
        response_future.result.side_effect = [
            ResultSet(Mock(), []),
            ResultSet(Mock(), [0, 1, 2, 3, 4]),
            ResultSet(Mock(), []),
            ResultSet(Mock(), [5, 6, 7, 8, 9]),
        ]
        rs = ResultSet(response_future, [])
        itr = iter(rs)
        self.assertListEqual(list(itr), expected)

    def test_list_non_paged(self):
        # list access on RS for backwards-compatibility
        expected = list(range(10))
        rs = ResultSet(Mock(has_more_pages=False), expected)
        for i in range(10):
            self.assertEqual(rs[i], expected[i])
        self.assertEqual(list(rs), expected)

    def test_list_paged(self):
        # list access on RS for backwards-compatibility
        expected = list(range(10))
        response_future = Mock(has_more_pages=True, _continuous_paging_session=None)
        response_future.result.side_effect = (ResultSet(Mock(), expected[-5:]), )  # ResultSet is iterable, so it must be protected in order to be returned whole by the Mock
        rs = ResultSet(response_future, expected[:5])
        # this is brittle, depends on internal impl details. Would like to find a better way
        type(response_future).has_more_pages = PropertyMock(side_effect=(True, True, True, False))  # First two True are consumed on check entering list mode
        self.assertEqual(rs[9], expected[9])
        self.assertEqual(list(rs), expected)

    def test_has_more_pages(self):
        response_future = Mock()
        response_future.has_more_pages.side_effect = PropertyMock(side_effect=(True, False))
        rs = ResultSet(response_future, [])
        type(response_future).has_more_pages = PropertyMock(side_effect=(True, False))  # after init to avoid side effects being consumed by init
        self.assertTrue(rs.has_more_pages)
        self.assertFalse(rs.has_more_pages)

    def test_iterate_then_index(self):
        # RuntimeError if indexing with no pages
        expected = list(range(10))
        rs = ResultSet(Mock(has_more_pages=False), expected)
        itr = iter(rs)
        # before consuming
        with self.assertRaises(RuntimeError):
            rs[0]
        list(itr)
        # after consuming
        with self.assertRaises(RuntimeError):
            rs[0]

        self.assertFalse(rs)
        self.assertFalse(list(rs))

        # RuntimeError if indexing during or after pages
        response_future = Mock(has_more_pages=True, _continuous_paging_session=None)
        response_future.result.side_effect = (ResultSet(Mock(), expected[-5:]), )  # ResultSet is iterable, so it must be protected in order to be returned whole by the Mock
        rs = ResultSet(response_future, expected[:5])
        type(response_future).has_more_pages = PropertyMock(side_effect=(True, False))
        itr = iter(rs)
        # before consuming
        with self.assertRaises(RuntimeError):
            rs[0]
        for row in itr:
            # while consuming
            with self.assertRaises(RuntimeError):
                rs[0]
        # after consuming
        with self.assertRaises(RuntimeError):
            rs[0]
        self.assertFalse(rs)
        self.assertFalse(list(rs))

    def test_index_list_mode(self):
        # no pages
        expected = list(range(10))
        rs = ResultSet(Mock(has_more_pages=False), expected)

        # index access before iteration causes list to be materialized
        self.assertEqual(rs[0], expected[0])

        # reusable iteration
        self.assertListEqual(list(rs), expected)
        self.assertListEqual(list(rs), expected)

        self.assertTrue(rs)

        # pages
        response_future = Mock(has_more_pages=True, _continuous_paging_session=None)
        response_future.result.side_effect = (ResultSet(Mock(), expected[-5:]), )  # ResultSet is iterable, so it must be protected in order to be returned whole by the Mock
        rs = ResultSet(response_future, expected[:5])
        # this is brittle, depends on internal impl details. Would like to find a better way
        type(response_future).has_more_pages = PropertyMock(side_effect=(True, True, True, False))  # First two True are consumed on check entering list mode
        # index access before iteration causes list to be materialized
        self.assertEqual(rs[0], expected[0])
        self.assertEqual(rs[9], expected[9])
        # reusable iteration
        self.assertListEqual(list(rs), expected)
        self.assertListEqual(list(rs), expected)

        self.assertTrue(rs)

    def test_eq(self):
        # no pages
        expected = list(range(10))
        rs = ResultSet(Mock(has_more_pages=False), expected)

        # eq before iteration causes list to be materialized
        self.assertEqual(rs, expected)

        # results can be iterated or indexed once we're materialized
        self.assertListEqual(list(rs), expected)
        self.assertEqual(rs[9], expected[9])
        self.assertTrue(rs)

        # pages
        response_future = Mock(has_more_pages=True, _continuous_paging_session=None)
        response_future.result.side_effect = (ResultSet(Mock(), expected[-5:]), )  # ResultSet is iterable, so it must be protected in order to be returned whole by the Mock
        rs = ResultSet(response_future, expected[:5])
        type(response_future).has_more_pages = PropertyMock(side_effect=(True, True, True, False))
        # eq before iteration causes list to be materialized
        self.assertEqual(rs, expected)

        # results can be iterated or indexed once we're materialized
        self.assertListEqual(list(rs), expected)
        self.assertEqual(rs[9], expected[9])
        self.assertTrue(rs)

    def test_bool(self):
        self.assertFalse(ResultSet(Mock(has_more_pages=False), []))
        self.assertTrue(ResultSet(Mock(has_more_pages=False), [1]))

    def test_was_applied(self):
        # unknown row factory raises
        with self.assertRaises(RuntimeError):
            ResultSet(Mock(), []).was_applied

        response_future = Mock(row_factory=named_tuple_factory)

        # no row
        with self.assertRaises(RuntimeError):
            ResultSet(response_future, []).was_applied

        # too many rows
        with self.assertRaises(RuntimeError):
            ResultSet(response_future, [tuple(), tuple()]).was_applied

        # various internal row factories
        for row_factory in (named_tuple_factory, tuple_factory):
            for applied in (True, False):
                rs = ResultSet(Mock(row_factory=row_factory), [(applied,)])
                self.assertEqual(rs.was_applied, applied)

        row_factory = dict_factory
        for applied in (True, False):
            rs = ResultSet(Mock(row_factory=row_factory), [{'[applied]': applied}])
            self.assertEqual(rs.was_applied, applied)

    def test_one(self):
        # no pages
        first, second = Mock(), Mock()
        rs = ResultSet(Mock(has_more_pages=False), [first, second])

        self.assertEqual(rs.one(), first)

    def test_all(self):
        first, second = Mock(), Mock()
        rs1 = ResultSet(Mock(has_more_pages=False), [first, second])
        rs2 = ResultSet(Mock(has_more_pages=False), [first, second])

        self.assertEqual(rs1.all(), list(rs2))

    @patch('cassandra.cluster.warn')
    def test_indexing_deprecation(self, mocked_warn):
        # normally we'd use catch_warnings to test this, but that doesn't work
        # pre-Py3.0 for some reason
        first, second = Mock(), Mock()
        rs = ResultSet(Mock(has_more_pages=False), [first, second])
        self.assertEqual(rs[0], first)
        self.assertEqual(len(mocked_warn.mock_calls), 1)
        index_warning_args = tuple(mocked_warn.mock_calls[0])[1]
        self.assertIn('indexing support will be removed in 4.0',
                      str(index_warning_args[0]))
        self.assertIs(index_warning_args[1], DeprecationWarning)
