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


from cassandra.query import named_tuple_factory

import logging
import warnings

import sys

from unittest import TestCase


log = logging.getLogger(__name__)


NAMEDTUPLE_CREATION_BUG = sys.version_info >= (3,) and sys.version_info < (3, 7)

class TestNamedTupleFactory(TestCase):

    long_colnames, long_rows = (
        ['col{}'.format(x) for x in range(300)],
        [
            ['value{}'.format(x) for x in range(300)]
            for _ in range(100)
        ]
    )
    short_colnames, short_rows = (
        ['col{}'.format(x) for x in range(200)],
        [
            ['value{}'.format(x) for x in range(200)]
            for _ in range(100)
        ]
    )

    def test_creation_warning_on_long_column_list(self):
        """
        Reproduces the failure described in PYTHON-893

        @since 3.15
        @jira_ticket PYTHON-893
        @expected_result creation fails on Python > 3 and < 3.7

        @test_category row_factory
        """
        if not NAMEDTUPLE_CREATION_BUG:
            named_tuple_factory(self.long_colnames, self.long_rows)
            return

        with warnings.catch_warnings(record=True) as w:
            rows = named_tuple_factory(self.long_colnames, self.long_rows)
        self.assertEqual(len(w), 1)
        warning = w[0]
        self.assertIn('pseudo_namedtuple_factory', str(warning))
        self.assertIn('3.7', str(warning))

        for r in rows:
            self.assertEqual(r.col0, self.long_rows[0][0])

    def test_creation_no_warning_on_short_column_list(self):
        """
        Tests that normal namedtuple row creation still works after PYTHON-893 fix

        @since 3.15
        @jira_ticket PYTHON-893
        @expected_result creates namedtuple-based Rows

        @test_category row_factory
        """
        with warnings.catch_warnings(record=True) as w:
            rows = named_tuple_factory(self.short_colnames, self.short_rows)
        self.assertEqual(len(w), 0)
        # check that this is a real namedtuple
        self.assertTrue(hasattr(rows[0], '_fields'))
        self.assertIsInstance(rows[0], tuple)
