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

from cassandra.cqlengine.columns import Column, Set, List, Text
from cassandra.cqlengine.operators import *
from cassandra.cqlengine.statements import (UpdateStatement, WhereClause,
                                  AssignmentClause, SetUpdateClause,
                                  ListUpdateClause)


class UpdateStatementTests(unittest.TestCase):

    def test_table_rendering(self):
        """ tests that fields are properly added to the select statement """
        us = UpdateStatement('table')
        self.assertTrue(str(us).startswith('UPDATE table SET'), str(us))
        self.assertTrue(str(us).startswith('UPDATE table SET'), str(us))

    def test_rendering(self):
        us = UpdateStatement('table')
        us.add_assignment(Column(db_field='a'), 'b')
        us.add_assignment(Column(db_field='c'), 'd')
        us.add_where(Column(db_field='a'), EqualsOperator(), 'x')
        self.assertEqual(str(us), 'UPDATE table SET "a" = %(0)s, "c" = %(1)s WHERE "a" = %(2)s', str(us))

        us.add_where(Column(db_field='a'), NotEqualsOperator(), 'y')
        self.assertEqual(str(us), 'UPDATE table SET "a" = %(0)s, "c" = %(1)s WHERE "a" = %(2)s AND "a" != %(3)s', str(us))

    def test_context(self):
        us = UpdateStatement('table')
        us.add_assignment(Column(db_field='a'), 'b')
        us.add_assignment(Column(db_field='c'), 'd')
        us.add_where(Column(db_field='a'), EqualsOperator(), 'x')
        self.assertEqual(us.get_context(), {'0': 'b', '1': 'd', '2': 'x'})

    def test_context_update(self):
        us = UpdateStatement('table')
        us.add_assignment(Column(db_field='a'), 'b')
        us.add_assignment(Column(db_field='c'), 'd')
        us.add_where(Column(db_field='a'), EqualsOperator(), 'x')
        us.update_context_id(3)
        self.assertEqual(str(us), 'UPDATE table SET "a" = %(4)s, "c" = %(5)s WHERE "a" = %(3)s')
        self.assertEqual(us.get_context(), {'4': 'b', '5': 'd', '3': 'x'})

    def test_additional_rendering(self):
        us = UpdateStatement('table', ttl=60)
        us.add_assignment(Column(db_field='a'), 'b')
        us.add_where(Column(db_field='a'), EqualsOperator(), 'x')
        self.assertIn('USING TTL 60', str(us))

    def test_update_set_add(self):
        us = UpdateStatement('table')
        us.add_update(Set(Text, db_field='a'), set((1,)), 'add')
        self.assertEqual(str(us), 'UPDATE table SET "a" = "a" + %(0)s')

    def test_update_empty_set_add_does_not_assign(self):
        us = UpdateStatement('table')
        us.add_update(Set(Text, db_field='a'), set(), 'add')
        self.assertFalse(us.assignments)

    def test_update_empty_set_removal_does_not_assign(self):
        us = UpdateStatement('table')
        us.add_update(Set(Text, db_field='a'), set(), 'remove')
        self.assertFalse(us.assignments)

    def test_update_list_prepend_with_empty_list(self):
        us = UpdateStatement('table')
        us.add_update(List(Text, db_field='a'), [], 'prepend')
        self.assertFalse(us.assignments)

    def test_update_list_append_with_empty_list(self):
        us = UpdateStatement('table')
        us.add_update(List(Text, db_field='a'), [], 'append')
        self.assertFalse(us.assignments)
