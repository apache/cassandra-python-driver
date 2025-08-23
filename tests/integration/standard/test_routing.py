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

from uuid import uuid1

import logging
log = logging.getLogger(__name__)

from tests.integration import use_singledc, TestCluster


def setup_module():
    use_singledc()


class RoutingTests(unittest.TestCase):

    @property
    def cfname(self):
        return self._testMethodName.lower()

    @classmethod
    def setup_class(cls):
        cls.cluster = TestCluster()
        cls.session = cls.cluster.connect('test1rf')

    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()

    def insert_select_token(self, insert, select, key_values):
        s = self.session

        bound = insert.bind(key_values)
        s.execute(bound)

        my_token = s.cluster.metadata.token_map.token_class.from_key(bound.routing_key)

        cass_token = s.execute(select, key_values)[0][0]
        token = s.cluster.metadata.token_map.token_class(cass_token)
        self.assertEqual(my_token, token)

    def create_prepare(self, key_types):
        s = self.session
        table_name = "%s_%s" % (self.cfname, '_'.join(key_types))
        key_count = len(key_types)
        key_decl = ', '.join(['k%d %s' % (i, t) for i, t in enumerate(key_types)])
        primary_key = ', '.join(['k%d' % i for i in range(key_count)])
        s.execute("CREATE TABLE %s (%s, v int, PRIMARY KEY ((%s)))" %
                  (table_name, key_decl, primary_key))

        parameter_places = ', '.join(['?'] * key_count)
        insert = s.prepare("INSERT INTO %s (%s, v) VALUES (%s, 1)" %
                           (table_name, primary_key, parameter_places))

        where_clause = ' AND '.join(['k%d = ?' % i for i in range(key_count)])
        select = s.prepare("SELECT token(%s) FROM %s WHERE %s" %
                           (primary_key, table_name, where_clause))

        return insert, select

    def test_singular_key(self):
        # string
        insert, select = self.create_prepare(('text',))
        self.insert_select_token(insert, select, ('some text value',))

        # non-string
        insert, select = self.create_prepare(('bigint',))
        self.insert_select_token(insert, select, (12390890177098123,))

    def test_composite(self):
        # double bool
        insert, select = self.create_prepare(('double', 'boolean'))
        self.insert_select_token(insert, select, (3.1459, True))
        self.insert_select_token(insert, select, (1.21e9, False))

        # uuid string int
        insert, select = self.create_prepare(('timeuuid', 'varchar', 'int'))
        self.insert_select_token(insert, select, (uuid1(), 'asdf', 400))
        self.insert_select_token(insert, select, (uuid1(), 'fdsa', -1))
