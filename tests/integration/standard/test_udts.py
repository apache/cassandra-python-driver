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

from collections import namedtuple
from functools import partial

from cassandra import InvalidRequest
from cassandra.cluster import UserTypeDoesNotExist, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.query import dict_factory
from cassandra.util import OrderedMap

from tests.integration import use_singledc, execute_until_pass, \
    BasicSegregatedKeyspaceUnitTestCase, greaterthancass20, lessthancass30, greaterthanorequalcass36, TestCluster
from tests.integration.datatype_utils import update_datatypes, PRIMITIVE_DATATYPES, PRIMITIVE_DATATYPES_KEYS, \
    COLLECTION_TYPES, get_sample, get_collection_sample

nested_collection_udt = namedtuple('nested_collection_udt', ['m', 't', 'l', 's'])
nested_collection_udt_nested = namedtuple('nested_collection_udt_nested', ['m', 't', 'l', 's', 'u'])


def setup_module():
    use_singledc()
    update_datatypes()


@greaterthancass20
class UDTTests(BasicSegregatedKeyspaceUnitTestCase):

    @property
    def table_name(self):
        return self._testMethodName.lower()

    def setUp(self):
        super(UDTTests, self).setUp()
        self.session.set_keyspace(self.keyspace_name)

    @greaterthanorequalcass36
    def test_non_frozen_udts(self):
        """
        Test to ensure that non frozen udt's work with C* >3.6.

        @since 3.7.0
        @jira_ticket PYTHON-498
        @expected_result Non frozen UDT's are supported

        @test_category data_types, udt
        """
        self.session.execute("USE {0}".format(self.keyspace_name))
        self.session.execute("CREATE TYPE user (state text, has_corn boolean)")
        self.session.execute("CREATE TABLE {0} (a int PRIMARY KEY, b user)".format(self.function_table_name))
        User = namedtuple('user', ('state', 'has_corn'))
        self.cluster.register_user_type(self.keyspace_name, "user", User)
        self.session.execute("INSERT INTO {0} (a, b) VALUES (%s, %s)".format(self.function_table_name), (0, User("Nebraska", True)))
        self.session.execute("UPDATE {0} SET b.has_corn = False where a = 0".format(self.function_table_name))
        result = self.session.execute("SELECT * FROM {0}".format(self.function_table_name))
        self.assertFalse(result[0].b.has_corn)
        table_sql = self.cluster.metadata.keyspaces[self.keyspace_name].tables[self.function_table_name].as_cql_query()
        self.assertNotIn("<frozen>", table_sql)

    def test_can_insert_unprepared_registered_udts(self):
        """
        Test the insertion of unprepared, registered UDTs
        """

        c = TestCluster()
        s = c.connect(self.keyspace_name, wait_for_all_pools=True)

        s.execute("CREATE TYPE user (age int, name text)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        User = namedtuple('user', ('age', 'name'))
        c.register_user_type(self.keyspace_name, "user", User)

        s.execute("INSERT INTO mytable (a, b) VALUES (%s, %s)", (0, User(42, 'bob')))
        result = s.execute("SELECT b FROM mytable WHERE a=0")
        row = result[0]
        self.assertEqual(42, row.b.age)
        self.assertEqual('bob', row.b.name)
        self.assertTrue(type(row.b) is User)

        # use the same UDT name in a different keyspace
        s.execute("""
            CREATE KEYSPACE udt_test_unprepared_registered2
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("udt_test_unprepared_registered2")
        s.execute("CREATE TYPE user (state text, is_cool boolean)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        User = namedtuple('user', ('state', 'is_cool'))
        c.register_user_type("udt_test_unprepared_registered2", "user", User)

        s.execute("INSERT INTO mytable (a, b) VALUES (%s, %s)", (0, User('Texas', True)))
        result = s.execute("SELECT b FROM mytable WHERE a=0")
        row = result[0]
        self.assertEqual('Texas', row.b.state)
        self.assertEqual(True, row.b.is_cool)
        self.assertTrue(type(row.b) is User)

        s.execute("DROP KEYSPACE udt_test_unprepared_registered2")

        c.shutdown()

    def test_can_register_udt_before_connecting(self):
        """
        Test the registration of UDTs before session creation
        """

        c = TestCluster()
        s = c.connect(wait_for_all_pools=True)

        s.execute("""
            CREATE KEYSPACE udt_test_register_before_connecting
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("udt_test_register_before_connecting")
        s.execute("CREATE TYPE user (age int, name text)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        s.execute("""
            CREATE KEYSPACE udt_test_register_before_connecting2
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("udt_test_register_before_connecting2")
        s.execute("CREATE TYPE user (state text, is_cool boolean)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        # now that types are defined, shutdown and re-create Cluster
        c.shutdown()
        c = TestCluster()

        User1 = namedtuple('user', ('age', 'name'))
        User2 = namedtuple('user', ('state', 'is_cool'))

        c.register_user_type("udt_test_register_before_connecting", "user", User1)
        c.register_user_type("udt_test_register_before_connecting2", "user", User2)

        s = c.connect(wait_for_all_pools=True)

        s.set_keyspace("udt_test_register_before_connecting")
        s.execute("INSERT INTO mytable (a, b) VALUES (%s, %s)", (0, User1(42, 'bob')))
        result = s.execute("SELECT b FROM mytable WHERE a=0")
        row = result[0]
        self.assertEqual(42, row.b.age)
        self.assertEqual('bob', row.b.name)
        self.assertTrue(type(row.b) is User1)

        # use the same UDT name in a different keyspace
        s.set_keyspace("udt_test_register_before_connecting2")
        s.execute("INSERT INTO mytable (a, b) VALUES (%s, %s)", (0, User2('Texas', True)))
        result = s.execute("SELECT b FROM mytable WHERE a=0")
        row = result[0]
        self.assertEqual('Texas', row.b.state)
        self.assertEqual(True, row.b.is_cool)
        self.assertTrue(type(row.b) is User2)

        s.execute("DROP KEYSPACE udt_test_register_before_connecting")
        s.execute("DROP KEYSPACE udt_test_register_before_connecting2")

        c.shutdown()

    def test_can_insert_prepared_unregistered_udts(self):
        """
        Test the insertion of prepared, unregistered UDTs
        """

        c = TestCluster()
        s = c.connect(self.keyspace_name, wait_for_all_pools=True)

        s.execute("CREATE TYPE user (age int, name text)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        User = namedtuple('user', ('age', 'name'))
        insert = s.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        s.execute(insert, (0, User(42, 'bob')))

        select = s.prepare("SELECT b FROM mytable WHERE a=?")
        result = s.execute(select, (0,))
        row = result[0]
        self.assertEqual(42, row.b.age)
        self.assertEqual('bob', row.b.name)

        # use the same UDT name in a different keyspace
        s.execute("""
            CREATE KEYSPACE udt_test_prepared_unregistered2
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("udt_test_prepared_unregistered2")
        s.execute("CREATE TYPE user (state text, is_cool boolean)")
        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        User = namedtuple('user', ('state', 'is_cool'))
        insert = s.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        s.execute(insert, (0, User('Texas', True)))

        select = s.prepare("SELECT b FROM mytable WHERE a=?")
        result = s.execute(select, (0,))
        row = result[0]
        self.assertEqual('Texas', row.b.state)
        self.assertEqual(True, row.b.is_cool)

        s.execute("DROP KEYSPACE udt_test_prepared_unregistered2")

        c.shutdown()

    def test_can_insert_prepared_registered_udts(self):
        """
        Test the insertion of prepared, registered UDTs
        """

        c = TestCluster()
        s = c.connect(self.keyspace_name, wait_for_all_pools=True)

        s.execute("CREATE TYPE user (age int, name text)")
        User = namedtuple('user', ('age', 'name'))
        c.register_user_type(self.keyspace_name, "user", User)

        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        insert = s.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        s.execute(insert, (0, User(42, 'bob')))

        select = s.prepare("SELECT b FROM mytable WHERE a=?")
        result = s.execute(select, (0,))
        row = result[0]
        self.assertEqual(42, row.b.age)
        self.assertEqual('bob', row.b.name)
        self.assertTrue(type(row.b) is User)

        # use the same UDT name in a different keyspace
        s.execute("""
            CREATE KEYSPACE udt_test_prepared_registered2
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        s.set_keyspace("udt_test_prepared_registered2")
        s.execute("CREATE TYPE user (state text, is_cool boolean)")
        User = namedtuple('user', ('state', 'is_cool'))
        c.register_user_type("udt_test_prepared_registered2", "user", User)

        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        insert = s.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        s.execute(insert, (0, User('Texas', True)))

        select = s.prepare("SELECT b FROM mytable WHERE a=?")
        result = s.execute(select, (0,))
        row = result[0]
        self.assertEqual('Texas', row.b.state)
        self.assertEqual(True, row.b.is_cool)
        self.assertTrue(type(row.b) is User)

        s.execute("DROP KEYSPACE udt_test_prepared_registered2")

        c.shutdown()

    def test_can_insert_udts_with_nulls(self):
        """
        Test the insertion of UDTs with null and empty string fields
        """

        c = TestCluster()
        s = c.connect(self.keyspace_name, wait_for_all_pools=True)

        s.execute("CREATE TYPE user (a text, b int, c uuid, d blob)")
        User = namedtuple('user', ('a', 'b', 'c', 'd'))
        c.register_user_type(self.keyspace_name, "user", User)

        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        insert = s.prepare("INSERT INTO mytable (a, b) VALUES (0, ?)")
        s.execute(insert, [User(None, None, None, None)])

        results = s.execute("SELECT b FROM mytable WHERE a=0")
        self.assertEqual((None, None, None, None), results[0].b)

        select = s.prepare("SELECT b FROM mytable WHERE a=0")
        self.assertEqual((None, None, None, None), s.execute(select)[0].b)

        # also test empty strings
        s.execute(insert, [User('', None, None, bytes())])
        results = s.execute("SELECT b FROM mytable WHERE a=0")
        self.assertEqual(('', None, None, bytes()), results[0].b)

        c.shutdown()

    def test_can_insert_udts_with_varying_lengths(self):
        """
        Test for ensuring extra-lengthy udts are properly inserted
        """

        c = TestCluster()
        s = c.connect(self.keyspace_name, wait_for_all_pools=True)

        max_test_length = 254

        # create the seed udt, increase timeout to avoid the query failure on slow systems
        s.execute("CREATE TYPE lengthy_udt ({0})"
                  .format(', '.join(['v_{0} int'.format(i)
                                    for i in range(max_test_length)])))

        # create a table with multiple sizes of nested udts
        # no need for all nested types, only a spot checked few and the largest one
        s.execute("CREATE TABLE mytable ("
                  "k int PRIMARY KEY, "
                  "v frozen<lengthy_udt>)")

        # create and register the seed udt type
        udt = namedtuple('lengthy_udt', tuple(['v_{0}'.format(i) for i in range(max_test_length)]))
        c.register_user_type(self.keyspace_name, "lengthy_udt", udt)

        # verify inserts and reads
        for i in (0, 1, 2, 3, max_test_length):
            # create udt
            params = [j for j in range(i)] + [None for j in range(max_test_length - i)]
            created_udt = udt(*params)

            # write udt
            s.execute("INSERT INTO mytable (k, v) VALUES (0, %s)", (created_udt,))

            # verify udt was written and read correctly, increase timeout to avoid the query failure on slow systems
            result = s.execute("SELECT v FROM mytable WHERE k=0")[0]
            self.assertEqual(created_udt, result.v)

        c.shutdown()

    def nested_udt_schema_helper(self, session, max_nesting_depth):
        # create the seed udt
        execute_until_pass(session, "CREATE TYPE depth_0 (age int, name text)")

        # create the nested udts
        for i in range(max_nesting_depth):
            execute_until_pass(session, "CREATE TYPE depth_{0} (value frozen<depth_{1}>)".format(i + 1, i))

        # create a table with multiple sizes of nested udts
        # no need for all nested types, only a spot checked few and the largest one
        execute_until_pass(session, "CREATE TABLE mytable ("
                                    "k int PRIMARY KEY, "
                                    "v_0 frozen<depth_0>, "
                                    "v_1 frozen<depth_1>, "
                                    "v_2 frozen<depth_2>, "
                                    "v_3 frozen<depth_3>, "
                                    "v_{0} frozen<depth_{0}>)".format(max_nesting_depth))

    def nested_udt_creation_helper(self, udts, i):
        if i == 0:
            return udts[0](42, 'Bob')
        else:
            return udts[i](self.nested_udt_creation_helper(udts, i - 1))

    def nested_udt_verification_helper(self, session, max_nesting_depth, udts):
        for i in (0, 1, 2, 3, max_nesting_depth):
            # create udt
            udt = self.nested_udt_creation_helper(udts, i)

            # write udt via simple statement
            session.execute("INSERT INTO mytable (k, v_%s) VALUES (0, %s)", [i, udt])

            # verify udt was written and read correctly
            result = session.execute("SELECT v_{0} FROM mytable WHERE k=0".format(i))[0]
            self.assertEqual(udt, result["v_{0}".format(i)])

            # write udt via prepared statement
            insert = session.prepare("INSERT INTO mytable (k, v_{0}) VALUES (1, ?)".format(i))
            session.execute(insert, [udt])

            # verify udt was written and read correctly
            result = session.execute("SELECT v_{0} FROM mytable WHERE k=1".format(i))[0]
            self.assertEqual(udt, result["v_{0}".format(i)])

    def _cluster_default_dict_factory(self):
        return TestCluster(
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(row_factory=dict_factory)}
        )

    def test_can_insert_nested_registered_udts(self):
        """
        Test for ensuring nested registered udts are properly inserted
        """
        with self._cluster_default_dict_factory() as c:
            s = c.connect(self.keyspace_name, wait_for_all_pools=True)

            max_nesting_depth = 16

            # create the schema
            self.nested_udt_schema_helper(s, max_nesting_depth)

            # create and register the seed udt type
            udts = []
            udt = namedtuple('depth_0', ('age', 'name'))
            udts.append(udt)
            c.register_user_type(self.keyspace_name, "depth_0", udts[0])

            # create and register the nested udt types
            for i in range(max_nesting_depth):
                udt = namedtuple('depth_{0}'.format(i + 1), ('value'))
                udts.append(udt)
                c.register_user_type(self.keyspace_name, "depth_{0}".format(i + 1), udts[i + 1])

            # insert udts and verify inserts with reads
            self.nested_udt_verification_helper(s, max_nesting_depth, udts)

    def test_can_insert_nested_unregistered_udts(self):
        """
        Test for ensuring nested unregistered udts are properly inserted
        """

        with self._cluster_default_dict_factory() as c:
            s = c.connect(self.keyspace_name, wait_for_all_pools=True)

            max_nesting_depth = 16

            # create the schema
            self.nested_udt_schema_helper(s, max_nesting_depth)

            # create the seed udt type
            udts = []
            udt = namedtuple('depth_0', ('age', 'name'))
            udts.append(udt)

            # create the nested udt types
            for i in range(max_nesting_depth):
                udt = namedtuple('depth_{0}'.format(i + 1), ('value'))
                udts.append(udt)

            # insert udts via prepared statements and verify inserts with reads
            for i in (0, 1, 2, 3, max_nesting_depth):
                # create udt
                udt = self.nested_udt_creation_helper(udts, i)

                # write udt
                insert = s.prepare("INSERT INTO mytable (k, v_{0}) VALUES (0, ?)".format(i))
                s.execute(insert, [udt])

                # verify udt was written and read correctly
                result = s.execute("SELECT v_{0} FROM mytable WHERE k=0".format(i))[0]
                self.assertEqual(udt, result["v_{0}".format(i)])

    def test_can_insert_nested_registered_udts_with_different_namedtuples(self):
        """
        Test for ensuring nested udts are inserted correctly when the
        created namedtuples are use names that are different the cql type.
        """

        with self._cluster_default_dict_factory() as c:
            s = c.connect(self.keyspace_name, wait_for_all_pools=True)

            max_nesting_depth = 16

            # create the schema
            self.nested_udt_schema_helper(s, max_nesting_depth)

            # create and register the seed udt type
            udts = []
            udt = namedtuple('level_0', ('age', 'name'))
            udts.append(udt)
            c.register_user_type(self.keyspace_name, "depth_0", udts[0])

            # create and register the nested udt types
            for i in range(max_nesting_depth):
                udt = namedtuple('level_{0}'.format(i + 1), ('value'))
                udts.append(udt)
                c.register_user_type(self.keyspace_name, "depth_{0}".format(i + 1), udts[i + 1])

            # insert udts and verify inserts with reads
            self.nested_udt_verification_helper(s, max_nesting_depth, udts)

    def test_raise_error_on_nonexisting_udts(self):
        """
        Test for ensuring that an error is raised for operating on a nonexisting udt or an invalid keyspace
        """

        c = TestCluster()
        s = c.connect(self.keyspace_name, wait_for_all_pools=True)
        User = namedtuple('user', ('age', 'name'))

        with self.assertRaises(UserTypeDoesNotExist):
            c.register_user_type("some_bad_keyspace", "user", User)

        with self.assertRaises(UserTypeDoesNotExist):
            c.register_user_type("system", "user", User)

        with self.assertRaises(InvalidRequest):
            s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")

        c.shutdown()

    def test_can_insert_udt_all_datatypes(self):
        """
        Test for inserting various types of PRIMITIVE_DATATYPES into UDT's
        """

        c = TestCluster()
        s = c.connect(self.keyspace_name, wait_for_all_pools=True)

        # create UDT
        alpha_type_list = []
        start_index = ord('a')
        for i, datatype in enumerate(PRIMITIVE_DATATYPES):
            alpha_type_list.append("{0} {1}".format(chr(start_index + i), datatype))

        s.execute("""
            CREATE TYPE alldatatypes ({0})
        """.format(', '.join(alpha_type_list))
                  )

        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<alldatatypes>)")

        # register UDT
        alphabet_list = []
        for i in range(ord('a'), ord('a') + len(PRIMITIVE_DATATYPES)):
            alphabet_list.append('{0}'.format(chr(i)))
        Alldatatypes = namedtuple("alldatatypes", alphabet_list)
        c.register_user_type(self.keyspace_name, "alldatatypes", Alldatatypes)

        # insert UDT data
        params = []
        for datatype in PRIMITIVE_DATATYPES:
            params.append((get_sample(datatype)))

        insert = s.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        s.execute(insert, (0, Alldatatypes(*params)))

        # retrieve and verify data
        results = s.execute("SELECT * FROM mytable")

        row = results[0].b
        for expected, actual in zip(params, row):
            self.assertEqual(expected, actual)

        c.shutdown()

    def test_can_insert_udt_all_collection_datatypes(self):
        """
        Test for inserting various types of COLLECTION_TYPES into UDT's
        """

        c = TestCluster()
        s = c.connect(self.keyspace_name, wait_for_all_pools=True)

        # create UDT
        alpha_type_list = []
        start_index = ord('a')
        for i, collection_type in enumerate(COLLECTION_TYPES):
            for j, datatype in enumerate(PRIMITIVE_DATATYPES_KEYS):
                if collection_type == "map":
                    type_string = "{0}_{1} {2}<{3}, {3}>".format(chr(start_index + i), chr(start_index + j),
                                                                 collection_type, datatype)
                elif collection_type == "tuple":
                    type_string = "{0}_{1} frozen<{2}<{3}>>".format(chr(start_index + i), chr(start_index + j),
                                                            collection_type, datatype)
                else:
                    type_string = "{0}_{1} {2}<{3}>".format(chr(start_index + i), chr(start_index + j),
                                                            collection_type, datatype)
                alpha_type_list.append(type_string)

        s.execute("""
            CREATE TYPE alldatatypes ({0})
        """.format(', '.join(alpha_type_list))
        )

        s.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<alldatatypes>)")

        # register UDT
        alphabet_list = []
        for i in range(ord('a'), ord('a') + len(COLLECTION_TYPES)):
            for j in range(ord('a'), ord('a') + len(PRIMITIVE_DATATYPES_KEYS)):
                alphabet_list.append('{0}_{1}'.format(chr(i), chr(j)))

        Alldatatypes = namedtuple("alldatatypes", alphabet_list)
        c.register_user_type(self.keyspace_name, "alldatatypes", Alldatatypes)

        # insert UDT data
        params = []
        for collection_type in COLLECTION_TYPES:
            for datatype in PRIMITIVE_DATATYPES_KEYS:
                params.append((get_collection_sample(collection_type, datatype)))

        insert = s.prepare("INSERT INTO mytable (a, b) VALUES (?, ?)")
        s.execute(insert, (0, Alldatatypes(*params)))

        # retrieve and verify data
        results = s.execute("SELECT * FROM mytable")

        row = results[0].b
        for expected, actual in zip(params, row):
            self.assertEqual(expected, actual)

        c.shutdown()

    def insert_select_column(self, session, table_name, column_name, value):
        insert = session.prepare("INSERT INTO %s (k, %s) VALUES (?, ?)" % (table_name, column_name))
        session.execute(insert, (0, value))
        result = session.execute("SELECT %s FROM %s WHERE k=%%s" % (column_name, table_name), (0,))[0][0]
        self.assertEqual(result, value)

    def test_can_insert_nested_collections(self):
        """
        Test for inserting various types of nested COLLECTION_TYPES into tables and UDTs
        """

        if self.cass_version < (2, 1, 3):
            raise unittest.SkipTest("Support for nested collections was introduced in Cassandra 2.1.3")

        c = TestCluster()
        s = c.connect(self.keyspace_name, wait_for_all_pools=True)
        s.encoder.mapping[tuple] = s.encoder.cql_encode_tuple

        name = self._testMethodName

        s.execute("""
            CREATE TYPE %s (
                m frozen<map<int,text>>,
                t tuple<int,text>,
                l frozen<list<int>>,
                s frozen<set<int>>
            )""" % name)
        s.execute("""
            CREATE TYPE %s_nested (
                m frozen<map<int,text>>,
                t tuple<int,text>,
                l frozen<list<int>>,
                s frozen<set<int>>,
                u frozen<%s>
            )""" % (name, name))
        s.execute("""
            CREATE TABLE %s (
                k int PRIMARY KEY,
                map_map map<frozen<map<int,int>>, frozen<map<int,int>>>,
                map_set map<frozen<set<int>>, frozen<set<int>>>,
                map_list map<frozen<list<int>>, frozen<list<int>>>,
                map_tuple map<frozen<tuple<int, int>>, frozen<tuple<int>>>,
                map_udt map<frozen<%s_nested>, frozen<%s>>,
            )""" % (name, name, name))

        validate = partial(self.insert_select_column, s, name)
        validate('map_map', OrderedMap([({1: 1, 2: 2}, {3: 3, 4: 4}), ({5: 5, 6: 6}, {7: 7, 8: 8})]))
        validate('map_set', OrderedMap([(set((1, 2)), set((3, 4))), (set((5, 6)), set((7, 8)))]))
        validate('map_list', OrderedMap([([1, 2], [3, 4]), ([5, 6], [7, 8])]))
        validate('map_tuple', OrderedMap([((1, 2), (3,)), ((4, 5), (6,))]))

        value = nested_collection_udt({1: 'v1', 2: 'v2'}, (3, 'v3'), [4, 5, 6, 7], set((8, 9, 10)))
        key = nested_collection_udt_nested(value.m, value.t, value.l, value.s, value)
        key2 = nested_collection_udt_nested({3: 'v3'}, value.t, value.l, value.s, value)
        validate('map_udt', OrderedMap([(key, value), (key2, value)]))

        c.shutdown()

    def test_non_alphanum_identifiers(self):
        """
        PYTHON-413
        """
        s = self.session
        non_alphanum_name = 'test.field@#$%@%#!'
        type_name = 'type2'
        s.execute('CREATE TYPE "%s" ("%s" text)' % (non_alphanum_name, non_alphanum_name))
        s.execute('CREATE TYPE %s ("%s" text)' % (type_name, non_alphanum_name))
        # table with types as map keys to make sure the tuple lookup works
        s.execute('CREATE TABLE %s (k int PRIMARY KEY, non_alphanum_type_map map<frozen<"%s">, int>, alphanum_type_map map<frozen<%s>, int>)' % (self.table_name, non_alphanum_name, type_name))
        s.execute('INSERT INTO %s (k, non_alphanum_type_map, alphanum_type_map) VALUES (%s, {{"%s": \'nonalphanum\'}: 0}, {{"%s": \'alphanum\'}: 1})' % (self.table_name, 0, non_alphanum_name, non_alphanum_name))
        row = s.execute('SELECT * FROM %s' % (self.table_name,))[0]

        k, v = row.non_alphanum_type_map.popitem()
        self.assertEqual(v, 0)
        self.assertEqual(k.__class__, tuple)
        self.assertEqual(k[0], 'nonalphanum')

        k, v = row.alphanum_type_map.popitem()
        self.assertEqual(v, 1)
        self.assertNotEqual(k.__class__, tuple)  # should be the namedtuple type
        self.assertEqual(k[0], 'alphanum')
        self.assertEqual(k.field_0_, 'alphanum')  # named tuple with positional field name

    @lessthancass30
    def test_type_alteration(self):
        """
        Support for ALTER TYPE was removed in CASSANDRA-12443
        """
        s = self.session
        type_name = "type_name"
        self.assertNotIn(type_name, s.cluster.metadata.keyspaces['udttests'].user_types)
        s.execute('CREATE TYPE %s (v0 int)' % (type_name,))
        self.assertIn(type_name, s.cluster.metadata.keyspaces['udttests'].user_types)

        s.execute('CREATE TABLE %s (k int PRIMARY KEY, v frozen<%s>)' % (self.table_name, type_name))
        s.execute('INSERT INTO %s (k, v) VALUES (0, {v0 : 1})' % (self.table_name,))

        s.cluster.register_user_type('udttests', type_name, dict)

        val = s.execute('SELECT v FROM %s' % self.table_name)[0][0]
        self.assertEqual(val['v0'], 1)

        # add field
        s.execute('ALTER TYPE %s ADD v1 text' % (type_name,))
        val = s.execute('SELECT v FROM %s' % self.table_name)[0][0]
        self.assertEqual(val['v0'], 1)
        self.assertIsNone(val['v1'])
        s.execute("INSERT INTO %s (k, v) VALUES (0, {v0 : 2, v1 : 'sometext'})" % (self.table_name,))
        val = s.execute('SELECT v FROM %s' % self.table_name)[0][0]
        self.assertEqual(val['v0'], 2)
        self.assertEqual(val['v1'], 'sometext')

        # alter field type
        s.execute('ALTER TYPE %s ALTER v1 TYPE blob' % (type_name,))
        s.execute("INSERT INTO %s (k, v) VALUES (0, {v0 : 3, v1 : 0xdeadbeef})" % (self.table_name,))
        val = s.execute('SELECT v FROM %s' % self.table_name)[0][0]
        self.assertEqual(val['v0'], 3)
        self.assertEqual(val['v1'], b'\xde\xad\xbe\xef')

    @lessthancass30
    def test_alter_udt(self):
        """
        Test to ensure that altered UDT's are properly surfaced without needing to restart the underlying session.

        @since 3.0.0
        @jira_ticket PYTHON-226
        @expected_result UDT's will reflect added columns without a session restart.

        @test_category data_types, udt
        """

        # Create udt ensure it has the proper column names.
        self.session.set_keyspace(self.keyspace_name)
        self.session.execute("CREATE TYPE typetoalter (a int)")
        typetoalter = namedtuple('typetoalter', ('a'))
        self.session.execute("CREATE TABLE {0} (pk int primary key, typetoalter frozen<typetoalter>)".format(self.function_table_name))
        insert_statement = self.session.prepare("INSERT INTO {0} (pk, typetoalter) VALUES (?, ?)".format(self.function_table_name))
        self.session.execute(insert_statement, [1, typetoalter(1)])
        results = self.session.execute("SELECT * from {0}".format(self.function_table_name))
        for result in results:
            self.assertTrue(hasattr(result.typetoalter, 'a'))
            self.assertFalse(hasattr(result.typetoalter, 'b'))

        # Alter UDT and ensure the alter is honored in results
        self.session.execute("ALTER TYPE typetoalter add b int")
        typetoalter = namedtuple('typetoalter', ('a', 'b'))
        self.session.execute(insert_statement, [2, typetoalter(2, 2)])
        results = self.session.execute("SELECT * from {0}".format(self.function_table_name))
        for result in results:
            self.assertTrue(hasattr(result.typetoalter, 'a'))
            self.assertTrue(hasattr(result.typetoalter, 'b'))
