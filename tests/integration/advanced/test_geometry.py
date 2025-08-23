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


from tests.integration import DSE_VERSION, requiredse
from tests.integration.advanced import BasicGeometricUnitTestCase, use_single_node_with_graph
from cassandra.util import OrderedMap, sortedset
from collections import namedtuple

import unittest
from uuid import uuid1
from cassandra.util import Point, LineString, Polygon
from cassandra.cqltypes import LineStringType, PointType, PolygonType


def setup_module():
    if DSE_VERSION:
        use_single_node_with_graph()


class AbstractGeometricTypeTest():

    original_value = ""

    def test_should_insert_simple(self):
        """
        This tests will attempt to insert a point, polygon, or line, using simple inline formatting.
        @since 3.20
        @jira_ticket PYTHON-456
        @test_category dse geometric
        @expected_result geometric types should be able to be inserted and queried.
        """
        uuid_key = uuid1()
        self.session.execute("INSERT INTO tbl (k, g) VALUES (%s, %s)", [uuid_key, self.original_value])
        self.validate('g', uuid_key, self.original_value)

    def test_should_insert_simple_prepared(self):
        """
        This tests will attempt to insert a point, polygon, or line, using prepared statements.
        @since 3.20
        @jira_ticket PYTHON-456
        @test_category dse geometric
        @expected_result geometric types should be able to be inserted and queried.
        """
        uuid_key = uuid1()
        prepared = self.session.prepare("INSERT INTO tbl (k, g) VALUES (?, ?)")
        self.session.execute(prepared, (uuid_key, self.original_value))
        self.validate('g', uuid_key, self.original_value)

    def test_should_insert_simple_prepared_with_bound(self):
        """
        This tests will attempt to insert a point, polygon, or line, using prepared statements and bind.
        @since 3.20
        @jira_ticket PYTHON-456
        @test_category dse geometric
        @expected_result geometric types should be able to be inserted and queried.
        """
        uuid_key = uuid1()
        prepared = self.session.prepare("INSERT INTO tbl (k, g) VALUES (?, ?)")
        bound_statement = prepared.bind((uuid_key, self.original_value))
        self.session.execute(bound_statement)
        self.validate('g', uuid_key, self.original_value)

    def test_should_insert_as_list(self):
        """
        This tests will attempt to insert a point, polygon, or line, as values of list.
        @since 3.20
        @jira_ticket PYTHON-456
        @test_category dse geometric
        @expected_result geometric types should be able to be inserted and queried as a list.
        """
        uuid_key = uuid1()
        prepared = self.session.prepare("INSERT INTO tbl (k, l) VALUES (?, ?)")
        bound_statement = prepared.bind((uuid_key, [self.original_value]))
        self.session.execute(bound_statement)
        self.validate('l', uuid_key, [self.original_value])

    def test_should_insert_as_set(self):
        """
        This tests will attempt to insert a point, polygon, or line, as values of set.
        @since 3.20
        @jira_ticket PYTHON-456
        @test_category dse geometric
        @expected_result geometric types should be able to be inserted and queried as a set.
        """
        uuid_key = uuid1()
        prepared = self.session.prepare("INSERT INTO tbl (k, s) VALUES (?, ?)")
        bound_statement = prepared.bind((uuid_key, sortedset([self.original_value])))
        self.session.execute(bound_statement)
        self.validate('s', uuid_key, sortedset([self.original_value]))

    def test_should_insert_as_map_keys(self):
        """
        This tests will attempt to insert a point, polygon, or line, as keys of a map.
        @since 3.20
        @jira_ticket PYTHON-456
        @test_category dse geometric
        @expected_result geometric types should be able to be inserted and queried as keys of a map.
        """
        uuid_key = uuid1()
        prepared = self.session.prepare("INSERT INTO tbl (k, m0) VALUES (?, ?)")
        bound_statement = prepared.bind((uuid_key, OrderedMap(zip([self.original_value], [1]))))
        self.session.execute(bound_statement)
        self.validate('m0', uuid_key, OrderedMap(zip([self.original_value], [1])))

    def test_should_insert_as_map_values(self):
        """
        This tests will attempt to insert a point, polygon, or line, as values of a map.
        @since 3.20
        @jira_ticket PYTHON-456
        @test_category dse geometric
        @expected_result geometric types should be able to be inserted and queried as values of a map.
        """
        uuid_key = uuid1()
        prepared = self.session.prepare("INSERT INTO tbl (k, m1) VALUES (?, ?)")
        bound_statement = prepared.bind((uuid_key, OrderedMap(zip([1], [self.original_value]))))
        self.session.execute(bound_statement)
        self.validate('m1', uuid_key, OrderedMap(zip([1], [self.original_value])))

    def test_should_insert_as_tuple(self):
        """
        This tests will attempt to insert a point, polygon, or line, as values of a tuple.
        @since 3.20
        @jira_ticket PYTHON-456
        @test_category dse geometric
        @expected_result geometric types should be able to be inserted and queried as values of a tuple.
        """
        uuid_key = uuid1()
        prepared = self.session.prepare("INSERT INTO tbl (k, t) VALUES (?, ?)")
        bound_statement = prepared.bind((uuid_key, (self.original_value, self.original_value, self.original_value)))
        self.session.execute(bound_statement)
        self.validate('t', uuid_key, (self.original_value, self.original_value, self.original_value))

    def test_should_insert_as_udt(self):
        """
        This tests will attempt to insert a point, polygon, or line, as members of a udt.
        @since 3.20
        @jira_ticket PYTHON-456
        @test_category dse geometric
        @expected_result geometric types should be able to be inserted and queried as members of a udt.
        """
        UDT1 = namedtuple('udt1', ('g'))
        self.cluster.register_user_type(self.ks_name, 'udt1', UDT1)
        uuid_key = uuid1()
        prepared = self.session.prepare("INSERT INTO tbl (k, u) values (?, ?)")
        bound_statement = prepared.bind((uuid_key, UDT1(self.original_value)))
        self.session.execute(bound_statement)
        rs = self.session.execute("SELECT {0} from {1} where k={2}".format('u', 'tbl', uuid_key))
        retrieved_udt = rs[0]._asdict()['u']

        self.assertEqual(retrieved_udt.g, self.original_value)

    def test_should_accept_as_partition_key(self):
        """
        This tests will attempt to insert a point, polygon, or line, as a partition key.
        @since 3.20
        @jira_ticket PYTHON-456
        @test_category dse geometric
        @expected_result geometric types should be able to be inserted and queried as a partition key.
        """
        prepared = self.session.prepare("INSERT INTO tblpk (k, v) VALUES (?, ?)")
        bound_statement = prepared.bind((self.original_value, 1))
        self.session.execute(bound_statement)
        rs = self.session.execute("SELECT k, v FROM tblpk")
        foundpk = rs[0]._asdict()['k']
        self.assertEqual(foundpk, self.original_value)

    def validate(self, value, key, expected):
        """
        Simple utility method used for validation of inserted types.
        """
        rs = self.session.execute("SELECT {0} from tbl where k={1}".format(value, key))
        retrieved = rs[0]._asdict()[value]
        self.assertEqual(expected, retrieved)

    def test_insert_empty_with_string(self):
        """
        This tests will attempt to insert a point, polygon, or line, as Empty
        @since 3.20
        @jira_ticket PYTHON-481
        @test_category dse geometric
        @expected_result EMPTY as a  keyword should be honored
        """
        uuid_key = uuid1()
        self.session.execute("INSERT INTO tbl (k, g) VALUES (%s, %s)", [uuid_key, self.empty_statement])
        self.validate('g', uuid_key, self.empty_value)

    def test_insert_empty_with_object(self):
        """
        This tests will attempt to insert a point, polygon, or line, as Empty
        @since 3.20
        @jira_ticket PYTHON-481
        @test_category dse geometric
        @expected_result EMPTY as a keyword should be used with empty objects
        """
        uuid_key = uuid1()
        prepared = self.session.prepare("INSERT INTO tbl (k, g) VALUES (?, ?)")
        self.session.execute(prepared, (uuid_key, self.empty_value))
        self.validate('g', uuid_key, self.empty_value)


@requiredse
class BasicGeometricPointTypeTest(AbstractGeometricTypeTest, BasicGeometricUnitTestCase):
    """
    Runs all the geometric tests against PointType
    """
    cql_type_name = "'{0}'".format(PointType.typename)
    original_value = Point(.5, .13)

    @unittest.skip("Empty String")
    def test_insert_empty_with_string(self):
        pass

    @unittest.skip("Empty String")
    def test_insert_empty_with_object(self):
        pass


@requiredse
class BasicGeometricLineStringTypeTest(AbstractGeometricTypeTest, BasicGeometricUnitTestCase):
    """
    Runs all the geometric tests against LineStringType
    """
    cql_type_name = cql_type_name = "'{0}'".format(LineStringType.typename)
    original_value = LineString(((1, 2), (3, 4), (9871234, 1235487215)))
    empty_statement = 'LINESTRING EMPTY'
    empty_value = LineString()


@requiredse
class BasicGeometricPolygonTypeTest(AbstractGeometricTypeTest, BasicGeometricUnitTestCase):
    """
    Runs all the geometric tests against PolygonType
    """
    cql_type_name = cql_type_name = "'{0}'".format(PolygonType.typename)
    original_value = Polygon([(10.0, 10.0), (110.0, 10.0), (110., 110.0), (10., 110.0), (10., 10.0)], [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)], [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]])
    empty_statement = 'POLYGON EMPTY'
    empty_value = Polygon()
