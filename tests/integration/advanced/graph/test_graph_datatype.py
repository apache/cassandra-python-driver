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

import time
import logging
from packaging.version import Version
from collections import namedtuple

from cassandra.cluster import EXEC_PROFILE_GRAPH_DEFAULT
from cassandra.graph import graph_result_row_factory
from cassandra.graph.query import GraphProtocol
from cassandra.graph.types import VertexProperty

from tests.util import wait_until
from tests.integration.advanced.graph import BasicGraphUnitTestCase, ClassicGraphFixtures, \
    ClassicGraphSchema, CoreGraphSchema
from tests.integration.advanced.graph import VertexLabel, GraphTestConfiguration, GraphUnitTestCase
from tests.integration import DSE_VERSION, requiredse

log = logging.getLogger(__name__)


@requiredse
class GraphBasicDataTypesTests(BasicGraphUnitTestCase):

    def test_result_types(self):
        """
        Test to validate that the edge and vertex version of results are constructed correctly.

        @since 1.0.0
        @jira_ticket PYTHON-479
        @expected_result edge/vertex result types should be unpacked correctly.
        @test_category dse graph
        """
        queries, params = ClassicGraphFixtures.multiple_fields()
        for query in queries:
            self.session.execute_graph(query, params)

        prof = self.session.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT, row_factory=graph_result_row_factory)  # requires simplified row factory to avoid shedding id/~type information used for validation below
        rs = self.session.execute_graph("g.V()", execution_profile=prof)

        for result in rs:
            self._validate_type(result)

    def _validate_type(self, vertex):
        for properties in vertex.properties.values():
            prop = properties[0]

            if DSE_VERSION >= Version("5.1"):
                type_indicator = prop['id']['~label']
            else:
                type_indicator = prop['id']['~type']

            if any(type_indicator.startswith(t) for t in
                   ('int', 'short', 'long', 'bigint', 'decimal', 'smallint', 'varint')):
                typ = int
            elif any(type_indicator.startswith(t) for t in ('float', 'double')):
                typ = float
            elif any(type_indicator.startswith(t) for t in ('duration', 'date', 'negdate', 'time',
                                                            'blob', 'timestamp', 'point', 'linestring', 'polygon',
                                                            'inet', 'uuid')):
                typ = str
            else:
                pass
                self.fail("Received unexpected type: %s" % type_indicator)
            self.assertIsInstance(prop['value'], typ)


class GenericGraphDataTypeTest(GraphUnitTestCase):

    def _test_all_datatypes(self, schema, graphson):
        ep = self.get_execution_profile(graphson)

        for data in schema.fixtures.datatypes().values():
            typ, value, deserializer = data
            vertex_label = VertexLabel([typ])
            property_name = next(iter(vertex_label.non_pk_properties.keys()))
            schema.create_vertex_label(self.session, vertex_label, execution_profile=ep)
            vertex = list(schema.add_vertex(self.session, vertex_label, property_name, value, execution_profile=ep))[0]

            def get_vertex_properties():
                return list(schema.get_vertex_properties(
                    self.session, vertex, execution_profile=ep))

            prop_returned = 1 if DSE_VERSION < Version('5.1') else 2  # include pkid >=5.1
            wait_until(
                lambda: len(get_vertex_properties()) == prop_returned, 0.2, 15)

            vertex_properties = get_vertex_properties()
            if graphson == GraphProtocol.GRAPHSON_1_0:
                vertex_properties = [vp.as_vertex_property() for vp in vertex_properties]

            for vp in vertex_properties:
                if vp.label == 'pkid':
                    continue

                self.assertIsInstance(vp, VertexProperty)
                self.assertEqual(vp.label, property_name)
                if graphson == GraphProtocol.GRAPHSON_1_0:
                    deserialized_value = deserializer(vp.value) if deserializer else vp.value
                    self.assertEqual(deserialized_value, value)
                else:
                    self.assertEqual(vp.value, value)

    def __test_udt(self, schema, graphson, address_class, address_with_tags_class,
                   complex_address_class, complex_address_with_owners_class):
        if schema is not CoreGraphSchema or DSE_VERSION < Version('6.8'):
            raise unittest.SkipTest("Graph UDT is only supported with DSE 6.8+ and Core graphs.")

        ep = self.get_execution_profile(graphson)

        Address = address_class
        AddressWithTags = address_with_tags_class
        ComplexAddress = complex_address_class
        ComplexAddressWithOwners = complex_address_with_owners_class

        # setup udt
        self.session.execute_graph("""
                schema.type('address').property('address', Text).property('city', Text).property('state', Text).create();
                schema.type('addressTags').property('address', Text).property('city', Text).property('state', Text).
                    property('tags', setOf(Text)).create();
                schema.type('complexAddress').property('address', Text).property('address_tags', frozen(typeOf('addressTags'))).
                    property('city', Text).property('state', Text).property('props', mapOf(Text, Int)).create();
                schema.type('complexAddressWithOwners').property('address', Text).
                    property('address_tags', frozen(typeOf('addressTags'))).
                    property('city', Text).property('state', Text).property('props', mapOf(Text, Int)).
                    property('owners', frozen(listOf(tupleOf(Text, Int)))).create();
                """, execution_profile=ep)

        time.sleep(2)  # wait the UDT to be discovered
        self.session.cluster.register_user_type(self.graph_name, 'address', Address)
        self.session.cluster.register_user_type(self.graph_name, 'addressTags', AddressWithTags)
        self.session.cluster.register_user_type(self.graph_name, 'complexAddress', ComplexAddress)
        self.session.cluster.register_user_type(self.graph_name, 'complexAddressWithOwners', ComplexAddressWithOwners)

        data = {
            "udt1": ["typeOf('address')", Address('1440 Rd Smith', 'Quebec', 'QC')],
            "udt2": ["tupleOf(typeOf('address'), Text)", (Address('1440 Rd Smith', 'Quebec', 'QC'), 'hello')],
            "udt3": ["tupleOf(frozen(typeOf('address')), Text)", (Address('1440 Rd Smith', 'Quebec', 'QC'), 'hello')],
            "udt4": ["tupleOf(tupleOf(Int, typeOf('address')), Text)",
                      ((42, Address('1440 Rd Smith', 'Quebec', 'QC')), 'hello')],
            "udt5": ["tupleOf(tupleOf(Int, typeOf('addressTags')), Text)",
                     ((42, AddressWithTags('1440 Rd Smith', 'Quebec', 'QC', {'t1', 't2'})), 'hello')],
            "udt6": ["tupleOf(tupleOf(Int, typeOf('complexAddress')), Text)",
                     ((42, ComplexAddress('1440 Rd Smith',
                                          AddressWithTags('1440 Rd Smith', 'Quebec', 'QC', {'t1', 't2'}),
                                          'Quebec', 'QC', {'p1': 42, 'p2': 33})), 'hello')],
            "udt7": ["tupleOf(tupleOf(Int, frozen(typeOf('complexAddressWithOwners'))), Text)",
                     ((42, ComplexAddressWithOwners(
                           '1440 Rd Smith',
                           AddressWithTags('1440 CRd Smith', 'Quebec', 'QC', {'t1', 't2'}),
                           'Quebec', 'QC', {'p1': 42, 'p2': 33}, [('Mike', 43), ('Gina', 39)])
                       ), 'hello')]
        }

        for typ, value in data.values():
            vertex_label = VertexLabel([typ])
            property_name = next(iter(vertex_label.non_pk_properties.keys()))
            schema.create_vertex_label(self.session, vertex_label, execution_profile=ep)

            vertex = list(schema.add_vertex(self.session, vertex_label, property_name, value, execution_profile=ep))[0]

            def get_vertex_properties():
                return list(schema.get_vertex_properties(
                    self.session, vertex, execution_profile=ep))

            wait_until(
                lambda: len(get_vertex_properties()) == 2, 0.2, 15)

            vertex_properties = get_vertex_properties()
            for vp in vertex_properties:
                if vp.label == 'pkid':
                    continue

                self.assertIsInstance(vp, VertexProperty)
                self.assertEqual(vp.label, property_name)
                self.assertEqual(vp.value, value)

    def _test_udt_with_classes(self, schema, graphson):
        class Address(object):

            def __init__(self, address, city, state):
                self.address = address
                self.city = city
                self.state = state

            def __eq__(self, other):
                return self.address == other.address and self.city == other.city and self.state == other.state

        class AddressWithTags(object):

            def __init__(self, address, city, state, tags):
                self.address = address
                self.city = city
                self.state = state
                self.tags = tags

            def __eq__(self, other):
                return (self.address == other.address and self.city == other.city
                        and self.state == other.state and self.tags == other.tags)

        class ComplexAddress(object):

            def __init__(self, address, address_tags, city, state, props):
                self.address = address
                self.address_tags = address_tags
                self.city = city
                self.state = state
                self.props = props

            def __eq__(self, other):
                return (self.address == other.address and self.address_tags == other.address_tags
                        and self.city == other.city and self.state == other.state
                        and self.props == other.props)

        class ComplexAddressWithOwners(object):

            def __init__(self, address, address_tags, city, state, props, owners):
                self.address = address
                self.address_tags = address_tags
                self.city = city
                self.state = state
                self.props = props
                self.owners = owners

            def __eq__(self, other):
                return (self.address == other.address and self.address_tags == other.address_tags
                        and self.city == other.city and self.state == other.state
                        and self.props == other.props and self.owners == other.owners)

        self.__test_udt(schema, graphson, Address, AddressWithTags, ComplexAddress, ComplexAddressWithOwners)

    def _test_udt_with_namedtuples(self, schema, graphson):
        AddressTuple = namedtuple('Address', ('address', 'city', 'state'))
        AddressWithTagsTuple = namedtuple('AddressWithTags', ('address', 'city', 'state', 'tags'))
        ComplexAddressTuple = namedtuple('ComplexAddress', ('address', 'address_tags', 'city', 'state', 'props'))
        ComplexAddressWithOwnersTuple = namedtuple('ComplexAddressWithOwners', ('address', 'address_tags', 'city',
                                                                                'state', 'props', 'owners'))

        self.__test_udt(schema, graphson, AddressTuple, AddressWithTagsTuple,
                        ComplexAddressTuple, ComplexAddressWithOwnersTuple)


@requiredse
@GraphTestConfiguration.generate_tests(schema=ClassicGraphSchema)
class ClassicGraphDataTypeTest(GenericGraphDataTypeTest):
    pass


@requiredse
@GraphTestConfiguration.generate_tests(schema=CoreGraphSchema)
class CoreGraphDataTypeTest(GenericGraphDataTypeTest):
    pass
