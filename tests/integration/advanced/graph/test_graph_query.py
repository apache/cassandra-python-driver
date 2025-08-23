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


import sys
from packaging.version import Version

from copy import copy
from itertools import chain
import json
import time

import unittest

from cassandra import OperationTimedOut, ConsistencyLevel, InvalidRequest
from cassandra.cluster import EXEC_PROFILE_GRAPH_DEFAULT, NoHostAvailable
from cassandra.protocol import ServerError, SyntaxException
from cassandra.query import QueryTrace
from cassandra.util import Point
from cassandra.graph import (SimpleGraphStatement, single_object_row_factory,
                       Result, GraphOptions, GraphProtocol, to_bigint)
from cassandra.datastax.graph.query import _graph_options
from cassandra.datastax.graph.types import T

from tests.integration import DSE_VERSION, requiredse, greaterthanorequaldse68
from tests.integration.advanced.graph import BasicGraphUnitTestCase, GraphTestConfiguration, \
    validate_classic_vertex, GraphUnitTestCase, validate_classic_edge, validate_path_result_type, \
    validate_line_edge, validate_generic_vertex_result_type, \
    ClassicGraphSchema, CoreGraphSchema, VertexLabel


@requiredse
class BasicGraphQueryTest(BasicGraphUnitTestCase):

    def test_consistency_passing(self):
        """
        Test to validated that graph consistency levels are properly surfaced to the base driver

        @since 1.0.0
        @jira_ticket PYTHON-509
        @expected_result graph consistency levels are surfaced correctly
        @test_category dse graph
        """
        cl_attrs = ('graph_read_consistency_level', 'graph_write_consistency_level')

        # Iterates over the graph options and constructs an array containing
        # The graph_options that correlate to graph read and write consistency levels
        graph_params = [a[2] for a in _graph_options if a[0] in cl_attrs]

        s = self.session
        default_profile = s.cluster.profile_manager.profiles[EXEC_PROFILE_GRAPH_DEFAULT]
        default_graph_opts = default_profile.graph_options
        try:
            # Checks the default graph attributes and ensures that both  graph_read_consistency_level and graph_write_consistency_level
            # Are None by default
            for attr in cl_attrs:
                self.assertIsNone(getattr(default_graph_opts, attr))

            res = s.execute_graph("null")
            for param in graph_params:
                self.assertNotIn(param, res.response_future.message.custom_payload)

            # session defaults are passed
            opts = GraphOptions()
            opts.update(default_graph_opts)
            cl = {0: ConsistencyLevel.ONE, 1: ConsistencyLevel.LOCAL_QUORUM}
            for k, v in cl.items():
                setattr(opts, cl_attrs[k], v)
            default_profile.graph_options = opts

            res = s.execute_graph("null")

            for k, v in cl.items():
                self.assertEqual(res.response_future.message.custom_payload[graph_params[k]], ConsistencyLevel.value_to_name[v].encode())

            # passed profile values override session defaults
            cl = {0: ConsistencyLevel.ALL, 1: ConsistencyLevel.QUORUM}
            opts = GraphOptions()
            opts.update(default_graph_opts)
            for k, v in cl.items():
                attr_name = cl_attrs[k]
                setattr(opts, attr_name, v)
                self.assertNotEqual(getattr(default_profile.graph_options, attr_name), getattr(opts, attr_name))
            tmp_profile = s.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT, graph_options=opts)
            res = s.execute_graph("null", execution_profile=tmp_profile)

            for k, v in cl.items():
                self.assertEqual(res.response_future.message.custom_payload[graph_params[k]], ConsistencyLevel.value_to_name[v].encode())
        finally:
            default_profile.graph_options = default_graph_opts

    def test_execute_graph_row_factory(self):
        s = self.session

        # default Results
        default_profile = s.cluster.profile_manager.profiles[EXEC_PROFILE_GRAPH_DEFAULT]
        self.assertEqual(default_profile.row_factory, None)  # will be resolved to graph_object_row_factory
        result = s.execute_graph("123")[0]
        self.assertIsInstance(result, Result)
        self.assertEqual(result.value, 123)

        # other via parameter
        prof = s.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT, row_factory=single_object_row_factory)
        rs = s.execute_graph("123", execution_profile=prof)
        self.assertEqual(rs.response_future.row_factory, single_object_row_factory)
        self.assertEqual(json.loads(rs[0]), {'result': 123})

    def test_execute_graph_timeout(self):
        s = self.session

        value = [1, 2, 3]
        query = "[%r]" % (value,)

        # default is passed down
        default_graph_profile = s.cluster.profile_manager.profiles[EXEC_PROFILE_GRAPH_DEFAULT]
        rs = self.session.execute_graph(query)
        self.assertEqual(rs[0].value, value)
        self.assertEqual(rs.response_future.timeout, default_graph_profile.request_timeout)

        # tiny timeout times out as expected
        tmp_profile = copy(default_graph_profile)
        tmp_profile.request_timeout = sys.float_info.min

        max_retry_count = 10
        for _ in range(max_retry_count):
            start = time.time()
            try:
                with self.assertRaises(OperationTimedOut):
                    s.execute_graph(query, execution_profile=tmp_profile)
                break
            except:
                end = time.time()
                self.assertAlmostEqual(start, end, 1)
        else:
            raise Exception("session.execute_graph didn't time out in {0} tries".format(max_retry_count))

    def test_profile_graph_options(self):
        s = self.session
        statement = SimpleGraphStatement("true")
        ep = self.session.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT)
        self.assertTrue(s.execute_graph(statement, execution_profile=ep)[0].value)

        # bad graph name to verify it's passed
        ep.graph_options = ep.graph_options.copy()
        ep.graph_options.graph_name = "definitely_not_correct"
        try:
            s.execute_graph(statement, execution_profile=ep)
        except NoHostAvailable:
            self.assertTrue(DSE_VERSION >= Version("6.0"))
        except InvalidRequest:
            self.assertTrue(DSE_VERSION >= Version("5.0"))
        else:
            if DSE_VERSION < Version("6.8"):  # >6.8 returns true
                self.fail("Should have risen ServerError or InvalidRequest")

    def test_additional_custom_payload(self):
        s = self.session
        custom_payload = {'some': 'example'.encode('utf-8'), 'items': 'here'.encode('utf-8')}
        sgs = SimpleGraphStatement("null", custom_payload=custom_payload)
        future = s.execute_graph_async(sgs)

        default_profile = s.cluster.profile_manager.profiles[EXEC_PROFILE_GRAPH_DEFAULT]
        default_graph_opts = default_profile.graph_options
        for k, v in chain(custom_payload.items(), default_graph_opts.get_options_map().items()):
            self.assertEqual(future.message.custom_payload[k], v)


class GenericGraphQueryTest(GraphUnitTestCase):

    def _test_basic_query(self, schema, graphson):
        """
        Test to validate that basic graph query results can be executed with a sane result set.

        Creates a simple classic tinkerpot graph, and attempts to find all vertices
        related the vertex marco, that have a label of knows.
        See reference graph here
        http://www.tinkerpop.com/docs/3.0.0.M1/

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result graph should find two vertices related to marco via 'knows' edges.

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.classic(), graphson)
        rs = self.execute_graph('''g.V().has('name','marko').out('knows').values('name')''', graphson)
        self.assertFalse(rs.has_more_pages)
        results_list = self.resultset_to_list(rs)
        self.assertEqual(len(results_list), 2)
        self.assertIn('vadas', results_list)
        self.assertIn('josh', results_list)

    def _test_geometric_graph_types(self, schema, graphson):
        """
        Test to validate that geometric types function correctly

        Creates a very simple graph, and tries to insert a simple point type

        @since 1.0.0
        @jira_ticket DSP-8087
        @expected_result json types associated with insert is parsed correctly

        @test_category dse graph
        """
        vertex_label = VertexLabel([('pointP', "Point()")])
        ep = self.get_execution_profile(graphson)
        schema.create_vertex_label(self.session, vertex_label, ep)
        # import org.apache.cassandra.db.marshal.geometry.Point;
        rs = schema.add_vertex(self.session, vertex_label, 'pointP', Point(0, 1), ep)

        # if result set is not parsed correctly this will throw an exception
        self.assertIsNotNone(rs)

    def _test_execute_graph_trace(self, schema, graphson):
        value = [1, 2, 3]
        query = "[%r]" % (value,)

        # default is no trace
        rs = self.execute_graph(query, graphson)
        results = self.resultset_to_list(rs)
        self.assertEqual(results[0], value)
        self.assertIsNone(rs.get_query_trace())

        # request trace
        rs = self.execute_graph(query, graphson, trace=True)
        results = self.resultset_to_list(rs)
        self.assertEqual(results[0], value)
        qt = rs.get_query_trace(max_wait_sec=10)
        self.assertIsInstance(qt, QueryTrace)
        self.assertIsNotNone(qt.duration)

    def _test_range_query(self, schema, graphson):
        """
        Test to validate range queries are handled correctly.

        Creates a very large line graph script and executes it. Then proceeds to a range
        limited query against it, and ensure that the results are formatted correctly and that
        the result set is properly sized.

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result result set should be properly formatted and properly sized

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.line(150), graphson)
        rs = self.execute_graph("g.E().range(0,10)", graphson)
        self.assertFalse(rs.has_more_pages)
        results = self.resultset_to_list(rs)
        self.assertEqual(len(results), 10)
        ep = self.get_execution_profile(graphson)
        for result in results:
            schema.ensure_properties(self.session, result, execution_profile=ep)
            validate_line_edge(self, result)

    def _test_classic_graph(self, schema, graphson):
        """
        Test to validate that basic graph generation, and vertex and edges are surfaced correctly

        Creates a simple classic tinkerpot graph, and iterates over the the vertices and edges
        ensureing that each one is correct. See reference graph here
        http://www.tinkerpop.com/docs/3.0.0.M1/

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result graph should generate and all vertices and edge results should be

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.classic(), graphson)
        rs = self.execute_graph('g.V()', graphson)
        ep = self.get_execution_profile(graphson)
        for vertex in rs:
            schema.ensure_properties(self.session, vertex, execution_profile=ep)
            validate_classic_vertex(self, vertex)
        rs = self.execute_graph('g.E()', graphson)
        for edge in rs:
            schema.ensure_properties(self.session, edge, execution_profile=ep)
            validate_classic_edge(self, edge)

    def _test_graph_classic_path(self, schema, graphson):
        """
        Test to validate that the path version of the result type is generated correctly. It also
        tests basic path results as that is not covered elsewhere

        @since 1.0.0
        @jira_ticket PYTHON-479
        @expected_result path object should be unpacked correctly including all nested edges and verticies
        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.classic(), graphson)
        rs = self.execute_graph("g.V().hasLabel('person').has('name', 'marko').as('a').outE('knows').inV().as('c', 'd')."
                                "   outE('created').as('e', 'f', 'g').inV().path()",
                                graphson)
        rs_list = list(rs)
        self.assertEqual(len(rs_list), 2)
        for result in rs_list:
            try:
                path = result.as_path()
            except:
                path = result

            ep = self.get_execution_profile(graphson)
            for obj in path.objects:
                schema.ensure_properties(self.session, obj, ep)

            validate_path_result_type(self, path)

    def _test_large_create_script(self, schema, graphson):
        """
        Test to validate that server errors due to large groovy scripts are properly surfaced

        Creates a very large line graph script and executes it. Then proceeds to create a line graph script
        that is to large for the server to handle expects a server error to be returned

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result graph should generate and all vertices and edge results should be

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.line(150), graphson)
        self.execute_graph(schema.fixtures.line(300), graphson)  # This should pass since the queries are split
        self.assertRaises(SyntaxException, self.execute_graph, schema.fixtures.line(300, single_script=True), graphson)  # this is not and too big

    def _test_large_result_set(self, schema, graphson):
        """
        Test to validate that large result sets return correctly.

        Creates a very large graph. Ensures that large result sets are handled appropriately.

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result when limits of result sets are hit errors should be surfaced appropriately

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.large(), graphson, execution_profile_options={'request_timeout': 32})
        rs = self.execute_graph("g.V()", graphson)
        for result in rs:
            validate_generic_vertex_result_type(self, result)

    def _test_param_passing(self, schema, graphson):
        """
        Test to validate that parameter passing works as expected

        @since 1.0.0
        @jira_ticket PYTHON-457
        @expected_result parameters work as expected

        @test_category dse graph
        """

        # unused parameters are passed, but ignored
        self.execute_graph("null", graphson, params={"doesn't": "matter", "what's": "passed"})

        # multiple params
        rs = self.execute_graph("[a, b]", graphson, params={'a': 0, 'b': 1})
        results = self.resultset_to_list(rs)
        self.assertEqual(results[0], 0)
        self.assertEqual(results[1], 1)

        if graphson == GraphProtocol.GRAPHSON_1_0:
            # different value types
            for param in (None, "string", 1234, 5.678, True, False):
                result = self.resultset_to_list(self.execute_graph('x', graphson, params={'x': param}))[0]
                self.assertEqual(result, param)

    def _test_vertex_property_properties(self, schema, graphson):
        """
        Test verifying vertex property properties

        @since 1.0.0
        @jira_ticket PYTHON-487

        @test_category dse graph
        """
        if schema is not ClassicGraphSchema:
            raise unittest.SkipTest('skipped because rich properties are only supported with classic graphs')

        self.execute_graph("schema.propertyKey('k0').Text().ifNotExists().create();", graphson)
        self.execute_graph("schema.propertyKey('k1').Text().ifNotExists().create();", graphson)
        self.execute_graph("schema.propertyKey('key').Text().properties('k0', 'k1').ifNotExists().create();", graphson)
        self.execute_graph("schema.vertexLabel('MLP').properties('key').ifNotExists().create();", graphson)
        v = self.execute_graph('''v = graph.addVertex('MLP')
                                 v.property('key', 'value', 'k0', 'v0', 'k1', 'v1')
                                 v''', graphson)[0]
        self.assertEqual(len(v.properties), 1)
        self.assertEqual(len(v.properties['key']), 1)
        p = v.properties['key'][0]
        self.assertEqual(p.label, 'key')
        self.assertEqual(p.value, 'value')
        self.assertEqual(p.properties, {'k0': 'v0', 'k1': 'v1'})

    def _test_vertex_multiple_properties(self, schema, graphson):
        """
        Test verifying vertex property form for various Cardinality

        All key types are encoded as a list, regardless of cardinality

        Single cardinality properties have only one value -- the last one added

        Default is single (this is config dependent)

        @since 1.0.0
        @jira_ticket PYTHON-487

        @test_category dse graph
        """
        if schema is not ClassicGraphSchema:
            raise unittest.SkipTest('skipped because multiple properties are only  supported with classic graphs')

        self.execute_graph('''Schema schema = graph.schema();
                           schema.propertyKey('mult_key').Text().multiple().ifNotExists().create();
                           schema.propertyKey('single_key').Text().single().ifNotExists().create();
                           schema.vertexLabel('MPW1').properties('mult_key').ifNotExists().create();
                           schema.vertexLabel('SW1').properties('single_key').ifNotExists().create();''', graphson)

        v = self.execute_graph('''v = graph.addVertex('MPW1')
                             v.property('mult_key', 'value')
                             v''', graphson)[0]
        self.assertEqual(len(v.properties), 1)
        self.assertEqual(len(v.properties['mult_key']), 1)
        self.assertEqual(v.properties['mult_key'][0].label, 'mult_key')
        self.assertEqual(v.properties['mult_key'][0].value, 'value')

        # multiple_with_two_values
        v = self.execute_graph('''g.addV('MPW1').property('mult_key', 'value0').property('mult_key', 'value1')''', graphson)[0]
        self.assertEqual(len(v.properties), 1)
        self.assertEqual(len(v.properties['mult_key']), 2)
        self.assertEqual(v.properties['mult_key'][0].label, 'mult_key')
        self.assertEqual(v.properties['mult_key'][1].label, 'mult_key')
        self.assertEqual(v.properties['mult_key'][0].value, 'value0')
        self.assertEqual(v.properties['mult_key'][1].value, 'value1')

        # single_with_one_value
        v = self.execute_graph('''v = graph.addVertex('SW1')
                             v.property('single_key', 'value')
                             v''', graphson)[0]
        self.assertEqual(len(v.properties), 1)
        self.assertEqual(len(v.properties['single_key']), 1)
        self.assertEqual(v.properties['single_key'][0].label, 'single_key')
        self.assertEqual(v.properties['single_key'][0].value, 'value')

        if DSE_VERSION < Version('6.8'):
            # single_with_two_values
            with self.assertRaises(InvalidRequest):
                v = self.execute_graph('''
                    v = graph.addVertex('SW1')
                    v.property('single_key', 'value0').property('single_key', 'value1').next()
                    v
                ''', graphson)[0]
        else:
            # >=6.8 single_with_two_values, first one wins
            v = self.execute_graph('''v = graph.addVertex('SW1')
                v.property('single_key', 'value0').property('single_key', 'value1')
                v''', graphson)[0]
            self.assertEqual(v.properties['single_key'][0].value, 'value0')

    def _test_result_forms(self, schema, graphson):
        """
        Test to validate that geometric types function correctly

        Creates a very simple graph, and tries to insert a simple point type

        @since 1.0.0
        @jira_ticket DSP-8087
        @expected_result json types associated with insert is parsed correctly

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.classic(), graphson)
        ep = self.get_execution_profile(graphson)

        results = self.resultset_to_list(self.session.execute_graph('g.V()', execution_profile=ep))
        self.assertGreater(len(results), 0, "Result set was empty this was not expected")
        for v in results:
            schema.ensure_properties(self.session, v, ep)
            validate_classic_vertex(self, v)

        results = self.resultset_to_list(self.session.execute_graph('g.E()', execution_profile=ep))
        self.assertGreater(len(results), 0, "Result set was empty this was not expected")
        for e in results:
            schema.ensure_properties(self.session, e, ep)
            validate_classic_edge(self, e)

    def _test_query_profile(self, schema, graphson):
        """
        Test to validate profiling results are deserialized properly.

        @since 1.6.0
        @jira_ticket PYTHON-1057
        @expected_result TraversalMetrics and Metrics are deserialized properly

        @test_category dse graph
        """
        if graphson == GraphProtocol.GRAPHSON_1_0:
            raise unittest.SkipTest('skipped because there is no metrics deserializer with graphson1')

        ep = self.get_execution_profile(graphson)
        results = list(self.session.execute_graph("g.V().profile()", execution_profile=ep))
        self.assertEqual(len(results), 1)
        self.assertIn('metrics', results[0])
        self.assertIn('dur', results[0])
        self.assertEqual(len(results[0]['metrics']), 2)
        self.assertIn('dur', results[0]['metrics'][0])

    def _test_query_bulkset(self, schema, graphson):
        """
        Test to validate bulkset results are deserialized properly.

        @since 1.6.0
        @jira_ticket PYTHON-1060
        @expected_result BulkSet is deserialized properly to a list

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.classic(), graphson)
        ep = self.get_execution_profile(graphson)
        results = list(self.session.execute_graph(
            'g.V().hasLabel("person").aggregate("x").by("age").cap("x")',
            execution_profile=ep))
        self.assertEqual(len(results), 1)
        results = results[0]
        if type(results) is Result:
            results = results.value
        else:
            self.assertEqual(len(results), 5)
        self.assertEqual(results.count(35), 2)

    @greaterthanorequaldse68
    def _test_elementMap_query(self, schema, graphson):
        """
        Test to validate that an elementMap can be serialized properly.
        """
        self.execute_graph(schema.fixtures.classic(), graphson)
        rs = self.execute_graph('''g.V().has('name','marko').elementMap()''', graphson)
        results_list = self.resultset_to_list(rs)
        self.assertEqual(len(results_list), 1)
        row = results_list[0]
        if graphson == GraphProtocol.GRAPHSON_3_0:
            self.assertIn(T.id, row)
            self.assertIn(T.label, row)
            if schema is CoreGraphSchema:
                self.assertEqual(row[T.id], 'dseg:/person/marko')
                self.assertEqual(row[T.label], 'person')
        else:
            self.assertIn('id', row)
            self.assertIn('label', row)


@GraphTestConfiguration.generate_tests(schema=ClassicGraphSchema)
class ClassicGraphQueryTest(GenericGraphQueryTest):
    pass


@GraphTestConfiguration.generate_tests(schema=CoreGraphSchema)
class CoreGraphQueryTest(GenericGraphQueryTest):
    pass


@GraphTestConfiguration.generate_tests(schema=CoreGraphSchema)
class CoreGraphQueryWithTypeWrapperTest(GraphUnitTestCase):

    def _test_basic_query_with_type_wrapper(self, schema, graphson):
        """
        Test to validate that a query using a type wrapper works.

        @since 2.8.0
        @jira_ticket PYTHON-1051
        @expected_result graph query works and doesn't raise an exception

        @test_category dse graph
        """
        ep = self.get_execution_profile(graphson)
        vl = VertexLabel(['tupleOf(Int, Bigint)'])
        schema.create_vertex_label(self.session, vl, execution_profile=ep)

        prop_name = next(iter(vl.non_pk_properties.keys()))
        with self.assertRaises(InvalidRequest):
            schema.add_vertex(self.session, vl, prop_name, (1, 42), execution_profile=ep)

        schema.add_vertex(self.session, vl, prop_name, (1, to_bigint(42)), execution_profile=ep)
