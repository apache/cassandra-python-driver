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

from cassandra import cluster
from cassandra.cluster import ContinuousPagingOptions
from cassandra.datastax.graph.fluent import DseGraph
from cassandra.graph import VertexProperty

from tests.integration import greaterthanorequaldse68
from tests.integration.advanced.graph import (
    GraphUnitTestCase, ClassicGraphSchema, CoreGraphSchema,
    VertexLabel, GraphTestConfiguration
)
from tests.integration import greaterthanorequaldse60
from tests.integration.advanced.graph.fluent import (
    BaseExplicitExecutionTest, create_traversal_profiles, check_equality_base)

import unittest


@greaterthanorequaldse60
@GraphTestConfiguration.generate_tests(traversal=True)
class BatchStatementTests(BaseExplicitExecutionTest):

    def setUp(self):
        super(BatchStatementTests, self).setUp()
        self.ep_graphson2, self.ep_graphson3 = create_traversal_profiles(self.cluster, self.graph_name)

    def _test_batch_with_schema(self, schema, graphson):
        """
        Sends a Batch statement and verifies it has succeeded with a schema created

        @since 1.1.0
        @jira_ticket PYTHON-789
        @expected_result ValueError is arisen

        @test_category dse graph
        """
        self._send_batch_and_read_results(schema, graphson)

    def _test_batch_without_schema(self, schema, graphson):
        """
        Sends a Batch statement and verifies it has succeeded without a schema created

        @since 1.1.0
        @jira_ticket PYTHON-789
        @expected_result ValueError is arisen

        @test_category dse graph
        """
        if schema is not ClassicGraphSchema:
            raise unittest.SkipTest('schema-less is only for classic graphs')
        self._send_batch_and_read_results(schema, graphson, use_schema=False)

    def _test_batch_with_schema_add_all(self, schema, graphson):
        """
        Sends a Batch statement and verifies it has succeeded with a schema created.
        Uses :method:`dse_graph.query._BatchGraphStatement.add_all` to add the statements
        instead of :method:`dse_graph.query._BatchGraphStatement.add`

        @since 1.1.0
        @jira_ticket PYTHON-789
        @expected_result ValueError is arisen

        @test_category dse graph
        """
        self._send_batch_and_read_results(schema, graphson, add_all=True)

    def _test_batch_without_schema_add_all(self, schema, graphson):
        """
        Sends a Batch statement and verifies it has succeeded without a schema created
        Uses :method:`dse_graph.query._BatchGraphStatement.add_all` to add the statements
        instead of :method:`dse_graph.query._BatchGraphStatement.add`

        @since 1.1.0
        @jira_ticket PYTHON-789
        @expected_result ValueError is arisen

        @test_category dse graph
        """
        if schema is not ClassicGraphSchema:
            raise unittest.SkipTest('schema-less is only for classic graphs')
        self._send_batch_and_read_results(schema, graphson, add_all=True, use_schema=False)

    def test_only_graph_traversals_are_accepted(self):
        """
        Verifies that ValueError is risen if the parameter add is not a traversal

        @since 1.1.0
        @jira_ticket PYTHON-789
        @expected_result ValueError is arisen

        @test_category dse graph
        """
        batch = DseGraph.batch()
        self.assertRaises(ValueError, batch.add, '{"@value":{"step":[["addV","poc_int"],'
                                                 '["property","bigint1value",{"@value":12,"@type":"g:Int32"}]]},'
                                                 '"@type":"g:Bytecode"}')
        another_batch = DseGraph.batch()
        self.assertRaises(ValueError, batch.add, another_batch)

    def _send_batch_and_read_results(self, schema, graphson, add_all=False, use_schema=True):
        traversals = []
        datatypes = schema.fixtures.datatypes()
        values = {}
        g = self.fetch_traversal_source(graphson)
        ep = self.get_execution_profile(graphson)
        batch = DseGraph.batch(session=self.session,
                               execution_profile=self.get_execution_profile(graphson, traversal=True))
        for data in datatypes.values():
            typ, value, deserializer = data
            vertex_label = VertexLabel([typ])
            property_name = next(iter(vertex_label.non_pk_properties.keys()))
            values[property_name] = value
            if use_schema or schema is CoreGraphSchema:
                schema.create_vertex_label(self.session, vertex_label, execution_profile=ep)

            traversal = g.addV(str(vertex_label.label)).property('pkid', vertex_label.id).property(property_name, value)
            if not add_all:
                batch.add(traversal)
            traversals.append(traversal)

        if add_all:
            batch.add_all(traversals)

        self.assertEqual(len(datatypes), len(batch))

        batch.execute()

        vertices = self.execute_traversal(g.V(), graphson)
        self.assertEqual(len(vertices), len(datatypes), "g.V() returned {}".format(vertices))

        # Iterate over all the vertices and check that they match the original input
        for vertex in vertices:
            schema.ensure_properties(self.session, vertex, execution_profile=ep)
            key = [k for k in list(vertex.properties.keys()) if k != 'pkid'][0].replace("value", "")
            original = values[key]
            self._check_equality(original, vertex)

    def _check_equality(self, original, vertex):
        for key in vertex.properties:
            if key == 'pkid':
                continue
            value = vertex.properties[key].value \
                if isinstance(vertex.properties[key], VertexProperty) else vertex.properties[key][0].value
            check_equality_base(self, original, value)


class ContinuousPagingOptionsForTests(ContinuousPagingOptions):
    def __init__(self,
                 page_unit=ContinuousPagingOptions.PagingUnit.ROWS, max_pages=1,  # max_pages=1
                 max_pages_per_second=0, max_queue_size=4):
        super(ContinuousPagingOptionsForTests, self).__init__(page_unit, max_pages, max_pages_per_second,
                                                              max_queue_size)


def reset_paging_options():
    cluster.ContinuousPagingOptions = ContinuousPagingOptions


@greaterthanorequaldse68
@GraphTestConfiguration.generate_tests(schema=CoreGraphSchema)
class GraphPagingTest(GraphUnitTestCase):

    def setUp(self):
        super(GraphPagingTest, self).setUp()
        self.addCleanup(reset_paging_options)
        self.ep_graphson2, self.ep_graphson3 = create_traversal_profiles(self.cluster, self.graph_name)

    def _setup_data(self, schema, graphson):
        self.execute_graph(
            "schema.vertexLabel('person').ifNotExists().partitionBy('name', Text).property('age', Int).create();",
            graphson)
        for i in range(100):
            self.execute_graph("g.addV('person').property('name', 'batman-{}')".format(i), graphson)

    def _test_cont_paging_is_enabled_by_default(self, schema, graphson):
        """
        Test that graph paging is automatically enabled with a >=6.8 cluster.

        @jira_ticket PYTHON-1045
        @expected_result the default continuous paging options are used

        @test_category dse graph
        """
        # with traversals... I don't have access to the response future... so this is a hack to ensure paging is on
        cluster.ContinuousPagingOptions = ContinuousPagingOptionsForTests
        ep = self.get_execution_profile(graphson, traversal=True)
        self._setup_data(schema, graphson)
        self.session.default_fetch_size = 10
        g = DseGraph.traversal_source(self.session, execution_profile=ep)
        results = g.V().toList()
        self.assertEqual(len(results), 10)  # only 10 results due to our hack

    def _test_cont_paging_can_be_disabled(self, schema, graphson):
        """
        Test that graph paging can be disabled.

        @jira_ticket PYTHON-1045
        @expected_result the default continuous paging options are not used

        @test_category dse graph
        """
        # with traversals... I don't have access to the response future... so this is a hack to ensure paging is on
        cluster.ContinuousPagingOptions = ContinuousPagingOptionsForTests
        ep = self.get_execution_profile(graphson, traversal=True)
        ep = self.session.execution_profile_clone_update(ep, continuous_paging_options=None)
        self._setup_data(schema, graphson)
        self.session.default_fetch_size = 10
        g = DseGraph.traversal_source(self.session, execution_profile=ep)
        results = g.V().toList()
        self.assertEqual(len(results), 100)  # 100 results since paging is disabled

    def _test_cont_paging_with_custom_options(self, schema, graphson):
        """
        Test that we can specify custom paging options.

        @jira_ticket PYTHON-1045
        @expected_result we get only the desired number of results

        @test_category dse graph
        """
        ep = self.get_execution_profile(graphson, traversal=True)
        ep = self.session.execution_profile_clone_update(ep,
                                                         continuous_paging_options=ContinuousPagingOptions(max_pages=1))
        self._setup_data(schema, graphson)
        self.session.default_fetch_size = 10
        g = DseGraph.traversal_source(self.session, execution_profile=ep)
        results = g.V().toList()
        self.assertEqual(len(results), 10)  # only 10 results since paging is disabled
