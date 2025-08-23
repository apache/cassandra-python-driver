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

from concurrent.futures import Future
from cassandra.datastax.graph.fluent import DseGraph

from tests.integration import requiredse, DSE_VERSION
from tests.integration.advanced import use_single_node_with_graph
from tests.integration.advanced.graph import GraphTestConfiguration
from tests.integration.advanced.graph.fluent import (
    BaseImplicitExecutionTest, create_traversal_profiles, _AbstractTraversalTest)


def setup_module():
    if DSE_VERSION:
        dse_options = {'graph': {'realtime_evaluation_timeout_in_seconds': 60}}
        use_single_node_with_graph(dse_options=dse_options)


@requiredse
@GraphTestConfiguration.generate_tests(traversal=True)
class ImplicitExecutionTest(BaseImplicitExecutionTest, _AbstractTraversalTest):
    def _test_iterate_step(self, schema, graphson):
        """
        Test to validate that the iterate() step work on all dse versions.
        @jira_ticket PYTHON-1155
        @expected_result iterate step works
        @test_category dse graph
        """

        g = self.fetch_traversal_source(graphson)
        self.execute_graph(schema.fixtures.classic(), graphson)
        g.addV('person').property('name', 'Person1').iterate()


@requiredse
@GraphTestConfiguration.generate_tests(traversal=True)
class ImplicitAsyncExecutionTest(BaseImplicitExecutionTest):
    """
    Test to validate that the traversal async execution works properly.

    @since 3.21.0
    @jira_ticket PYTHON-1129

    @test_category dse graph
    """

    def setUp(self):
        super(ImplicitAsyncExecutionTest, self).setUp()
        self.ep_graphson2, self.ep_graphson3 = create_traversal_profiles(self.cluster, self.graph_name)

    def _validate_results(self, results):
        results = list(results)
        self.assertEqual(len(results), 2)
        self.assertIn('vadas', results)
        self.assertIn('josh', results)

    def _test_promise(self, schema, graphson):
        self.execute_graph(schema.fixtures.classic(), graphson)
        g = self.fetch_traversal_source(graphson)
        traversal_future = g.V().has('name', 'marko').out('knows').values('name').promise()
        self._validate_results(traversal_future.result())

    def _test_promise_error_is_propagated(self, schema, graphson):
        self.execute_graph(schema.fixtures.classic(), graphson)
        g = DseGraph().traversal_source(self.session, 'wrong_graph', execution_profile=self.ep)
        traversal_future = g.V().has('name', 'marko').out('knows').values('name').promise()
        with self.assertRaises(Exception):
            traversal_future.result()

    def _test_promise_callback(self, schema, graphson):
        self.execute_graph(schema.fixtures.classic(), graphson)
        g = self.fetch_traversal_source(graphson)
        future = Future()

        def cb(f):
            future.set_result(f.result())

        traversal_future = g.V().has('name', 'marko').out('knows').values('name').promise()
        traversal_future.add_done_callback(cb)
        self._validate_results(future.result())

    def _test_promise_callback_on_error(self, schema, graphson):
        self.execute_graph(schema.fixtures.classic(), graphson)
        g = DseGraph().traversal_source(self.session, 'wrong_graph', execution_profile=self.ep)
        future = Future()

        def cb(f):
            try:
                f.result()
            except Exception as e:
                future.set_exception(e)

        traversal_future = g.V().has('name', 'marko').out('knows').values('name').promise()
        traversal_future.add_done_callback(cb)
        with self.assertRaises(Exception):
            future.result()
