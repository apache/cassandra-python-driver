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

from cassandra.graph import Vertex, Edge

from tests.integration.advanced.graph import (
    validate_classic_vertex, validate_classic_edge, validate_generic_vertex_result_type,
    validate_classic_edge_properties, validate_line_edge,
    validate_generic_edge_result_type, validate_path_result_type)

from tests.integration import requiredse, DSE_VERSION
from tests.integration.advanced import use_single_node_with_graph
from tests.integration.advanced.graph import GraphTestConfiguration
from tests.integration.advanced.graph.fluent import (
    BaseExplicitExecutionTest, _AbstractTraversalTest, _validate_prop)


def setup_module():
    if DSE_VERSION:
        dse_options = {'graph': {'realtime_evaluation_timeout_in_seconds': 60}}
        use_single_node_with_graph(dse_options=dse_options)


@requiredse
@GraphTestConfiguration.generate_tests(traversal=True)
class ExplicitExecutionTest(BaseExplicitExecutionTest, _AbstractTraversalTest):
    """
    This test class will execute all tests of the AbstractTraversalTestClass using Explicit execution
    All queries will be run by converting them to byte code, and calling execute graph explicitly with a generated ep.
    """
    @staticmethod
    def fetch_key_from_prop(property):
        return property.label

    def _validate_classic_vertex(self, g, vertex):
        validate_classic_vertex(self, vertex)

    def _validate_generic_vertex_result_type(self, g, vertex):
        validate_generic_vertex_result_type(self, vertex)

    def _validate_classic_edge_properties(self, g, edge):
        validate_classic_edge_properties(self, edge)

    def _validate_classic_edge(self, g, edge):
        validate_classic_edge(self, edge)

    def _validate_line_edge(self, g, edge):
        validate_line_edge(self, edge)

    def _validate_generic_edge_result_type(self, edge):
        validate_generic_edge_result_type(self, edge)

    def _validate_type(self, g,  vertex):
        for key in vertex.properties:
            value = vertex.properties[key][0].value
            _validate_prop(key, value, self)

    def _validate_path_result_type(self, g, path_obj):
        # This pre-processing is due to a change in TinkerPop
        # properties are not returned automatically anymore
        # with some queries.
        for obj in path_obj.objects:
            if not obj.properties:
                props = []
                if isinstance(obj, Edge):
                    obj.properties = {
                        p.key: p.value
                        for p in self.fetch_edge_props(g, obj)
                    }
                elif isinstance(obj, Vertex):
                    obj.properties = {
                        p.label: p.value
                        for p in self.fetch_vertex_props(g, obj)
                    }

        validate_path_result_type(self, path_obj)

    def _validate_meta_property(self, g, vertex):

        self.assertEqual(len(vertex.properties), 1)
        self.assertEqual(len(vertex.properties['key']), 1)
        p = vertex.properties['key'][0]
        self.assertEqual(p.label, 'key')
        self.assertEqual(p.value, 'meta_prop')
        self.assertEqual(p.properties, {'k0': 'v0', 'k1': 'v1'})
