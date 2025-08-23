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

from cassandra.cluster import ContinuousPagingOptions

from tests.integration import greaterthanorequaldse68
from tests.integration.advanced.graph import GraphUnitTestCase, CoreGraphSchema, GraphTestConfiguration


@greaterthanorequaldse68
@GraphTestConfiguration.generate_tests(schema=CoreGraphSchema)
class GraphPagingTest(GraphUnitTestCase):

    def _setup_data(self, schema, graphson):
        self.execute_graph("schema.vertexLabel('person').ifNotExists().partitionBy('name', Text).property('age', Int).create();", graphson)
        for i in range(100):
            self.execute_graph("g.addV('person').property('name', 'batman-{}')".format(i), graphson)

    def _test_cont_paging_is_enabled_by_default(self, schema, graphson):
        """
        Test that graph paging is automatically enabled with a >=6.8 cluster.

        @jira_ticket PYTHON-1045
        @expected_result the response future has a continuous_paging_session since graph paging is enabled

        @test_category dse graph
        """
        ep = self.get_execution_profile(graphson)
        self._setup_data(schema, graphson)
        rf = self.session.execute_graph_async("g.V()", execution_profile=ep)
        results = list(rf.result())
        self.assertIsNotNone(rf._continuous_paging_session)
        self.assertEqual(len(results), 100)

    def _test_cont_paging_can_be_disabled(self, schema, graphson):
        """
        Test that graph paging can be disabled.

        @jira_ticket PYTHON-1045
        @expected_result the response future doesn't have a continuous_paging_session since graph paging is disabled

        @test_category dse graph
        """
        ep = self.get_execution_profile(graphson)
        new_ep = self.session.execution_profile_clone_update(ep, continuous_paging_options=None)
        self._setup_data(schema, graphson)
        rf = self.session.execute_graph_async("g.V()", execution_profile=new_ep)
        results = list(rf.result())
        self.assertIsNone(rf._continuous_paging_session)
        self.assertEqual(len(results), 100)

    def _test_cont_paging_with_custom_options(self, schema, graphson):
        """
        Test that we can specify custom paging options.

        @jira_ticket PYTHON-1045
        @expected_result we get only the desired number of results

        @test_category dse graph
        """
        ep = self.get_execution_profile(graphson)
        new_ep = self.session.execution_profile_clone_update(
            ep, continuous_paging_options=ContinuousPagingOptions(max_pages=1))
        self._setup_data(schema, graphson)
        self.session.default_fetch_size = 10
        results = list(self.session.execute_graph("g.V()", execution_profile=new_ep))
        self.assertEqual(len(results), 10)
