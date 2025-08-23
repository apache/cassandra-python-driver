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

import re

from cassandra import OperationTimedOut, InvalidRequest
from cassandra.protocol import SyntaxException
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.cluster import NoHostAvailable
from cassandra.cluster import EXEC_PROFILE_GRAPH_DEFAULT, GraphExecutionProfile
from cassandra.graph import single_object_row_factory, Vertex, graph_object_row_factory, \
    graph_graphson2_row_factory, graph_graphson3_row_factory
from cassandra.util import SortedSet

from tests.integration import DSE_VERSION, greaterthanorequaldse51, greaterthanorequaldse68, \
    requiredse, TestCluster
from tests.integration.advanced.graph import BasicGraphUnitTestCase, GraphUnitTestCase, \
    GraphProtocol, ClassicGraphSchema, CoreGraphSchema, use_single_node_with_graph


def setup_module():
    if DSE_VERSION:
        dse_options = {'graph': {'realtime_evaluation_timeout_in_seconds': 60}}
        use_single_node_with_graph(dse_options=dse_options)


@requiredse
class GraphTimeoutTests(BasicGraphUnitTestCase):

    def test_should_wait_indefinitely_by_default(self):
        """
        Tests that by default the client should wait indefinitely for server timeouts

        @since 1.0.0
        @jira_ticket PYTHON-589

        @test_category dse graph
        """
        desired_timeout = 1000

        graph_source = "test_timeout_1"
        ep_name = graph_source
        ep = self.session.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT)
        ep.graph_options = ep.graph_options.copy()
        ep.graph_options.graph_source = graph_source
        self.cluster.add_execution_profile(ep_name, ep)

        to_run = '''graph.schema().config().option("graph.traversal_sources.{0}.evaluation_timeout").set('{1} ms')'''.format(
            graph_source, desired_timeout)
        self.session.execute_graph(to_run, execution_profile=ep_name)
        with self.assertRaises(InvalidRequest) as ir:
            self.session.execute_graph("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(35000L);1+1",
                                       execution_profile=ep_name)
        self.assertTrue("evaluation exceeded the configured threshold of 1000" in str(ir.exception) or
                        "evaluation exceeded the configured threshold of evaluation_timeout at 1000" in str(
            ir.exception))

    def test_request_timeout_less_then_server(self):
        """
        Tests that with explicit request_timeouts set, that a server timeout is honored if it's relieved prior to the
        client timeout

        @since 1.0.0
        @jira_ticket PYTHON-589

        @test_category dse graph
        """
        desired_timeout = 1000
        graph_source = "test_timeout_2"
        ep_name = graph_source
        ep = self.session.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT, request_timeout=32)
        ep.graph_options = ep.graph_options.copy()
        ep.graph_options.graph_source = graph_source
        self.cluster.add_execution_profile(ep_name, ep)

        to_run = '''graph.schema().config().option("graph.traversal_sources.{0}.evaluation_timeout").set('{1} ms')'''.format(
            graph_source, desired_timeout)
        self.session.execute_graph(to_run, execution_profile=ep_name)
        with self.assertRaises(InvalidRequest) as ir:
            self.session.execute_graph("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(35000L);1+1",
                                       execution_profile=ep_name)
        self.assertTrue("evaluation exceeded the configured threshold of 1000" in str(ir.exception) or
                        "evaluation exceeded the configured threshold of evaluation_timeout at 1000" in str(
            ir.exception))

    def test_server_timeout_less_then_request(self):
        """
        Tests that with explicit request_timeouts set, that a client timeout is honored if it's triggered prior to the
        server sending a timeout.

        @since 1.0.0
        @jira_ticket PYTHON-589

        @test_category dse graph
        """
        graph_source = "test_timeout_3"
        ep_name = graph_source
        ep = self.session.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT, request_timeout=1)
        ep.graph_options = ep.graph_options.copy()
        ep.graph_options.graph_source = graph_source
        self.cluster.add_execution_profile(ep_name, ep)
        server_timeout = 10000
        to_run = '''graph.schema().config().option("graph.traversal_sources.{0}.evaluation_timeout").set('{1} ms')'''.format(
            graph_source, server_timeout)
        self.session.execute_graph(to_run, execution_profile=ep_name)

        with self.assertRaises(Exception) as e:
            self.session.execute_graph("java.util.concurrent.TimeUnit.MILLISECONDS.sleep(35000L);1+1",
                                       execution_profile=ep_name)
            self.assertTrue(isinstance(e, InvalidRequest) or isinstance(e, OperationTimedOut))


@requiredse
class GraphProfileTests(BasicGraphUnitTestCase):
    def test_graph_profile(self):
        """
        Test verifying various aspects of graph config properties.

        @since 1.0.0
        @jira_ticket PYTHON-570

        @test_category dse graph
        """
        hosts = self.cluster.metadata.all_hosts()
        first_host = hosts[0].address
        second_hosts = "1.2.3.4"

        self._execute(ClassicGraphSchema.fixtures.classic(), graphson=GraphProtocol.GRAPHSON_1_0)
        # Create various execution policies
        exec_dif_factory = GraphExecutionProfile(row_factory=single_object_row_factory)
        exec_dif_factory.graph_options.graph_name = self.graph_name
        exec_dif_lbp = GraphExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([first_host]))
        exec_dif_lbp.graph_options.graph_name = self.graph_name
        exec_bad_lbp = GraphExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([second_hosts]))
        exec_dif_lbp.graph_options.graph_name = self.graph_name
        exec_short_timeout = GraphExecutionProfile(request_timeout=1,
                                                   load_balancing_policy=WhiteListRoundRobinPolicy([first_host]))
        exec_short_timeout.graph_options.graph_name = self.graph_name

        # Add a single execution policy on cluster creation
        local_cluster = TestCluster(execution_profiles={"exec_dif_factory": exec_dif_factory})
        local_session = local_cluster.connect()
        self.addCleanup(local_cluster.shutdown)

        rs1 = self.session.execute_graph('g.V()')
        rs2 = local_session.execute_graph('g.V()', execution_profile='exec_dif_factory')

        # Verify default and non default policy works
        self.assertFalse(isinstance(rs2[0], Vertex))
        self.assertTrue(isinstance(rs1[0], Vertex))
        # Add other policies validate that lbp are honored
        local_cluster.add_execution_profile("exec_dif_ldp", exec_dif_lbp)
        local_session.execute_graph('g.V()', execution_profile="exec_dif_ldp")
        local_cluster.add_execution_profile("exec_bad_lbp", exec_bad_lbp)
        with self.assertRaises(NoHostAvailable):
            local_session.execute_graph('g.V()', execution_profile="exec_bad_lbp")

        # Try with missing EP
        with self.assertRaises(ValueError):
            local_session.execute_graph('g.V()', execution_profile='bad_exec_profile')

        # Validate that timeout is honored
        local_cluster.add_execution_profile("exec_short_timeout", exec_short_timeout)
        with self.assertRaises(Exception) as e:
            self.assertTrue(isinstance(e, InvalidRequest) or isinstance(e, OperationTimedOut))
            local_session.execute_graph('java.util.concurrent.TimeUnit.MILLISECONDS.sleep(2000L);',
                                        execution_profile='exec_short_timeout')


@requiredse
class GraphMetadataTest(BasicGraphUnitTestCase):

    @greaterthanorequaldse51
    def test_dse_workloads(self):
        """
        Test to ensure dse_workloads is populated appropriately.
        Field added in DSE 5.1

        @since DSE 2.0
        @jira_ticket PYTHON-667
        @expected_result dse_workloads set is set on host model

        @test_category metadata
        """
        for host in self.cluster.metadata.all_hosts():
            self.assertIsInstance(host.dse_workloads, SortedSet)
            self.assertIn("Cassandra", host.dse_workloads)
            self.assertIn("Graph", host.dse_workloads)


@requiredse
class GraphExecutionProfileOptionsResolveTest(GraphUnitTestCase):
    """
    Test that the execution profile options are properly resolved for graph queries.

    @since DSE 6.8
    @jira_ticket PYTHON-1004 PYTHON-1056
    @expected_result execution profile options are properly determined following the rules.
    """

    def test_default_options(self):
        ep = self.session.get_execution_profile(EXEC_PROFILE_GRAPH_DEFAULT)
        self.assertEqual(ep.graph_options.graph_protocol, None)
        self.assertEqual(ep.row_factory, None)
        self.session._resolve_execution_profile_options(ep)
        self.assertEqual(ep.graph_options.graph_protocol, GraphProtocol.GRAPHSON_1_0)
        self.assertEqual(ep.row_factory, graph_object_row_factory)

    def test_default_options_when_not_groovy(self):
        ep = self.session.get_execution_profile(EXEC_PROFILE_GRAPH_DEFAULT)
        self.assertEqual(ep.graph_options.graph_protocol, None)
        self.assertEqual(ep.row_factory, None)
        ep.graph_options.graph_language = 'whatever'
        self.session._resolve_execution_profile_options(ep)
        self.assertEqual(ep.graph_options.graph_protocol, GraphProtocol.GRAPHSON_2_0)
        self.assertEqual(ep.row_factory, graph_graphson2_row_factory)

    def test_default_options_when_explicitly_specified(self):
        ep = self.session.get_execution_profile(EXEC_PROFILE_GRAPH_DEFAULT)
        self.assertEqual(ep.graph_options.graph_protocol, None)
        self.assertEqual(ep.row_factory, None)
        obj = object()
        ep.graph_options.graph_protocol = obj
        ep.row_factory = obj
        self.session._resolve_execution_profile_options(ep)
        self.assertEqual(ep.graph_options.graph_protocol, obj)
        self.assertEqual(ep.row_factory, obj)

    @greaterthanorequaldse68
    def test_graph_protocol_default_for_core_is_graphson3(self):
        """Test that graphson3 is automatically resolved for a core graph query"""
        self.setup_graph(CoreGraphSchema)
        ep = self.session.get_execution_profile(EXEC_PROFILE_GRAPH_DEFAULT)
        self.assertEqual(ep.graph_options.graph_protocol, None)
        self.assertEqual(ep.row_factory, None)
        # Ensure we have the graph metadata
        self.session.cluster.refresh_schema_metadata()
        self.session._resolve_execution_profile_options(ep)
        self.assertEqual(ep.graph_options.graph_protocol, GraphProtocol.GRAPHSON_3_0)
        self.assertEqual(ep.row_factory, graph_graphson3_row_factory)

        self.execute_graph_queries(CoreGraphSchema.fixtures.classic(), verify_graphson=GraphProtocol.GRAPHSON_3_0)

    @greaterthanorequaldse68
    def test_graph_protocol_default_for_core_fallback_to_graphson1_if_no_graph_name(self):
        """Test that graphson1 is set when we cannot detect if it's a core graph"""
        self.setup_graph(CoreGraphSchema)
        default_ep = self.session.get_execution_profile(EXEC_PROFILE_GRAPH_DEFAULT)
        graph_options = default_ep.graph_options.copy()
        graph_options.graph_name = None
        ep = self.session.execution_profile_clone_update(EXEC_PROFILE_GRAPH_DEFAULT, graph_options=graph_options)
        self.session._resolve_execution_profile_options(ep)
        self.assertEqual(ep.graph_options.graph_protocol, GraphProtocol.GRAPHSON_1_0)
        self.assertEqual(ep.row_factory, graph_object_row_factory)

        regex = re.compile(".*Variable.*is unknown.*", re.S)
        with self.assertRaisesRegex(SyntaxException, regex):
            self.execute_graph_queries(CoreGraphSchema.fixtures.classic(),
                                       execution_profile=ep, verify_graphson=GraphProtocol.GRAPHSON_1_0)
