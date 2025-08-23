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

from cassandra.cluster import GraphExecutionProfile, GraphAnalyticsExecutionProfile
from cassandra.graph import GraphOptions


class GraphExecutionProfileTest(unittest.TestCase):

    def test_graph_source_can_be_set_with_graph_execution_profile(self):
        options = GraphOptions(graph_source='a')
        ep = GraphExecutionProfile(graph_options=options)
        self.assertEqual(ep.graph_options.graph_source, b'a')

    def test_graph_source_is_preserve_with_graph_analytics_execution_profile(self):
        options = GraphOptions(graph_source='doesnt_matter')
        ep = GraphAnalyticsExecutionProfile(graph_options=options)
        self.assertEqual(ep.graph_options.graph_source, b'a')  # graph source is set automatically
