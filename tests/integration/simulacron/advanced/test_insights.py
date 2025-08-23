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
import json
import re

from cassandra.cluster import Cluster
from cassandra.datastax.insights.util import version_supports_insights

from tests.integration import requiressimulacron, requiredse, DSE_VERSION
from tests.integration.simulacron import DseSimulacronCluster, PROTOCOL_VERSION
from tests.integration.simulacron.utils import SimulacronClient, GetLogsQuery, ClearLogsQuery


@requiredse
@requiressimulacron
@unittest.skipUnless(DSE_VERSION and version_supports_insights(str(DSE_VERSION)), 'DSE {} does not support insights'.format(DSE_VERSION))
class InsightsTests(DseSimulacronCluster):
    """
    Tests insights integration

    @since 3.18
    @jira_ticket PYTHON-1047
    @expected_result startup and status messages are sent
    """

    connect = False

    def tearDown(self):
        if self.cluster:
            self.cluster.shutdown()

    @staticmethod
    def _get_node_logs(raw_data):
        return list(filter(lambda q: q['type'] == 'QUERY' and q['query'].startswith('CALL InsightsRpc.reportInsight'),
                           json.loads(raw_data)['data_centers'][0]['nodes'][0]['queries']))

    @staticmethod
    def _parse_data(data, index=0):
        return json.loads(re.match(
                r"CALL InsightsRpc.reportInsight\('(.+)'\)",
                data[index]['frame']['message']['query']).group(1))

    def test_startup_message(self):
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False)
        self.session = self.cluster.connect(wait_for_all_pools=True)

        time.sleep(1) # wait the monitor thread is started
        response = SimulacronClient().submit_request(GetLogsQuery())
        self.assertTrue('CALL InsightsRpc.reportInsight' in response)

        node_queries = self._get_node_logs(response)
        self.assertEqual(1, len(node_queries))
        self.assertTrue(node_queries, "RPC query not found")

        message = self._parse_data(node_queries)

        self.assertEqual(message['metadata']['name'], 'driver.startup')
        self.assertEqual(message['data']['initialControlConnection'],
                         self.cluster.control_connection._connection.host)
        self.assertEqual(message['data']['sessionId'], str(self.session.session_id))
        self.assertEqual(message['data']['clientId'], str(self.cluster.client_id))
        self.assertEqual(message['data']['compression'], 'NONE')

    def test_status_message(self):
        SimulacronClient().submit_request(ClearLogsQuery())

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False, monitor_reporting_interval=1)
        self.session = self.cluster.connect(wait_for_all_pools=True)

        time.sleep(1.1)
        response = SimulacronClient().submit_request(GetLogsQuery())
        self.assertTrue('CALL InsightsRpc.reportInsight' in response)

        node_queries = self._get_node_logs(response)
        self.assertEqual(2, len(node_queries))
        self.assertTrue(node_queries, "RPC query not found")

        message = self._parse_data(node_queries, 1)

        self.assertEqual(message['metadata']['name'], 'driver.status')
        self.assertEqual(message['data']['controlConnection'],
                         self.cluster.control_connection._connection.host)
        self.assertEqual(message['data']['sessionId'], str(self.session.session_id))
        self.assertEqual(message['data']['clientId'], str(self.cluster.client_id))
        self.assertEqual(message['metadata']['insightType'], 'EVENT')

    def test_monitor_disabled(self):
        SimulacronClient().submit_request(ClearLogsQuery())

        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False, monitor_reporting_enabled=False)
        self.session = self.cluster.connect(wait_for_all_pools=True)

        response = SimulacronClient().submit_request(GetLogsQuery())
        self.assertFalse('CALL InsightsRpc.reportInsight' in response)
