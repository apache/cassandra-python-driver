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

import logging

from cassandra.cluster import EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT
from cassandra.graph import SimpleGraphStatement
from tests.integration import DSE_VERSION, requiredse
from tests.integration.advanced import use_singledc_wth_graph_and_spark, find_spark_master
from tests.integration.advanced.graph import BasicGraphUnitTestCase, ClassicGraphFixtures
log = logging.getLogger(__name__)


def setup_module():
    if DSE_VERSION:
        use_singledc_wth_graph_and_spark()


@requiredse
class SparkLBTests(BasicGraphUnitTestCase):
    """
    Test to validate that analytics query can run in a multi-node environment. Also check to ensure
    that the master spark node is correctly targeted when OLAP queries are run

    @since 3.20
    @jira_ticket PYTHON-510
    @expected_result OLAP results should come back correctly, master spark coordinator should always be picked.
    @test_category dse graph
    """
    def test_spark_analytic_query(self):
        self.session.execute_graph(ClassicGraphFixtures.classic())
        spark_master = find_spark_master(self.session)

        # Run multiple times to ensure we don't round-robin
        for i in range(3):
            to_run = SimpleGraphStatement("g.V().count()")
            rs = self.session.execute_graph(to_run, execution_profile=EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT)
            self.assertEqual(rs[0].value, 7)
            self.assertEqual(rs.response_future._current_host.address, spark_master)
