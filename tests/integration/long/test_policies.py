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

from cassandra import ConsistencyLevel, Unavailable
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT

from tests.integration import use_cluster, get_cluster, get_node, TestCluster


def setup_module():
    use_cluster('test_cluster', [4])


class RetryPolicyTests(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        cluster = get_cluster()
        cluster.start(wait_for_binary_proto=True)  # make sure other nodes are restarted

    def test_should_rethrow_on_unvailable_with_default_policy_if_cas(self):
        """
        Tests for the default retry policy in combination with lightweight transactions.

        @since 3.17
        @jira_ticket PYTHON-1007
        @expected_result the query is retried with the default CL, not the serial one.

        @test_category policy
        """
        ep = ExecutionProfile(consistency_level=ConsistencyLevel.ALL,
                              serial_consistency_level=ConsistencyLevel.SERIAL)

        cluster = TestCluster(execution_profiles={EXEC_PROFILE_DEFAULT: ep})
        session = cluster.connect()

        session.execute("CREATE KEYSPACE test_retry_policy_cas WITH replication = {'class':'SimpleStrategy','replication_factor': 3};")
        session.execute("CREATE TABLE test_retry_policy_cas.t (id int PRIMARY KEY, data text);")
        session.execute('INSERT INTO test_retry_policy_cas.t ("id", "data") VALUES (%(0)s, %(1)s)', {'0': 42, '1': 'testing'})

        get_node(2).stop()
        get_node(4).stop()

        # before fix: cassandra.InvalidRequest: Error from server: code=2200 [Invalid query] message="SERIAL is not
        # supported as conditional update commit consistency. ....""

        # after fix: cassandra.Unavailable (expected since replicas are down)
        with self.assertRaises(Unavailable) as cm:
            session.execute("update test_retry_policy_cas.t set data = 'staging' where id = 42 if data ='testing'")

        exception = cm.exception
        self.assertEqual(exception.consistency, ConsistencyLevel.SERIAL)
        self.assertEqual(exception.required_replicas, 2)
        self.assertEqual(exception.alive_replicas, 1)
