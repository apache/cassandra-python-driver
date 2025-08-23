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

from cassandra.query import BatchStatement

from tests.integration import use_singledc, PROTOCOL_VERSION, local, TestCluster


def setup_module():
    use_singledc()


class ClientWarningTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if PROTOCOL_VERSION < 4:
            return

        cls.cluster = TestCluster()
        cls.session = cls.cluster.connect()

        cls.session.execute("CREATE TABLE IF NOT EXISTS test1rf.client_warning (k int, v0 int, v1 int, PRIMARY KEY (k, v0))")
        cls.prepared = cls.session.prepare("INSERT INTO test1rf.client_warning (k, v0, v1) VALUES (?, ?, ?)")

        cls.warn_batch = BatchStatement()
        # 213 = 5 * 1024 / (4+4 + 4+4 + 4+4)
        #        thresh_kb/ (min param size)
        for x in range(214):
            cls.warn_batch.add(cls.prepared, (x, x, 1))

    @classmethod
    def tearDownClass(cls):
        if PROTOCOL_VERSION < 4:
            return

        cls.cluster.shutdown()

    def setUp(self):
        if PROTOCOL_VERSION < 4:
            raise unittest.SkipTest(
                "Native protocol 4,0+ is required for client warnings, currently using %r"
                % (PROTOCOL_VERSION,))

    def test_warning_basic(self):
        """
        Test to validate that client warnings can be surfaced

        @since 2.6.0
        @jira_ticket PYTHON-315
        @expected_result valid warnings returned
        @test_assumptions
            - batch_size_warn_threshold_in_kb: 5
        @test_category queries:client_warning
        """
        future = self.session.execute_async(self.warn_batch)
        future.result()
        self.assertEqual(len(future.warnings), 1)
        self.assertRegex(future.warnings[0], 'Batch.*exceeding.*')

    def test_warning_with_trace(self):
        """
        Test to validate client warning with tracing

        @since 2.6.0
        @jira_ticket PYTHON-315
        @expected_result valid warnings returned
        @test_assumptions
            - batch_size_warn_threshold_in_kb: 5
        @test_category queries:client_warning
        """
        future = self.session.execute_async(self.warn_batch, trace=True)
        future.result()
        self.assertEqual(len(future.warnings), 1)
        self.assertRegex(future.warnings[0], 'Batch.*exceeding.*')
        self.assertIsNotNone(future.get_query_trace())

    @local
    def test_warning_with_custom_payload(self):
        """
        Test to validate client warning with custom payload

        @since 2.6.0
        @jira_ticket PYTHON-315
        @expected_result valid warnings returned
        @test_assumptions
            - batch_size_warn_threshold_in_kb: 5
        @test_category queries:client_warning
        """
        payload = {'key': b'value'}
        future = self.session.execute_async(self.warn_batch, custom_payload=payload)
        future.result()
        self.assertEqual(len(future.warnings), 1)
        self.assertRegex(future.warnings[0], 'Batch.*exceeding.*')
        self.assertDictEqual(future.custom_payload, payload)

    @local
    def test_warning_with_trace_and_custom_payload(self):
        """
        Test to validate client warning with tracing and client warning

        @since 2.6.0
        @jira_ticket PYTHON-315
        @expected_result valid warnings returned
        @test_assumptions
            - batch_size_warn_threshold_in_kb: 5
        @test_category queries:client_warning
        """
        payload = {'key': b'value'}
        future = self.session.execute_async(self.warn_batch, trace=True, custom_payload=payload)
        future.result()
        self.assertEqual(len(future.warnings), 1)
        self.assertRegex(future.warnings[0], 'Batch.*exceeding.*')
        self.assertIsNotNone(future.get_query_trace())
        self.assertDictEqual(future.custom_payload, payload)
