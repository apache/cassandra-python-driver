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

from cassandra.query import (SimpleStatement, BatchStatement, BatchType)

from tests.integration import use_singledc, PROTOCOL_VERSION, local, TestCluster


def setup_module():
    use_singledc()

#These test rely on the custom payload being returned but by default C*
#ignores all the payloads.


@local
class CustomPayloadTests(unittest.TestCase):

    def setUp(self):
        if PROTOCOL_VERSION < 4:
            raise unittest.SkipTest(
                "Native protocol 4,0+ is required for custom payloads, currently using %r"
                % (PROTOCOL_VERSION,))
        self.cluster = TestCluster()
        self.session = self.cluster.connect()

    def tearDown(self):

        self.cluster.shutdown()

    def test_custom_query_basic(self):
        """
        Test to validate that custom payloads work with simple queries

        creates a simple query and ensures that custom payloads are passed to C*. A custom
        query provider is used with C* so we can validate that same custom payloads are sent back
        with the results


        @since 2.6
        @jira_ticket PYTHON-280
        @expected_result valid custom payloads should be sent and received

        @test_category queries:custom_payload
        """

        # Create a simple query statement a
        query = "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        # Validate that various types of custom payloads are sent and received okay
        self.validate_various_custom_payloads(statement=statement)

    def test_custom_query_batching(self):
        """
        Test to validate that custom payloads work with batch queries

        creates a batch query and ensures that custom payloads are passed to C*. A custom
        query provider is used with C* so we can validate that same custom payloads are sent back
        with the results


        @since 2.6
        @jira_ticket PYTHON-280
        @expected_result valid custom payloads should be sent and received

        @test_category queries:custom_payload
        """

        # Construct Batch Statement
        batch = BatchStatement(BatchType.LOGGED)
        for i in range(10):
            batch.add(SimpleStatement("INSERT INTO test3rf.test (k, v) VALUES (%s, %s)"), (i, i))

        # Validate that various types of custom payloads are sent and received okay
        self.validate_various_custom_payloads(statement=batch)

    def test_custom_query_prepared(self):
        """
        Test to validate that custom payloads work with prepared queries

        creates a batch query and ensures that custom payloads are passed to C*. A custom
        query provider is used with C* so we can validate that same custom payloads are sent back
        with the results


        @since 2.6
        @jira_ticket PYTHON-280
        @expected_result valid custom payloads should be sent and received

        @test_category queries:custom_payload
        """

        # Construct prepared statement
        prepared = self.session.prepare(
            """
            INSERT INTO test3rf.test (k, v) VALUES  (?, ?)
            """)

        bound = prepared.bind((1, None))

        # Validate that various custom payloads are validated correctly
        self.validate_various_custom_payloads(statement=bound)

    def validate_various_custom_payloads(self, statement):
        """
        This is a utility method that given a statement will attempt
        to submit the statement with various custom payloads. It will
        validate that the custom payloads are sent and received correctly.

        @param statement The statement to validate the custom queries in conjunction with
        """

        # Simple key value
        custom_payload = {'test': b'test_return'}
        self.execute_async_validate_custom_payload(statement=statement, custom_payload=custom_payload)

        # no key value
        custom_payload = {'': b''}
        self.execute_async_validate_custom_payload(statement=statement, custom_payload=custom_payload)

        # Space value
        custom_payload = {' ': b' '}
        self.execute_async_validate_custom_payload(statement=statement, custom_payload=custom_payload)

        # Long key value pair
        key_value = "x" * 10
        custom_payload = {key_value: key_value.encode()}
        self.execute_async_validate_custom_payload(statement=statement, custom_payload=custom_payload)

        # Max supported value key pairs according C* binary protocol v4 should be 65534 (unsigned short max value)
        for i in range(65534):
            custom_payload[str(i)] = b'x'
        self.execute_async_validate_custom_payload(statement=statement, custom_payload=custom_payload)

        # Add one custom payload to this is too many key value pairs and should fail
        custom_payload[str(65535)] = b'x'
        with self.assertRaises(ValueError):
            self.execute_async_validate_custom_payload(statement=statement, custom_payload=custom_payload)

    def execute_async_validate_custom_payload(self, statement, custom_payload):
        """
        This is just a simple method that submits a statement with a payload, and validates
        that the custom payload we submitted matches the one that we got back
        @param statement The statement to execute
        @param custom_payload The custom payload to submit with
        """

        # Submit the statement with our custom payload. Validate the one
        # we receive from the server matches
        response_future = self.session.execute_async(statement, custom_payload=custom_payload)
        response_future.result()
        returned_custom_payload = response_future.custom_payload
        self.assertEqual(custom_payload, returned_custom_payload)
