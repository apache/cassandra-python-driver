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

from cassandra.protocol import ProtocolHandler, ResultMessage, QueryMessage, UUIDType, read_int
from cassandra.query import tuple_factory, SimpleStatement
from cassandra.cluster import (ResponseFuture, ExecutionProfile, EXEC_PROFILE_DEFAULT,
    ContinuousPagingOptions, NoHostAvailable)
from cassandra import ProtocolVersion, ConsistencyLevel

from tests.integration import use_singledc, drop_keyspace_shutdown_cluster, \
    greaterthanorequalcass30, execute_with_long_wait_retry, greaterthanorequaldse51, greaterthanorequalcass3_10, \
    TestCluster, greaterthanorequalcass40, requirecassandra
from tests.integration.datatype_utils import update_datatypes, PRIMITIVE_DATATYPES
from tests.integration.standard.utils import create_table_with_all_types, get_all_primitive_params

import uuid
from unittest import mock


def setup_module():
    use_singledc()
    update_datatypes()


class CustomProtocolHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cluster = TestCluster()
        cls.session = cls.cluster.connect()
        cls.session.execute("CREATE KEYSPACE custserdes WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1'}")
        cls.session.set_keyspace("custserdes")

    @classmethod
    def tearDownClass(cls):
        drop_keyspace_shutdown_cluster("custserdes", cls.session, cls.cluster)

    def test_custom_raw_uuid_row_results(self):
        """
        Test to validate that custom protocol handlers work with raw row results

        Connect and validate that the normal protocol handler is used.
        Re-Connect and validate that the custom protocol handler is used.
        Re-Connect and validate that the normal protocol handler is used.

        @since 2.7
        @jira_ticket PYTHON-313
        @expected_result custom protocol handler is invoked appropriately.

        @test_category data_types:serialization
        """

        # Ensure that we get normal uuid back first
        cluster = TestCluster(
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(row_factory=tuple_factory)}
        )
        session = cluster.connect(keyspace="custserdes")

        result = session.execute("SELECT schema_version FROM system.local")
        uuid_type = result[0][0]
        self.assertEqual(type(uuid_type), uuid.UUID)

        # use our custom protocol handler
        session.client_protocol_handler = CustomTestRawRowType
        result_set = session.execute("SELECT schema_version FROM system.local")
        raw_value = result_set[0][0]
        self.assertTrue(isinstance(raw_value, bytes))
        self.assertEqual(len(raw_value), 16)

        # Ensure that we get normal uuid back when we re-connect
        session.client_protocol_handler = ProtocolHandler
        result_set = session.execute("SELECT schema_version FROM system.local")
        uuid_type = result_set[0][0]
        self.assertEqual(type(uuid_type), uuid.UUID)
        cluster.shutdown()

    def test_custom_raw_row_results_all_types(self):
        """
        Test to validate that custom protocol handlers work with varying types of
        results

        Connect, create a table with all sorts of data. Query the data, make the sure the custom results handler is
        used correctly.

        @since 2.7
        @jira_ticket PYTHON-313
        @expected_result custom protocol handler is invoked with various result types

        @test_category data_types:serialization
        """
        # Connect using a custom protocol handler that tracks the various types the result message is used with.
        cluster = TestCluster(
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(row_factory=tuple_factory)}
        )
        session = cluster.connect(keyspace="custserdes")
        session.client_protocol_handler = CustomProtocolHandlerResultMessageTracked

        colnames = create_table_with_all_types("alltypes", session, 1)
        columns_string = ", ".join(colnames)

        # verify data
        params = get_all_primitive_params(0)
        results = session.execute("SELECT {0} FROM alltypes WHERE primkey=0".format(columns_string))[0]
        for expected, actual in zip(params, results):
            self.assertEqual(actual, expected)
        # Ensure we have covered the various primitive types
        self.assertEqual(len(CustomResultMessageTracked.checked_rev_row_set), len(PRIMITIVE_DATATYPES)-1)
        cluster.shutdown()

    @requirecassandra
    @greaterthanorequalcass40
    def test_protocol_divergence_v5_fail_by_continuous_paging(self):
        """
        Test to validate that V5 and DSE_V1 diverge. ContinuousPagingOptions is not supported by V5

        @since DSE 2.0b3 GRAPH 1.0b1
        @jira_ticket PYTHON-694
        @expected_result NoHostAvailable will be risen when the continuous_paging_options parameter is set

        @test_category connection
        """
        cluster = TestCluster(protocol_version=ProtocolVersion.V5, allow_beta_protocol_version=True)
        session = cluster.connect()

        max_pages = 4
        max_pages_per_second = 3
        continuous_paging_options = ContinuousPagingOptions(max_pages=max_pages,
                                                            max_pages_per_second=max_pages_per_second)

        future = self._send_query_message(session, timeout=session.default_timeout,
                                        consistency_level=ConsistencyLevel.ONE,
                            continuous_paging_options=continuous_paging_options)

        # This should raise NoHostAvailable because continuous paging is not supported under ProtocolVersion.DSE_V1
        with self.assertRaises(NoHostAvailable) as context:
            future.result()
        self.assertIn("Continuous paging may only be used with protocol version ProtocolVersion.DSE_V1 or higher",
                    str(context.exception))

        cluster.shutdown()

    @greaterthanorequalcass30
    def test_protocol_divergence_v4_fail_by_flag_uses_int(self):
        """
        Test to validate that the _PAGE_SIZE_FLAG is not treated correctly in V4 if the flags are
        written using write_uint instead of write_int

        @since 3.9
        @jira_ticket PYTHON-713
        @expected_result the fetch_size=1 parameter will be ignored

        @test_category connection
        """
        self._protocol_divergence_fail_by_flag_uses_int(ProtocolVersion.V4, uses_int_query_flag=False,
                                                        int_flag=True)

    @requirecassandra
    @greaterthanorequalcass40
    def test_protocol_v5_uses_flag_int(self):
        """
        Test to validate that the _PAGE_SIZE_FLAG is treated correctly using write_uint for V5

        @jira_ticket PYTHON-694
        @expected_result the fetch_size=1 parameter will be honored

        @test_category connection
        """
        self._protocol_divergence_fail_by_flag_uses_int(ProtocolVersion.V5, uses_int_query_flag=True, beta=True,
                                                        int_flag=True)

    @greaterthanorequaldse51
    def test_protocol_dsev1_uses_flag_int(self):
        """
        Test to validate that the _PAGE_SIZE_FLAG is treated correctly using write_uint for DSE_V1

        @jira_ticket PYTHON-694
        @expected_result the fetch_size=1 parameter will be honored

        @test_category connection
        """
        self._protocol_divergence_fail_by_flag_uses_int(ProtocolVersion.DSE_V1, uses_int_query_flag=True,
                                                        int_flag=True)

    @requirecassandra
    @greaterthanorequalcass40
    def test_protocol_divergence_v5_fail_by_flag_uses_int(self):
        """
        Test to validate that the _PAGE_SIZE_FLAG is treated correctly using write_uint for V5

        @jira_ticket PYTHON-694
        @expected_result the fetch_size=1 parameter will be honored

        @test_category connection
        """
        self._protocol_divergence_fail_by_flag_uses_int(ProtocolVersion.V5, uses_int_query_flag=False, beta=True,
                                                        int_flag=False)

    @greaterthanorequaldse51
    def test_protocol_divergence_dsev1_fail_by_flag_uses_int(self):
        """
        Test to validate that the _PAGE_SIZE_FLAG is treated correctly using write_uint for DSE_V1

        @jira_ticket PYTHON-694
        @expected_result the fetch_size=1 parameter will be honored

        @test_category connection
        """
        self._protocol_divergence_fail_by_flag_uses_int(ProtocolVersion.DSE_V1, uses_int_query_flag=False,
                                                        int_flag=False)

    def _send_query_message(self, session, timeout, **kwargs):
        query = "SELECT * FROM test3rf.test"
        message = QueryMessage(query=query, **kwargs)
        future = ResponseFuture(session, message, query=None, timeout=timeout)
        future.send_request()
        return future

    def _protocol_divergence_fail_by_flag_uses_int(self, version, uses_int_query_flag, int_flag = True, beta=False):
        cluster = TestCluster(protocol_version=version, allow_beta_protocol_version=beta)
        session = cluster.connect()

        query_one = SimpleStatement("INSERT INTO test3rf.test (k, v) VALUES (1, 1)")
        query_two = SimpleStatement("INSERT INTO test3rf.test (k, v) VALUES (2, 2)")

        execute_with_long_wait_retry(session, query_one)
        execute_with_long_wait_retry(session, query_two)

        with mock.patch('cassandra.protocol.ProtocolVersion.uses_int_query_flags', new=mock.Mock(return_value=int_flag)):
            future = self._send_query_message(session, 10,
                                              consistency_level=ConsistencyLevel.ONE, fetch_size=1)

            response = future.result()

            # This means the flag are not handled as they are meant by the server if uses_int=False
            self.assertEqual(response.has_more_pages, uses_int_query_flag)

        execute_with_long_wait_retry(session, SimpleStatement("TRUNCATE test3rf.test"))
        cluster.shutdown()


class CustomResultMessageRaw(ResultMessage):
    """
    This is a custom Result Message that is used to return raw results, rather then
    results which contain objects.
    """
    my_type_codes = ResultMessage.type_codes.copy()
    my_type_codes[0xc] = UUIDType
    type_codes = my_type_codes

    def recv_results_rows(self, f, protocol_version, user_type_map, result_metadata, column_encryption_policy):
            self.recv_results_metadata(f, user_type_map)
            column_metadata = self.column_metadata or result_metadata
            rowcount = read_int(f)
            self.parsed_rows = [self.recv_row(f, len(column_metadata)) for _ in range(rowcount)]
            self.column_names = [c[2] for c in column_metadata]
            self.column_types = [c[3] for c in column_metadata]


class CustomTestRawRowType(ProtocolHandler):
    """
    This is a custom protocol handler that will substitute the
    customResultMesageRowRaw Result message for our own implementation
    """
    my_opcodes = ProtocolHandler.message_types_by_opcode.copy()
    my_opcodes[CustomResultMessageRaw.opcode] = CustomResultMessageRaw
    message_types_by_opcode = my_opcodes


class CustomResultMessageTracked(ResultMessage):
    """
    This is a custom Result Message that is used to track what primitive types
    have been processed when it receives results
    """
    my_type_codes = ResultMessage.type_codes.copy()
    my_type_codes[0xc] = UUIDType
    type_codes = my_type_codes
    checked_rev_row_set = set()

    def recv_results_rows(self, f, protocol_version, user_type_map, result_metadata, column_encryption_policy):
        self.recv_results_metadata(f, user_type_map)
        column_metadata = self.column_metadata or result_metadata
        rowcount = read_int(f)
        rows = [self.recv_row(f, len(column_metadata)) for _ in range(rowcount)]
        self.column_names = [c[2] for c in column_metadata]
        self.column_types = [c[3] for c in column_metadata]
        self.checked_rev_row_set.update(self.column_types)
        self.parsed_rows = [
            tuple(ctype.from_binary(val, protocol_version)
                  for ctype, val in zip(self.column_types, row))
            for row in rows]


class CustomProtocolHandlerResultMessageTracked(ProtocolHandler):
    """
    This is a custom protocol handler that will substitute the
    CustomTestRawRowTypeTracked Result message for our own implementation
    """
    my_opcodes = ProtocolHandler.message_types_by_opcode.copy()
    my_opcodes[CustomResultMessageTracked.opcode] = CustomResultMessageTracked
    message_types_by_opcode = my_opcodes
