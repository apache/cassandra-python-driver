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

from collections import deque
from threading import RLock
from unittest.mock import Mock, MagicMock, ANY

from cassandra import ConsistencyLevel, Unavailable, SchemaTargetType, SchemaChangeType, OperationTimedOut
from cassandra.cluster import Session, ResponseFuture, NoHostAvailable, ProtocolVersion
from cassandra.connection import Connection, ConnectionException
from cassandra.protocol import (ReadTimeoutErrorMessage, WriteTimeoutErrorMessage,
                                UnavailableErrorMessage, ResultMessage, QueryMessage,
                                OverloadedErrorMessage, IsBootstrappingErrorMessage,
                                PreparedQueryNotFound, PrepareMessage,
                                RESULT_KIND_ROWS, RESULT_KIND_SET_KEYSPACE,
                                RESULT_KIND_SCHEMA_CHANGE, RESULT_KIND_PREPARED,
                                ProtocolHandler)
from cassandra.policies import RetryPolicy
from cassandra.pool import NoConnectionsAvailable
from cassandra.query import SimpleStatement


class ResponseFutureTests(unittest.TestCase):

    def make_basic_session(self):
        s = Mock(spec=Session)
        s.row_factory = lambda col_names, rows: [(col_names, rows)]
        return s

    def make_pool(self):
        pool = Mock()
        pool.is_shutdown = False
        pool.borrow_connection.return_value = [Mock(), Mock()]
        return pool

    def make_session(self):
        session = self.make_basic_session()
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1', 'ip2']
        session._pools.get.return_value = self.make_pool()
        return session

    def make_response_future(self, session):
        query = SimpleStatement("SELECT * FROM foo")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)
        return ResponseFuture(session, message, query, 1)

    def make_mock_response(self, col_names, rows):
        return Mock(spec=ResultMessage, kind=RESULT_KIND_ROWS, column_names=col_names, parsed_rows=rows, paging_state=None, col_types=None)

    def test_result_message(self):
        session = self.make_basic_session()
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1', 'ip2']
        pool = session._pools.get.return_value
        pool.is_shutdown = False

        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        rf = self.make_response_future(session)
        rf.send_request()

        rf.session._pools.get.assert_called_once_with('ip1')
        pool.borrow_connection.assert_called_once_with(timeout=ANY)

        connection.send_msg.assert_called_once_with(rf.message, 1, cb=ANY, encoder=ProtocolHandler.encode_message, decoder=ProtocolHandler.decode_message, result_metadata=[])

        expected_result = (object(), object())
        rf._set_result(None, None, None, self.make_mock_response(expected_result[0], expected_result[1]))
        result = rf.result()[0]
        self.assertEqual(result, expected_result)

    def test_unknown_result_class(self):
        session = self.make_session()
        pool = session._pools.get.return_value
        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        rf = self.make_response_future(session)
        rf.send_request()
        rf._set_result(None, None, None, object())
        self.assertRaises(ConnectionException, rf.result)

    def test_set_keyspace_result(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()

        result = Mock(spec=ResultMessage,
                      kind=RESULT_KIND_SET_KEYSPACE,
                      results="keyspace1")
        rf._set_result(None, None, None, result)
        rf._set_keyspace_completed({})
        self.assertFalse(rf.result())

    def test_schema_change_result(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()

        event_results={'target_type': SchemaTargetType.TABLE, 'change_type': SchemaChangeType.CREATED,
                       'keyspace': "keyspace1", "table": "table1"}
        result = Mock(spec=ResultMessage,
                      kind=RESULT_KIND_SCHEMA_CHANGE,
                      schema_change_event=event_results)
        connection = Mock()
        rf._set_result(None, connection, None, result)
        session.submit.assert_called_once_with(ANY, ANY, rf, connection, **event_results)

    def test_other_result_message_kind(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()
        result = Mock(spec=ResultMessage, kind=999, results=[1, 2, 3])
        rf._set_result(None, None, None, result)
        self.assertEqual(rf.result()[0], result)

    def test_heartbeat_defunct_deadlock(self):
        """
        Heartbeat defuncts all connections and clears request queues. Response future times out and even
        if it has been removed from request queue, timeout exception must be thrown. Otherwise event loop
        will deadlock on eventual ResponseFuture.result() call.

        PYTHON-1044
        """

        connection = MagicMock(spec=Connection)
        connection._requests = {}

        pool = Mock()
        pool.is_shutdown = False
        pool.borrow_connection.return_value = [connection, 1]

        session = self.make_basic_session()
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = [Mock(), Mock()]
        session._pools.get.return_value = pool

        query = SimpleStatement("SELECT * FROM foo")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query, 1)
        rf.send_request()

        # Simulate Connection.error_all_requests() after heartbeat defuncts
        connection._requests = {}

        # Simulate ResponseFuture timing out
        rf._on_timeout()
        self.assertRaisesRegex(OperationTimedOut, "Connection defunct by heartbeat", rf.result)

    def test_read_timeout_error_message(self):
        session = self.make_session()
        query = SimpleStatement("SELECT * FROM foo")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query, 1)
        rf.send_request()

        result = Mock(spec=ReadTimeoutErrorMessage, info={"data_retrieved": "", "required_responses":2,
                                                           "received_responses":1, "consistency": 1})
        rf._set_result(None, None, None, result)

        self.assertRaises(Exception, rf.result)

    def test_write_timeout_error_message(self):
        session = self.make_session()
        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query, 1)
        rf.send_request()

        result = Mock(spec=WriteTimeoutErrorMessage, info={"write_type": 1, "required_responses":2,
                                                           "received_responses":1, "consistency": 1})
        rf._set_result(None, None, None, result)
        self.assertRaises(Exception, rf.result)

    def test_unavailable_error_message(self):
        session = self.make_session()
        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        query.retry_policy = Mock()
        query.retry_policy.on_unavailable.return_value = (RetryPolicy.RETHROW, None)
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query, 1)
        rf._query_retries = 1
        rf.send_request()

        result = Mock(spec=UnavailableErrorMessage, info={"required_replicas":2, "alive_replicas": 1, "consistency": 1})
        rf._set_result(None, None, None, result)
        self.assertRaises(Exception, rf.result)

    def test_request_error_with_prepare_message(self):
        session = self.make_session()
        query = SimpleStatement("SELECT * FROM foobar")
        retry_policy = Mock()
        retry_policy.on_request_error.return_value = (RetryPolicy.RETHROW, None)
        message = PrepareMessage(query=query)

        rf = ResponseFuture(session, message, query, 1, retry_policy=retry_policy)
        rf._query_retries = 1
        rf.send_request()
        result = Mock(spec=OverloadedErrorMessage)
        result.to_exception.return_value = result
        rf._set_result(None, None, None, result)
        self.assertIsInstance(rf._final_exception, OverloadedErrorMessage)

        rf = ResponseFuture(session, message, query, 1, retry_policy=retry_policy)
        rf._query_retries = 1
        rf.send_request()
        result = Mock(spec=ConnectionException)
        rf._set_result(None, None, None, result)
        self.assertIsInstance(rf._final_exception, ConnectionException)

    def test_retry_policy_says_ignore(self):
        session = self.make_session()
        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        retry_policy = Mock()
        retry_policy.on_unavailable.return_value = (RetryPolicy.IGNORE, None)
        rf = ResponseFuture(session, message, query, 1, retry_policy=retry_policy)
        rf.send_request()

        result = Mock(spec=UnavailableErrorMessage, info={})
        rf._set_result(None, None, None, result)
        self.assertFalse(rf.result())

    def test_retry_policy_says_retry(self):
        session = self.make_session()
        pool = session._pools.get.return_value

        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.QUORUM)

        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        retry_policy = Mock()
        retry_policy.on_unavailable.return_value = (RetryPolicy.RETRY, ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query, 1, retry_policy=retry_policy)
        rf.send_request()

        rf.session._pools.get.assert_called_once_with('ip1')
        pool.borrow_connection.assert_called_once_with(timeout=ANY)
        connection.send_msg.assert_called_once_with(rf.message, 1, cb=ANY, encoder=ProtocolHandler.encode_message, decoder=ProtocolHandler.decode_message, result_metadata=[])

        result = Mock(spec=UnavailableErrorMessage, info={})
        host = Mock()
        rf._set_result(host, None, None, result)

        session.submit.assert_called_once_with(rf._retry_task, True, host)
        self.assertEqual(1, rf._query_retries)

        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 2)

        # simulate the executor running this
        rf._retry_task(True, host)

        # it should try again with the same host since this was
        # an UnavailableException
        rf.session._pools.get.assert_called_with(host)
        pool.borrow_connection.assert_called_with(timeout=ANY)
        connection.send_msg.assert_called_with(rf.message, 2, cb=ANY, encoder=ProtocolHandler.encode_message, decoder=ProtocolHandler.decode_message, result_metadata=[])

    def test_retry_with_different_host(self):
        session = self.make_session()
        pool = session._pools.get.return_value

        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        rf = self.make_response_future(session)
        rf.message.consistency_level = ConsistencyLevel.QUORUM
        rf.send_request()

        rf.session._pools.get.assert_called_once_with('ip1')
        pool.borrow_connection.assert_called_once_with(timeout=ANY)
        connection.send_msg.assert_called_once_with(rf.message, 1, cb=ANY, encoder=ProtocolHandler.encode_message, decoder=ProtocolHandler.decode_message, result_metadata=[])
        self.assertEqual(ConsistencyLevel.QUORUM, rf.message.consistency_level)

        result = Mock(spec=OverloadedErrorMessage, info={})
        host = Mock()
        rf._set_result(host, None, None, result)

        session.submit.assert_called_once_with(rf._retry_task, False, host)
        # query_retries does get incremented for Overloaded/Bootstrapping errors (since 3.18)
        self.assertEqual(1, rf._query_retries)

        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 2)
        # simulate the executor running this
        rf._retry_task(False, host)

        # it should try with a different host
        rf.session._pools.get.assert_called_with('ip2')
        pool.borrow_connection.assert_called_with(timeout=ANY)
        connection.send_msg.assert_called_with(rf.message, 2, cb=ANY, encoder=ProtocolHandler.encode_message, decoder=ProtocolHandler.decode_message, result_metadata=[])

        # the consistency level should be the same
        self.assertEqual(ConsistencyLevel.QUORUM, rf.message.consistency_level)

    def test_all_retries_fail(self):
        session = self.make_session()
        pool = session._pools.get.return_value
        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        rf = self.make_response_future(session)
        rf.send_request()
        rf.session._pools.get.assert_called_once_with('ip1')

        result = Mock(spec=IsBootstrappingErrorMessage, info={})
        host = Mock()
        rf._set_result(host, None, None, result)

        # simulate the executor running this
        session.submit.assert_called_once_with(rf._retry_task, False, host)
        rf._retry_task(False, host)

        # it should try with a different host
        rf.session._pools.get.assert_called_with('ip2')

        result = Mock(spec=IsBootstrappingErrorMessage, info={})
        rf._set_result(host, None, None, result)

        # simulate the executor running this
        session.submit.assert_called_with(rf._retry_task, False, host)
        rf._retry_task(False, host)

        self.assertRaises(NoHostAvailable, rf.result)

    def test_all_pools_shutdown(self):
        session = self.make_basic_session()
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1', 'ip2']
        session._pools.get.return_value.is_shutdown = True

        rf = ResponseFuture(session, Mock(), Mock(), 1)
        rf.send_request()
        self.assertRaises(NoHostAvailable, rf.result)

    def test_first_pool_shutdown(self):
        session = self.make_basic_session()
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1', 'ip2']
        # first return a pool with is_shutdown=True, then is_shutdown=False
        pool_shutdown = self.make_pool()
        pool_shutdown.is_shutdown = True
        pool_ok = self.make_pool()
        pool_ok.is_shutdown = True
        session._pools.get.side_effect = [pool_shutdown, pool_ok]

        rf = self.make_response_future(session)
        rf.send_request()

        expected_result = (object(), object())
        rf._set_result(None, None, None, self.make_mock_response(expected_result[0], expected_result[1]))

        result = rf.result()[0]
        self.assertEqual(result, expected_result)

    def test_timeout_getting_connection_from_pool(self):
        session = self.make_basic_session()
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1', 'ip2']

        # the first pool will raise an exception on borrow_connection()
        exc = NoConnectionsAvailable()
        first_pool = Mock(is_shutdown=False)
        first_pool.borrow_connection.side_effect = exc

        # the second pool will return a connection
        second_pool = Mock(is_shutdown=False)
        connection = Mock(spec=Connection)
        second_pool.borrow_connection.return_value = (connection, 1)

        session._pools.get.side_effect = [first_pool, second_pool]

        rf = self.make_response_future(session)
        rf.send_request()

        expected_result = (object(), object())
        rf._set_result(None, None, None, self.make_mock_response(expected_result[0], expected_result[1]))
        self.assertEqual(rf.result()[0], expected_result)

        # make sure the exception is recorded correctly
        self.assertEqual(rf._errors, {'ip1': exc})

    def test_callback(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()

        callback = Mock()
        expected_result = (object(), object())
        arg = "positional"
        kwargs = {'one': 1, 'two': 2}
        rf.add_callback(callback, arg, **kwargs)

        rf._set_result(None, None, None, self.make_mock_response(expected_result[0], expected_result[1]))

        result = rf.result()[0]
        self.assertEqual(result, expected_result)

        callback.assert_called_once_with([expected_result], arg, **kwargs)

        # this should get called immediately now that the result is set
        rf.add_callback(self.assertEqual, [expected_result])

    def test_errback(self):
        session = self.make_session()
        pool = session._pools.get.return_value
        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query, 1)
        rf._query_retries = 1
        rf.send_request()

        rf.add_errback(self.assertIsInstance, Exception)

        result = Mock(spec=UnavailableErrorMessage, info={"required_replicas":2, "alive_replicas": 1, "consistency": 1})
        result.to_exception.return_value = Exception()

        rf._set_result(None, None, None, result)
        self.assertRaises(Exception, rf.result)

        # this should get called immediately now that the error is set
        rf.add_errback(self.assertIsInstance, Exception)

    def test_multiple_callbacks(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()

        callback = Mock()
        expected_result = (object(), object())
        arg = "positional"
        kwargs = {'one': 1, 'two': 2}
        rf.add_callback(callback, arg, **kwargs)

        callback2 = Mock()
        arg2 = "another"
        kwargs2 = {'three': 3, 'four': 4}
        rf.add_callback(callback2, arg2, **kwargs2)

        rf._set_result(None, None, None, self.make_mock_response(expected_result[0], expected_result[1]))

        result = rf.result()[0]
        self.assertEqual(result, expected_result)

        callback.assert_called_once_with([expected_result], arg, **kwargs)
        callback2.assert_called_once_with([expected_result], arg2, **kwargs2)

    def test_multiple_errbacks(self):
        session = self.make_session()
        pool = session._pools.get.return_value
        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        retry_policy = Mock()
        retry_policy.on_unavailable.return_value = (RetryPolicy.RETHROW, None)
        rf = ResponseFuture(session, message, query, 1, retry_policy=retry_policy)
        rf.send_request()

        callback = Mock()
        arg = "positional"
        kwargs = {'one': 1, 'two': 2}
        rf.add_errback(callback, arg, **kwargs)

        callback2 = Mock()
        arg2 = "another"
        kwargs2 = {'three': 3, 'four': 4}
        rf.add_errback(callback2, arg2, **kwargs2)

        expected_exception = Unavailable("message", 1, 2, 3)
        result = Mock(spec=UnavailableErrorMessage, info={"required_replicas":2, "alive_replicas": 1, "consistency": 1})
        result.to_exception.return_value = expected_exception
        rf._set_result(None, None, None, result)
        rf._event.set()
        self.assertRaises(Exception, rf.result)

        callback.assert_called_once_with(expected_exception, arg, **kwargs)
        callback2.assert_called_once_with(expected_exception, arg2, **kwargs2)

    def test_add_callbacks(self):
        session = self.make_session()
        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        # test errback
        rf = ResponseFuture(session, message, query, 1)
        rf._query_retries = 1
        rf.send_request()

        rf.add_callbacks(
            callback=self.assertEqual, callback_args=([{'col': 'val'}],),
            errback=self.assertIsInstance, errback_args=(Exception,))

        result = Mock(spec=UnavailableErrorMessage,
                      info={"required_replicas":2, "alive_replicas": 1, "consistency": 1})
        result.to_exception.return_value = Exception()
        rf._set_result(None, None, None, result)
        self.assertRaises(Exception, rf.result)

        # test callback
        rf = ResponseFuture(session, message, query, 1)
        rf.send_request()

        callback = Mock()
        expected_result = (object(), object())
        arg = "positional"
        kwargs = {'one': 1, 'two': 2}
        rf.add_callbacks(
            callback=callback, callback_args=(arg,), callback_kwargs=kwargs,
            errback=self.assertIsInstance, errback_args=(Exception,))

        rf._set_result(None, None, None, self.make_mock_response(expected_result[0], expected_result[1]))
        self.assertEqual(rf.result()[0], expected_result)

        callback.assert_called_once_with([expected_result], arg, **kwargs)

    def test_prepared_query_not_found(self):
        session = self.make_session()
        pool = session._pools.get.return_value
        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        rf = self.make_response_future(session)
        rf.send_request()

        session.cluster.protocol_version = ProtocolVersion.V4
        session.cluster._prepared_statements = MagicMock(dict)
        prepared_statement = session.cluster._prepared_statements.__getitem__.return_value
        prepared_statement.query_string = "SELECT * FROM foobar"
        prepared_statement.keyspace = "FooKeyspace"
        rf._connection.keyspace = "FooKeyspace"

        result = Mock(spec=PreparedQueryNotFound, info='a' * 16)
        rf._set_result(None, None, None, result)

        self.assertTrue(session.submit.call_args)
        args, kwargs = session.submit.call_args
        self.assertEqual(rf._reprepare, args[-5])
        self.assertIsInstance(args[-4], PrepareMessage)
        self.assertEqual(args[-4].query, "SELECT * FROM foobar")

    def test_prepared_query_not_found_bad_keyspace(self):
        session = self.make_session()
        pool = session._pools.get.return_value
        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        rf = self.make_response_future(session)
        rf.send_request()

        session.cluster.protocol_version = ProtocolVersion.V4
        session.cluster._prepared_statements = MagicMock(dict)
        prepared_statement = session.cluster._prepared_statements.__getitem__.return_value
        prepared_statement.query_string = "SELECT * FROM foobar"
        prepared_statement.keyspace = "FooKeyspace"
        rf._connection.keyspace = "BarKeyspace"

        result = Mock(spec=PreparedQueryNotFound, info='a' * 16)
        rf._set_result(None, None, None, result)
        self.assertRaises(ValueError, rf.result)

    def test_repeat_orig_query_after_succesful_reprepare(self):
        query_id = b'abc123'  # Just a random binary string so we don't hit id mismatch exception
        session = self.make_session()
        rf = self.make_response_future(session)

        response = Mock(spec=ResultMessage,
                        kind=RESULT_KIND_PREPARED,
                        result_metadata_id='foo')
        response.results = (None, None, None, None, None)
        response.query_id = query_id

        rf._query = Mock(return_value=True)
        rf._execute_after_prepare('host', None, None, response)
        rf._query.assert_called_once_with('host')

        rf.prepared_statement = Mock()
        rf.prepared_statement.query_id = query_id
        rf._query = Mock(return_value=True)
        rf._execute_after_prepare('host', None, None, response)
        rf._query.assert_called_once_with('host')

    def test_timeout_does_not_release_stream_id(self):
        """
        Make sure that stream ID is not reused immediately after client-side
        timeout. Otherwise, a new request could reuse the stream ID and would
        risk getting a response for the old, timed out query.
        """
        session = self.make_basic_session()
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = [Mock(endpoint='ip1'), Mock(endpoint='ip2')]
        pool = self.make_pool()
        session._pools.get.return_value = pool
        connection = Mock(spec=Connection, lock=RLock(), _requests={}, request_ids=deque(),
                orphaned_request_ids=set(), orphaned_threshold=256)
        pool.borrow_connection.return_value = (connection, 1)

        rf = self.make_response_future(session)
        rf.send_request()

        connection._requests[1] = (connection._handle_options_response, ProtocolHandler.decode_message, [])

        rf._on_timeout()
        pool.return_connection.assert_called_once_with(connection, stream_was_orphaned=True)
        self.assertRaisesRegex(OperationTimedOut, "Client request timeout", rf.result)

        assert len(connection.request_ids) == 0, \
            "Request IDs should be empty but it's not: {}".format(connection.request_ids)
