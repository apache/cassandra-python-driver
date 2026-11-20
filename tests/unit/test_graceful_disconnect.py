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

"""
Tests for graceful disconnect support (CEP-59): decoding the
GRACEFUL_DISCONNECT event, per-connection capability negotiation against the
SUPPORTED response, draining connections without disrupting in-flight
requests, and the control connection / pool / cluster plumbing.
"""

import unittest
from io import BytesIO
from threading import Lock
from unittest.mock import Mock, NonCallableMagicMock, patch

from cassandra.cluster import Cluster, ControlConnection, Session
from cassandra.connection import (Connection, ConnectionShutdown,
                                  DefaultEndPoint)
from cassandra.metrics import Metrics
from cassandra.pool import Host, HostConnection, NoConnectionsAvailable
from cassandra.policies import HostDistance, SimpleConvictionPolicy
from cassandra.protocol import (EventMessage, NotSupportedError,
                                ProtocolHandler, SupportedMessage,
                                write_string, write_stringmultimap)
from cassandra.connection import _Frame

from tests.unit.test_control_connection import (MockCluster, MockConnection,
                                                FakeTime)


class GracefulDisconnectEventDecodeTest(unittest.TestCase):
    """
    Decodes the CEP-59 GRACEFUL_DISCONNECT event body as the server would
    send it (the event is body-less: only the type string is present).
    """

    @staticmethod
    def _event_body(event_type):
        buf = BytesIO()
        write_string(buf, event_type)
        return buf.getvalue()

    def test_decode_graceful_disconnect_event(self):
        for protocol_version in (3, 4, 5, 6):
            msg = EventMessage.recv_body(
                BytesIO(self._event_body('GRACEFUL_DISCONNECT')), protocol_version)
            self.assertIsInstance(msg, EventMessage)
            self.assertEqual(msg.event_type, 'GRACEFUL_DISCONNECT')
            self.assertEqual(msg.event_args, {})

    def test_decode_is_case_insensitive(self):
        msg = EventMessage.recv_body(
            BytesIO(self._event_body('graceful_disconnect')), 4)
        self.assertEqual(msg.event_type, 'GRACEFUL_DISCONNECT')

    def test_unknown_event_type_raises(self):
        self.assertRaises(
            NotSupportedError,
            EventMessage.recv_body,
            BytesIO(self._event_body('GRACEFUL_DISCONNECT_V2')), 4)


class SupportsGracefulDisconnectTest(unittest.TestCase):
    """
    The capability is negotiated per connection from the SUPPORTED options
    string-multimap; the server may send the key with an explicit "false"
    value when the feature is disabled.
    """

    def test_detect_capability_from_supported_options(self):
        supports = Connection._supports_graceful_disconnect
        self.assertFalse(supports(None))
        self.assertFalse(supports({}))
        self.assertFalse(supports({'CQL_VERSION': ['3.4.7']}))
        self.assertTrue(supports({'GRACEFUL_DISCONNECT': []}))
        self.assertTrue(supports({'GRACEFUL_DISCONNECT': ['true']}))
        self.assertFalse(supports({'GRACEFUL_DISCONNECT': ['false']}))
        self.assertFalse(supports({'GRACEFUL_DISCONNECT': ['FALSE']}))


class ConnectionGracefulDrainTest(unittest.TestCase):
    """
    Draining behavior of a single connection when a GRACEFUL_DISCONNECT
    event is received on it.
    """

    def make_connection(self):
        c = Connection(DefaultEndPoint('1.2.3.4'))
        c._socket = Mock()
        c._socket.send.side_effect = lambda x: len(x)
        c.close = Mock()
        return c

    def _options_response_requests(self, connection):
        return {0: (connection._handle_options_response,
                    ProtocolHandler.decode_message, [])}

    def _process_supported_message(self, connection, options):
        options_buf = BytesIO()
        write_stringmultimap(options_buf, options)
        body = options_buf.getvalue()
        connection.process_msg(
            _Frame(version=4, flags=0, stream=0, opcode=SupportedMessage.opcode,
                   body_offset=9, end_pos=9 + len(body)),
            body)

    def test_options_response_sets_supports_graceful_disconnect(self):
        c = self.make_connection()
        c._requests = self._options_response_requests(c)
        self.assertFalse(c.supports_graceful_disconnect)
        self._process_supported_message(c, {
            'CQL_VERSION': ['3.4.7'],
            'COMPRESSION': [],
            'GRACEFUL_DISCONNECT': ['true']
        })
        self.assertTrue(c.supports_graceful_disconnect)

    def test_options_response_without_capability(self):
        c = self.make_connection()
        c._requests = self._options_response_requests(c)
        self._process_supported_message(c, {
            'CQL_VERSION': ['3.4.7'],
            'COMPRESSION': []
        })
        self.assertFalse(c.supports_graceful_disconnect)

    def test_options_response_with_capability_disabled(self):
        c = self.make_connection()
        c._requests = self._options_response_requests(c)
        self._process_supported_message(c, {
            'CQL_VERSION': ['3.4.7'],
            'COMPRESSION': [],
            'GRACEFUL_DISCONNECT': ['false']
        })
        self.assertFalse(c.supports_graceful_disconnect)

    def test_drain_waits_for_pending_requests(self):
        c = self.make_connection()
        # a pending request on stream 5
        response_callback = Mock()
        c._requests = {5: (response_callback, ProtocolHandler.decode_message, [])}

        c.start_graceful_drain()

        # connection not closed yet because there is a pending request
        self.assertTrue(c.is_draining)
        self.assertEqual(c.close.call_count, 0)

        # new writes are refused
        self.assertRaises(ConnectionShutdown, c.send_msg,
                          Mock(), 6, Mock())

        # when the pending request completes, the connection closes
        options_buf = BytesIO()
        write_stringmultimap(options_buf, {'CQL_VERSION': ['3.4.7'],
                                           'COMPRESSION': []})
        body = options_buf.getvalue()
        c.process_msg(
            _Frame(version=4, flags=0, stream=5, opcode=SupportedMessage.opcode,
                   body_offset=9, end_pos=9 + len(body)),
            body)

        response_callback.assert_called_once()
        c.close.assert_called_once_with()

    def test_drain_closes_immediately_when_no_pending(self):
        c = self.make_connection()
        c.start_graceful_drain()
        self.assertTrue(c.is_draining)
        c.close.assert_called_once_with()

    def test_drain_is_idempotent(self):
        c = self.make_connection()
        c.start_graceful_drain()
        c.start_graceful_drain()
        c.close.assert_called_once_with()

    def test_pushed_event_starts_drain_and_notifies_watchers(self):
        c = self.make_connection()
        watcher = Mock()
        c._push_watchers['GRACEFUL_DISCONNECT'].add(watcher)

        c.handle_pushed(EventMessage('GRACEFUL_DISCONNECT', {}))

        # the drain started (and completed, since nothing was pending)
        # before the watcher was notified
        self.assertTrue(c.is_draining)
        c.close.assert_called_once_with()
        watcher.assert_called_once_with({})

    def test_pushed_event_drains_even_if_watcher_errors(self):
        c = self.make_connection()
        watcher = Mock(side_effect=RuntimeError("misbehaving callback"))
        c._push_watchers['GRACEFUL_DISCONNECT'].add(watcher)

        c.handle_pushed(EventMessage('GRACEFUL_DISCONNECT', {}))

        self.assertTrue(c.is_draining)
        c.close.assert_called_once_with()

    def test_other_events_do_not_drain(self):
        c = self.make_connection()
        c.handle_pushed(EventMessage('STATUS_CHANGE',
                                     {'change_type': 'UP',
                                      'address': ('1.2.3.4', 9042)}))
        self.assertFalse(c.is_draining)
        self.assertEqual(c.close.call_count, 0)


class ControlConnectionGracefulDisconnectTest(unittest.TestCase):

    def setUp(self):
        self.cluster = MockCluster()
        self.cluster.graceful_disconnect_enabled = True
        self.cluster.on_graceful_disconnect = Mock()
        self.connection = MockConnection()
        self.time = FakeTime()

        self.control_connection = ControlConnection(self.cluster, 1, 0, 0, 0)
        self.control_connection._connection = self.connection
        self.control_connection._time = self.time

    def _registered_watchers(self, connection):
        host = self.cluster.metadata.get_host(DefaultEndPoint("192.168.1.0"))
        self.cluster.connection_factory = Mock(return_value=connection)
        connection.register_watchers = Mock()
        connection.close = Mock()
        # _try_connect issues the peers/local queries with fail_on_error=False,
        # which returns (success, result) pairs
        peers_result, local_result = connection.wait_for_responses.return_value
        connection.wait_for_responses = Mock(
            return_value=((True, peers_result), (True, local_result)))
        cc = self.control_connection
        cc._refresh_node_list_and_token_map = Mock()
        cc._refresh_schema = Mock()
        cc._try_connect(host)
        args, kwargs = connection.register_watchers.call_args
        return args[0]

    def test_registers_graceful_disconnect_when_supported(self):
        connection = MockConnection()
        connection.supports_graceful_disconnect = True

        watchers = self._registered_watchers(connection)

        self.assertEqual(
            sorted(watchers.keys()),
            ['GRACEFUL_DISCONNECT', 'SCHEMA_CHANGE', 'STATUS_CHANGE',
             'TOPOLOGY_CHANGE'])

    def test_does_not_register_when_server_does_not_advertise(self):
        connection = MockConnection()
        connection.supports_graceful_disconnect = False

        watchers = self._registered_watchers(connection)

        self.assertEqual(
            sorted(watchers.keys()),
            ['SCHEMA_CHANGE', 'STATUS_CHANGE', 'TOPOLOGY_CHANGE'])

    def test_does_not_register_when_disabled(self):
        self.cluster.graceful_disconnect_enabled = False
        connection = MockConnection()
        connection.supports_graceful_disconnect = True

        watchers = self._registered_watchers(connection)

        self.assertEqual(
            sorted(watchers.keys()),
            ['SCHEMA_CHANGE', 'STATUS_CHANGE', 'TOPOLOGY_CHANGE'])

    def test_handle_graceful_disconnect(self):
        self.control_connection.reconnect = Mock()

        self.control_connection._handle_graceful_disconnect({})

        host = self.cluster.metadata.get_host(DefaultEndPoint("192.168.1.0"))
        self.cluster.on_graceful_disconnect.assert_called_once_with(host)
        # the control connection proactively moves to another host
        self.control_connection.reconnect.assert_called_once_with()

    def test_handle_graceful_disconnect_without_connection(self):
        self.control_connection.reconnect = Mock()
        self.control_connection._connection = None

        self.control_connection._handle_graceful_disconnect({})

        self.assertEqual(self.cluster.on_graceful_disconnect.call_count, 0)
        self.control_connection.reconnect.assert_called_once_with()


class HostConnectionGracefulDisconnectTest(unittest.TestCase):

    def make_session(self, graceful_disconnect_enabled=True):
        session = NonCallableMagicMock(spec=Session, keyspace='foobarkeyspace')
        session.cluster.get_core_connections_per_host.return_value = 1
        session.cluster.get_max_requests_per_connection.return_value = 1
        session.cluster.get_max_connections_per_host.return_value = 1
        session.cluster.graceful_disconnect_enabled = graceful_disconnect_enabled
        return session

    def make_connection_mock(self, supports_graceful_disconnect=True):
        return NonCallableMagicMock(
            spec=Connection, in_flight=0, is_draining=False, is_defunct=False,
            is_closed=False, max_request_id=100, lock=Lock(),
            supports_graceful_disconnect=supports_graceful_disconnect)

    def make_pool(self, session, conn):
        host = Mock(spec=Host, address='ip1')
        session.cluster.connection_factory.return_value = conn
        return HostConnection(host, HostDistance.LOCAL, session)

    def test_registers_watcher_when_enabled_and_supported(self):
        session = self.make_session()
        conn = self.make_connection_mock()
        pool = self.make_pool(session, conn)

        conn.register_watcher.assert_called_once_with(
            "GRACEFUL_DISCONNECT", pool._on_graceful_disconnect_event)

    def test_does_not_register_watcher_when_disabled(self):
        session = self.make_session(graceful_disconnect_enabled=False)
        conn = self.make_connection_mock()
        self.make_pool(session, conn)

        self.assertEqual(conn.register_watcher.call_count, 0)

    def test_does_not_register_watcher_when_not_supported(self):
        session = self.make_session()
        conn = self.make_connection_mock(supports_graceful_disconnect=False)
        self.make_pool(session, conn)

        self.assertEqual(conn.register_watcher.call_count, 0)

    def test_event_on_pooled_connection_notifies_cluster(self):
        session = self.make_session()
        conn = self.make_connection_mock()
        pool = self.make_pool(session, conn)

        pool._on_graceful_disconnect_event({})

        session.cluster.on_graceful_disconnect.assert_called_once_with(pool.host)

    def test_on_graceful_disconnect_drains_connections(self):
        session = self.make_session()
        conn = self.make_connection_mock()
        pool = self.make_pool(session, conn)

        pool.on_graceful_disconnect()

        conn.start_graceful_drain.assert_called_once_with()

    def test_on_graceful_disconnect_noop_when_shutdown(self):
        session = self.make_session()
        conn = self.make_connection_mock()
        pool = self.make_pool(session, conn)
        pool.shutdown()
        conn.start_graceful_drain.reset_mock()

        pool.on_graceful_disconnect()

        self.assertEqual(conn.start_graceful_drain.call_count, 0)

    def test_borrow_fails_over_while_draining(self):
        session = self.make_session()
        conn = self.make_connection_mock()
        pool = self.make_pool(session, conn)

        conn.is_draining = True

        self.assertRaises(NoConnectionsAvailable,
                          pool.borrow_connection, 0)


class ClusterGracefulDisconnectTest(unittest.TestCase):

    def test_enabled_by_default(self):
        cluster = Cluster(contact_points=['127.0.0.1'])
        self.assertTrue(cluster.graceful_disconnect_enabled)

    def test_can_be_disabled(self):
        cluster = Cluster(contact_points=['127.0.0.1'],
                          graceful_disconnect_enabled=False)
        self.assertFalse(cluster.graceful_disconnect_enabled)

    def _cluster_with_mock_session(self):
        cluster = Cluster(contact_points=['127.0.0.1'])
        session = Mock(spec=Session)
        host = Mock(spec=Host, address='ip1')
        other_host = Mock(spec=Host, address='ip2')
        pool = Mock()
        session._pools = {host: pool}
        cluster.sessions.add(session)
        return cluster, session, host, other_host, pool

    def test_on_graceful_disconnect_drains_pools_for_host(self):
        cluster, session, host, _, pool = self._cluster_with_mock_session()

        cluster.on_graceful_disconnect(host)

        pool.on_graceful_disconnect.assert_called_once_with()

    def test_on_graceful_disconnect_ignores_other_hosts(self):
        cluster, session, host, other_host, pool = self._cluster_with_mock_session()

        cluster.on_graceful_disconnect(other_host)

        self.assertEqual(pool.on_graceful_disconnect.call_count, 0)

    def test_on_graceful_disconnect_ignores_unknown_host(self):
        cluster, _, _, _, pool = self._cluster_with_mock_session()

        cluster.on_graceful_disconnect(None)

        self.assertEqual(pool.on_graceful_disconnect.call_count, 0)

    def test_on_graceful_disconnect_increments_metrics(self):
        cluster, _, host, _, _ = self._cluster_with_mock_session()
        cluster.metrics_enabled = True
        cluster.metrics = Mock(spec=Metrics)

        cluster.on_graceful_disconnect(host)

        cluster.metrics.on_graceful_disconnect.assert_called_once_with()


class MetricsGracefulDisconnectTest(unittest.TestCase):

    def test_graceful_disconnects_counter(self):
        metrics = Metrics(Mock())
        before = metrics.stats.graceful_disconnects
        metrics.on_graceful_disconnect()
        self.assertEqual(metrics.stats.graceful_disconnects, before + 1)
