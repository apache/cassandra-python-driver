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
from unittest.mock import Mock, patch

from cassandra.connection import DefaultEndPoint

try:
    from twisted.test import proto_helpers
    from twisted.python.failure import Failure
    from cassandra.io import twistedreactor
    from cassandra.io.twistedreactor import TwistedConnection
except ImportError:
    twistedreactor = TwistedConnection = None  # NOQA


from cassandra.connection import _Frame

from tests.unit.io.utils import TimerTestMixin

class TestTwistedTimer(TimerTestMixin, unittest.TestCase):
    """
    Simple test class that is used to validate that the TimerManager, and timer
    classes function appropriately with the twisted infrastructure
    """

    connection_class = TwistedConnection

    @property
    def create_timer(self):
        return self.connection.create_timer

    @property
    def _timers(self):
        return self.connection._loop._timers

    def setUp(self):
        if twistedreactor is None:
            raise unittest.SkipTest("Twisted libraries not available")
        twistedreactor.TwistedConnection.initialize_reactor()
        super(TestTwistedTimer, self).setUp()


class TestTwistedProtocol(unittest.TestCase):

    def setUp(self):
        if twistedreactor is None:
            raise unittest.SkipTest("Twisted libraries not available")
        twistedreactor.TwistedConnection.initialize_reactor()
        self.tr = proto_helpers.StringTransportWithDisconnection()
        self.tr.connector = Mock()
        self.mock_connection = Mock()
        self.obj_ut = twistedreactor.TwistedConnectionProtocol(self.mock_connection)
        self.tr.protocol = self.obj_ut

    def tearDown(self):
        loop = twistedreactor.TwistedConnection._loop
        if not loop._reactor_stopped():
            loop._cleanup()

    def test_makeConnection(self):
        """
        Verify that the protocol class notifies the connection
        object that a successful connection was made.
        """
        self.obj_ut.makeConnection(self.tr)
        self.assertTrue(self.mock_connection.client_connection_made.called)

    def test_receiving_data(self):
        """
        Verify that the dataReceived() callback writes the data to
        the connection object's buffer and calls handle_read().
        """
        self.obj_ut.makeConnection(self.tr)
        self.obj_ut.dataReceived('foobar')
        self.assertTrue(self.mock_connection.handle_read.called)
        self.mock_connection._iobuf.write.assert_called_with("foobar")


class TestTwistedConnection(unittest.TestCase):
    def setUp(self):
        if twistedreactor is None:
            raise unittest.SkipTest("Twisted libraries not available")
        twistedreactor.TwistedConnection.initialize_reactor()
        self.reactor_cft_patcher = patch(
            'twisted.internet.reactor.callFromThread')
        self.reactor_run_patcher = patch('twisted.internet.reactor.run')
        self.mock_reactor_cft = self.reactor_cft_patcher.start()
        self.mock_reactor_run = self.reactor_run_patcher.start()
        self.obj_ut = twistedreactor.TwistedConnection(DefaultEndPoint('1.2.3.4'),
                                                       cql_version='3.0.1')

    def tearDown(self):
        self.reactor_cft_patcher.stop()
        self.reactor_run_patcher.stop()

    def test_connection_initialization(self):
        """
        Verify that __init__() works correctly.
        """
        self.mock_reactor_cft.assert_called_with(self.obj_ut.add_connection)
        self.obj_ut._loop._cleanup()
        self.mock_reactor_run.assert_called_with(installSignalHandlers=False)

    def test_client_connection_made(self):
        """
        Verifiy that _send_options_message() is called in
        client_connection_made()
        """
        self.obj_ut._send_options_message = Mock()
        self.obj_ut.client_connection_made(Mock())
        self.obj_ut._send_options_message.assert_called_with()

    @patch('twisted.internet.reactor.connectTCP')
    def test_close(self, mock_connectTCP):
        """
        Verify that close() disconnects the connector and errors callbacks.
        """
        transport = Mock()
        self.obj_ut.error_all_requests = Mock()
        self.obj_ut.add_connection()
        self.obj_ut.client_connection_made(transport)
        self.obj_ut.is_closed = False
        self.obj_ut.close()

        self.assertTrue(self.obj_ut.connected_event.is_set())
        self.assertTrue(self.obj_ut.error_all_requests.called)

    def test_handle_read__incomplete(self):
        """
        Verify that handle_read() processes incomplete messages properly.
        """
        self.obj_ut.process_msg = Mock()
        self.assertEqual(self.obj_ut._iobuf.getvalue(), b'')  # buf starts empty
        # incomplete header
        self.obj_ut._iobuf.write(b'\x84\x00\x00\x00\x00')
        self.obj_ut.handle_read()
        self.assertEqual(self.obj_ut._io_buffer.cql_frame_buffer.getvalue(), b'\x84\x00\x00\x00\x00')

        # full header, but incomplete body
        self.obj_ut._iobuf.write(b'\x00\x00\x00\x15')
        self.obj_ut.handle_read()
        self.assertEqual(self.obj_ut._io_buffer.cql_frame_buffer.getvalue(),
                         b'\x84\x00\x00\x00\x00\x00\x00\x00\x15')
        self.assertEqual(self.obj_ut._current_frame.end_pos, 30)

        # verify we never attempted to process the incomplete message
        self.assertFalse(self.obj_ut.process_msg.called)

    def test_handle_read__fullmessage(self):
        """
        Verify that handle_read() processes complete messages properly.
        """
        self.obj_ut.process_msg = Mock()
        self.assertEqual(self.obj_ut._iobuf.getvalue(), b'')  # buf starts empty

        # write a complete message, plus 'NEXT' (to simulate next message)
        # assumes protocol v3+ as default Connection.protocol_version
        body = b'this is the drum roll'
        extra = b'NEXT'
        self.obj_ut._iobuf.write(
            b'\x84\x01\x00\x02\x03\x00\x00\x00\x15' + body + extra)
        self.obj_ut.handle_read()
        self.assertEqual(self.obj_ut._io_buffer.cql_frame_buffer.getvalue(), extra)
        self.obj_ut.process_msg.assert_called_with(
            _Frame(version=4, flags=1, stream=2, opcode=3, body_offset=9, end_pos=9 + len(body)), body)

    @patch('twisted.internet.reactor.connectTCP')
    def test_push(self, mock_connectTCP):
        """
        Verifiy that push() calls transport.write(data).
        """
        self.obj_ut.add_connection()
        transport_mock = Mock()
        self.obj_ut.transport = transport_mock
        self.obj_ut.push('123 pickup')
        self.mock_reactor_cft.assert_called_with(
            transport_mock.write, '123 pickup')
