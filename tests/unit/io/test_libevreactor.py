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

from unittest.mock import patch, Mock
import weakref
import socket

from cassandra import DependencyException
try:
    from cassandra.io.libevreactor import _cleanup as libev__cleanup
    from cassandra.io.libevreactor import LibevConnection
except DependencyException:
    LibevConnection = None  # noqa

from tests import is_monkey_patched
from tests.unit.io.utils import ReactorTestMixin, TimerTestMixin, noop_if_monkey_patched


class LibevConnectionTest(ReactorTestMixin, unittest.TestCase):

    connection_class = LibevConnection
    socket_attr_name = '_socket'
    null_handle_function_args = None, 0

    def setUp(self):
        if is_monkey_patched():
            raise unittest.SkipTest("Can't test libev with monkey patching")
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        LibevConnection.initialize_reactor()

        # we patch here rather than as a decorator so that the Mixin can avoid
        # specifying patch args to test methods
        patchers = [patch(obj) for obj in
                    ('socket.socket',
                     'cassandra.io.libevwrapper.IO',
                     'cassandra.io.libevreactor.LibevLoop.maybe_start'
                     )]
        for p in patchers:
            self.addCleanup(p.stop)
        for p in patchers:
            p.start()

    def test_watchers_are_finished(self):
        """
        Test for asserting that watchers are closed in LibevConnection

        This test simulates a process termination without calling cluster.shutdown(), which would trigger
        _global_loop._cleanup. It will check the watchers have been closed
        Finally it will restore the LibevConnection reactor so it doesn't affect
        the rest of the tests

        @since 3.10
        @jira_ticket PYTHON-747
        @expected_result the watchers are closed

        @test_category connection
        """
        from cassandra.io.libevreactor import _global_loop
        with patch.object(_global_loop, "_thread"),\
             patch.object(_global_loop, "notify"):

            self.make_connection()

            # We have to make a copy because the connections shouldn't
            # be alive when we verify them
            live_connections = set(_global_loop._live_conns)

            # This simulates the process ending without cluster.shutdown()
            # being called, then with atexit _cleanup for libevreactor would
            # be called
            libev__cleanup(_global_loop)
            for conn in live_connections:
                self.assertTrue(conn._write_watcher.stop.mock_calls)
                self.assertTrue(conn._read_watcher.stop.mock_calls)

        _global_loop._shutdown = False


class LibevTimerPatcher(unittest.TestCase):

    @classmethod
    @noop_if_monkey_patched
    def setUpClass(cls):
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        cls.patchers = [
            patch('socket.socket', spec=socket.socket),
            patch('cassandra.io.libevwrapper.IO')
        ]
        for p in cls.patchers:
            p.start()

    @classmethod
    @noop_if_monkey_patched
    def tearDownClass(cls):
        for p in cls.patchers:
            try:
                p.stop()
            except:
                pass


class LibevTimerTest(TimerTestMixin, LibevTimerPatcher):
    connection_class = LibevConnection

    @property
    def create_timer(self):
        return self.connection.create_timer

    @property
    def _timers(self):
        from cassandra.io.libevreactor import _global_loop
        return _global_loop._timers

    def make_connection(self):
        c = LibevConnection('1.2.3.4', cql_version='3.0.1')
        c._socket_impl = Mock()
        c._socket.return_value.send.side_effect = lambda x: len(x)
        return c

    def setUp(self):
        if is_monkey_patched():
            raise unittest.SkipTest("Can't test libev with monkey patching.")
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')

        LibevConnection.initialize_reactor()
        super(LibevTimerTest, self).setUp()
