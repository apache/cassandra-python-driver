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

from tests.unit.io.utils import TimerTestMixin
from tests import notpypy, EVENT_LOOP_MANAGER

from eventlet import monkey_patch
from unittest.mock import patch

try:
    from cassandra.io.eventletreactor import EventletConnection
except ImportError:
    EventletConnection = None  # noqa

skip_condition = EventletConnection is None or EVENT_LOOP_MANAGER != "eventlet"
# There are some issues with some versions of pypy and eventlet
@notpypy
@unittest.skipIf(skip_condition, "Skipping the eventlet tests because it's not installed")
class EventletTimerTest(TimerTestMixin, unittest.TestCase):

    connection_class = EventletConnection

    @classmethod
    def setUpClass(cls):
        # This is run even though the class is skipped, so we need
        # to make sure no monkey patching is happening
        if skip_condition:
            return

        # This is being added temporarily due to a bug in eventlet:
        # https://github.com/eventlet/eventlet/issues/401
        import eventlet
        eventlet.sleep()
        monkey_patch()
        # cls.connection_class = EventletConnection

        EventletConnection.initialize_reactor()
        assert EventletConnection._timers is not None

    def setUp(self):
        socket_patcher = patch('eventlet.green.socket.socket')
        self.addCleanup(socket_patcher.stop)
        socket_patcher.start()

        super(EventletTimerTest, self).setUp()

        recv_patcher = patch.object(self.connection._socket,
                                    'recv',
                                    return_value=b'')
        self.addCleanup(recv_patcher.stop)
        recv_patcher.start()

    @property
    def create_timer(self):
        return self.connection.create_timer

    @property
    def _timers(self):
        return self.connection._timers

    # There is no unpatching because there is not a clear way
    # of doing it reliably
