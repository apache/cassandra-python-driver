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
from unittest.mock import patch


from tests.unit.io.utils import TimerTestMixin
from tests import EVENT_LOOP_MANAGER
try:
    from cassandra.io.geventreactor import GeventConnection
    import gevent.monkey
except ImportError:
    GeventConnection = None  # noqa


skip_condition = GeventConnection is None or EVENT_LOOP_MANAGER != "gevent"
@unittest.skipIf(skip_condition, "Skipping the gevent tests because it's not installed")
class GeventTimerTest(TimerTestMixin, unittest.TestCase):

    connection_class = GeventConnection

    @classmethod
    def setUpClass(cls):
        # This is run even though the class is skipped, so we need
        # to make sure no monkey patching is happening
        if skip_condition:
            return
        # There is no unpatching because there is not a clear way
        # of doing it reliably
        gevent.monkey.patch_all()
        GeventConnection.initialize_reactor()

    def setUp(self):
        socket_patcher = patch('gevent.socket.socket')
        self.addCleanup(socket_patcher.stop)
        socket_patcher.start()

        super(GeventTimerTest, self).setUp()

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
