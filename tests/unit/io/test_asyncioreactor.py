AsyncioConnection, ASYNCIO_AVAILABLE = None, False
try:
    from cassandra.io.asyncioreactor import AsyncioConnection
    import asynctest
    ASYNCIO_AVAILABLE = True
except (ImportError, SyntaxError):
    AsyncioConnection = None
    ASYNCIO_AVAILABLE = False

from tests import connection_class
from tests.unit.io.utils import TimerCallback, TimerTestMixin

from unittest.mock import patch

import unittest
import time

skip_me = ( not ASYNCIO_AVAILABLE or
           (connection_class is not AsyncioConnection))


@unittest.skipIf(connection_class is not AsyncioConnection,
                 'not running asyncio tests; current connection_class is {}'.format(connection_class))
@unittest.skipUnless(ASYNCIO_AVAILABLE, "asyncio is not available for this runtime")
class AsyncioTimerTests(TimerTestMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if skip_me:
            return
        cls.connection_class = AsyncioConnection
        AsyncioConnection.initialize_reactor()

    @classmethod
    def tearDownClass(cls):
        if skip_me:
            return
        if ASYNCIO_AVAILABLE and AsyncioConnection._loop:
            AsyncioConnection._loop.stop()

    @property
    def create_timer(self):
        return self.connection.create_timer

    @property
    def _timers(self):
        raise RuntimeError('no TimerManager for AsyncioConnection')

    def setUp(self):
        if skip_me:
            return
        socket_patcher = patch('socket.socket')
        self.addCleanup(socket_patcher.stop)
        socket_patcher.start()

        old_selector = AsyncioConnection._loop._selector
        AsyncioConnection._loop._selector = asynctest.TestSelector()

        def reset_selector():
            AsyncioConnection._loop._selector = old_selector

        self.addCleanup(reset_selector)

        super(AsyncioTimerTests, self).setUp()

    def test_timer_cancellation(self):
        # Various lists for tracking callback stage
        timeout = .1
        callback = TimerCallback(timeout)
        timer = self.create_timer(timeout, callback.invoke)
        timer.cancel()
        # Release context allow for timer thread to run.
        time.sleep(.2)
        # Assert that the cancellation was honored
        self.assertFalse(callback.was_invoked())
