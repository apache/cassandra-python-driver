import threading

from cassandra.connection import Connection, ConnectionShutdown
import sys
import asyncio
import logging
import os
import socket
import ssl
from threading import Lock, Thread, get_ident


log = logging.getLogger(__name__)


# This module uses ``yield from`` and ``@asyncio.coroutine`` over ``await`` and
# ``async def`` for pre-Python-3.5 compatibility, so keep in mind that the
# managed coroutines are generator-based, not native coroutines. See PEP 492:
# https://www.python.org/dev/peps/pep-0492/#coroutine-objects


try:
    asyncio.run_coroutine_threadsafe
except AttributeError:
    raise ImportError(
        'Cannot use asyncioreactor without access to '
        'asyncio.run_coroutine_threadsafe (added in 3.4.6 and 3.5.1)'
    )


class AsyncioTimer(object):
    """
    An ``asyncioreactor``-specific Timer. Similar to :class:`.connection.Timer,
    but with a slightly different API due to limitations in the underlying
    ``call_later`` interface. Not meant to be used with a
    :class:`.connection.TimerManager`.
    """

    @property
    def end(self):
        raise NotImplementedError('{} is not compatible with TimerManager and '
                                  'does not implement .end()')

    def __init__(self, timeout, callback, loop):
        delayed = self._call_delayed_coro(timeout=timeout,
                                          callback=callback)
        self._handle = asyncio.run_coroutine_threadsafe(delayed, loop=loop)

    @staticmethod
    async def _call_delayed_coro(timeout, callback):
        await asyncio.sleep(timeout)
        return callback()

    def __lt__(self, other):
        try:
            return self._handle < other._handle
        except AttributeError:
            raise NotImplemented

    def cancel(self):
        self._handle.cancel()

    def finish(self):
        # connection.Timer method not implemented here because we can't inspect
        # the Handle returned from call_later
        raise NotImplementedError('{} is not compatible with TimerManager and '
                                  'does not implement .finish()')


class AsyncioConnection(Connection):
    """
    An experimental implementation of :class:`.Connection` that uses the
    ``asyncio`` module in the Python standard library for its event loop.

    Note that it requires ``asyncio`` features that were only introduced in the
    3.4 line in 3.4.6, and in the 3.5 line in 3.5.1.
    """

    _loop = None
    _pid = os.getpid()

    _lock = Lock()
    _loop_thread = None

    _write_queue = None
    _write_queue_lock = None

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)

        self._connect_socket()
        self._socket.setblocking(0)
        loop_args = dict()
        if sys.version_info[0] == 3 and sys.version_info[1] < 10:
            loop_args['loop'] = self._loop
        self._write_queue = asyncio.Queue(**loop_args)
        self._write_queue_lock = asyncio.Lock(**loop_args)

        # see initialize_reactor -- loop is running in a separate thread, so we
        # have to use a threadsafe call
        self._read_watcher = asyncio.run_coroutine_threadsafe(
            self.handle_read(), loop=self._loop
        )
        self._write_watcher = asyncio.run_coroutine_threadsafe(
            self.handle_write(), loop=self._loop
        )
        self._send_options_message()

    @classmethod
    def initialize_reactor(cls):
        with cls._lock:
            if cls._pid != os.getpid():
                cls._loop = None
            if cls._loop is None:
                try:
                    cls._loop = asyncio.get_running_loop()
                except RuntimeError:
                    cls._loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(cls._loop)

            if not cls._loop_thread:
                # daemonize so the loop will be shut down on interpreter
                # shutdown
                cls._loop_thread = Thread(target=cls._loop.run_forever,
                                          daemon=True, name="asyncio_thread")
                cls._loop_thread.start()

    @classmethod
    def create_timer(cls, timeout, callback):
        return AsyncioTimer(timeout, callback, loop=cls._loop)

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        # close from the loop thread to avoid races when removing file
        # descriptors
        asyncio.run_coroutine_threadsafe(
            self._close(), loop=self._loop
        )

    async def _close(self):
        log.debug("Closing connection (%s) to %s" % (id(self), self.endpoint))
        if self._write_watcher:
            self._write_watcher.cancel()
        if self._read_watcher:
            self._read_watcher.cancel()
        if self._socket:
            self._loop.remove_writer(self._socket.fileno())
            self._loop.remove_reader(self._socket.fileno())
            self._socket.close()

        log.debug("Closed socket to %s" % (self.endpoint,))

        if not self.is_defunct:
            self.error_all_requests(
                ConnectionShutdown("Connection to %s was closed" % self.endpoint))
            # don't leave in-progress operations hanging
            self.connected_event.set()

    def push(self, data):
        buff_size = self.out_buffer_size
        if len(data) > buff_size:
            chunks = []
            for i in range(0, len(data), buff_size):
                chunks.append(data[i:i + buff_size])
        else:
            chunks = [data]

        if self._loop_thread != threading.current_thread():
            asyncio.run_coroutine_threadsafe(
                self._push_msg(chunks),
                loop=self._loop
            )
        else:
            # avoid races/hangs by just scheduling this, not using threadsafe
            self._loop.create_task(self._push_msg(chunks))

    async def _push_msg(self, chunks):
        # This lock ensures all chunks of a message are sequential in the Queue
        async with self._write_queue_lock:
            for chunk in chunks:
                self._write_queue.put_nowait(chunk)


    async def handle_write(self):
        while True:
            try:
                next_msg = await self._write_queue.get()
                if next_msg:
                    await self._loop.sock_sendall(self._socket, next_msg)
            except socket.error as err:
                log.debug("Exception in send for %s: %s", self, err)
                self.defunct(err)
                return
            except asyncio.CancelledError:
                return

    async def handle_read(self):
        while True:
            try:
                buf = await self._loop.sock_recv(self._socket, self.in_buffer_size)
                self._iobuf.write(buf)
            # sock_recv expects EWOULDBLOCK if socket provides no data, but
            # nonblocking ssl sockets raise these instead, so we handle them
            # ourselves by yielding to the event loop, where the socket will
            # get the reading/writing it "wants" before retrying
            except (ssl.SSLWantWriteError, ssl.SSLWantReadError):
                # Apparently the preferred way to yield to the event loop from within
                # a native coroutine based on https://github.com/python/asyncio/issues/284
                await asyncio.sleep(0)
                continue
            except socket.error as err:
                log.debug("Exception during socket recv for %s: %s",
                          self, err)
                self.defunct(err)
                return  # leave the read loop
            except asyncio.CancelledError:
                return

            if buf and self._iobuf.tell():
                self.process_io_buffer()
            else:
                log.debug("Connection %s closed by server", self)
                self.close()
                return
