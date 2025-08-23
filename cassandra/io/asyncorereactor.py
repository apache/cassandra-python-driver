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
import atexit
from collections import deque
from functools import partial
import logging
import os
import socket
import sys
from threading import Lock, Thread, Event
import time
import weakref
import sys
import ssl

try:
    from weakref import WeakSet
except ImportError:
    from cassandra.util import WeakSet  # noqa

from cassandra import DependencyException
try:
    import asyncore
except ModuleNotFoundError:
    raise DependencyException(
        "Unable to import asyncore module.  Note that this module has been removed in Python 3.12 "
        "so when using the driver with this version (or anything newer) you will need to use one of the "
        "other event loop implementations."
    )

from cassandra.connection import Connection, ConnectionShutdown, NONBLOCKING, Timer, TimerManager


log = logging.getLogger(__name__)

_dispatcher_map = {}

def _cleanup(loop):
    if loop:
        loop._cleanup()


class WaitableTimer(Timer):
    def __init__(self, timeout, callback):
        Timer.__init__(self, timeout, callback)
        self.callback = callback
        self.event = Event()

        self.final_exception = None

    def finish(self, time_now):
        try:
            finished = Timer.finish(self, time_now)
            if finished:
                self.event.set()
                return True
            return False

        except Exception as e:
            self.final_exception = e
            self.event.set()
            return True

    def wait(self, timeout=None):
        self.event.wait(timeout)
        if self.final_exception:
            raise self.final_exception


class _PipeWrapper(object):

    def __init__(self, fd):
        self.fd = fd

    def fileno(self):
        return self.fd

    def close(self):
        os.close(self.fd)

    def getsockopt(self, level, optname, buflen=None):
        # act like an unerrored socket for the asyncore error handling
        if level == socket.SOL_SOCKET and optname == socket.SO_ERROR and not buflen:
            return 0
        raise NotImplementedError()


class _AsyncoreDispatcher(asyncore.dispatcher):

    def __init__(self, socket):
        asyncore.dispatcher.__init__(self, map=_dispatcher_map)
        # inject after to avoid base class validation
        self.set_socket(socket)
        self._notified = False

    def writable(self):
        return False

    def validate(self):
        assert not self._notified
        self.notify_loop()
        assert self._notified
        self.loop(0.1)
        assert not self._notified

    def loop(self, timeout):
        asyncore.loop(timeout=timeout, use_poll=True, map=_dispatcher_map, count=1)


class _AsyncorePipeDispatcher(_AsyncoreDispatcher):

    def __init__(self):
        self.read_fd, self.write_fd = os.pipe()
        _AsyncoreDispatcher.__init__(self, _PipeWrapper(self.read_fd))

    def writable(self):
        return False

    def handle_read(self):
        while len(os.read(self.read_fd, 4096)) == 4096:
            pass
        self._notified = False

    def notify_loop(self):
        if not self._notified:
            self._notified = True
            os.write(self.write_fd, b'x')


class _AsyncoreUDPDispatcher(_AsyncoreDispatcher):
    """
    Experimental alternate dispatcher for avoiding busy wait in the asyncore loop. It is not used by default because
    it relies on local port binding.
    Port scanning is not implemented, so multiple clients on one host will collide. This address would need to be set per
    instance, or this could be specialized to scan until an address is found.

    To use::

        from cassandra.io.asyncorereactor import _AsyncoreUDPDispatcher, AsyncoreLoop
        AsyncoreLoop._loop_dispatch_class = _AsyncoreUDPDispatcher

    """
    bind_address = ('localhost', 10000)

    def __init__(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.bind(self.bind_address)
        self._socket.setblocking(0)
        _AsyncoreDispatcher.__init__(self, self._socket)

    def handle_read(self):
        try:
            d = self._socket.recvfrom(1)
            while d and d[1]:
                d = self._socket.recvfrom(1)
        except socket.error as e:
            pass
        self._notified = False

    def notify_loop(self):
        if not self._notified:
            self._notified = True
            self._socket.sendto(b'', self.bind_address)

    def loop(self, timeout):
        asyncore.loop(timeout=timeout, use_poll=False, map=_dispatcher_map, count=1)


class _BusyWaitDispatcher(object):

    max_write_latency = 0.001
    """
    Timeout pushed down to asyncore select/poll. Dictates the amount of time it will sleep before coming back to check
    if anything is writable.
    """

    def notify_loop(self):
        pass

    def loop(self, timeout):
        if not _dispatcher_map:
            time.sleep(0.005)
        count = timeout // self.max_write_latency
        asyncore.loop(timeout=self.max_write_latency, use_poll=True, map=_dispatcher_map, count=count)

    def validate(self):
        pass

    def close(self):
        pass


class AsyncoreLoop(object):

    timer_resolution = 0.1  # used as the max interval to be in the io loop before returning to service timeouts

    _loop_dispatch_class = _AsyncorePipeDispatcher if os.name != 'nt' else _BusyWaitDispatcher

    def __init__(self):
        self._pid = os.getpid()
        self._loop_lock = Lock()
        self._started = False
        self._shutdown = False

        self._thread = None

        self._timers = TimerManager()

        try:
            dispatcher = self._loop_dispatch_class()
            dispatcher.validate()
            log.debug("Validated loop dispatch with %s", self._loop_dispatch_class)
        except Exception:
            log.exception("Failed validating loop dispatch with %s. Using busy wait execution instead.", self._loop_dispatch_class)
            dispatcher.close()
            dispatcher = _BusyWaitDispatcher()
        self._loop_dispatcher = dispatcher

    def maybe_start(self):
        should_start = False
        did_acquire = False
        try:
            did_acquire = self._loop_lock.acquire(False)
            if did_acquire and not self._started:
                self._started = True
                should_start = True
        finally:
            if did_acquire:
                self._loop_lock.release()

        if should_start:
            self._thread = Thread(target=self._run_loop, name="asyncore_cassandra_driver_event_loop")
            self._thread.daemon = True
            self._thread.start()

    def wake_loop(self):
        self._loop_dispatcher.notify_loop()

    def _run_loop(self):
        log.debug("Starting asyncore event loop")
        with self._loop_lock:
            while not self._shutdown:
                try:
                    self._loop_dispatcher.loop(self.timer_resolution)
                    self._timers.service_timeouts()
                except Exception as exc:
                    self._maybe_log_debug("Asyncore event loop stopped unexpectedly", exc_info=exc)
                    break
            self._started = False

        self._maybe_log_debug("Asyncore event loop ended")

    def _maybe_log_debug(self, *args, **kwargs):
        try:
            log.debug(*args, **kwargs)
        except Exception:
            # TODO: Remove when Python 2 support is removed
            # PYTHON-1266. If our logger has disappeared, there's nothing we
            # can do, so just log nothing.
            pass

    def add_timer(self, timer):
        self._timers.add_timer(timer)

        # This function is called from a different thread than the event loop
        # thread, so for this call to be thread safe, we must wake up the loop
        # in case it's stuck at a select
        self.wake_loop()

    def _cleanup(self):
        global _dispatcher_map

        self._shutdown = True
        if not self._thread:
            return

        log.debug("Waiting for event loop thread to join...")
        self._thread.join(timeout=1.0)
        if self._thread.is_alive():
            log.warning(
                "Event loop thread could not be joined, so shutdown may not be clean. "
                "Please call Cluster.shutdown() to avoid this.")

        log.debug("Event loop thread was joined")

        # Ensure all connections are closed and in-flight requests cancelled
        for conn in tuple(_dispatcher_map.values()):
            if conn is not self._loop_dispatcher:
                conn.close()
        self._timers.service_timeouts()
        # Once all the connections are closed, close the dispatcher
        self._loop_dispatcher.close()

        log.debug("Dispatchers were closed")


_global_loop = None
atexit.register(partial(_cleanup, _global_loop))


class AsyncoreConnection(Connection, asyncore.dispatcher):
    """
    An implementation of :class:`.Connection` that uses the ``asyncore``
    module in the Python standard library for its event loop.
    """

    _writable = False
    _readable = False

    @classmethod
    def initialize_reactor(cls):
        global _global_loop
        if not _global_loop:
            _global_loop = AsyncoreLoop()
        else:
            current_pid = os.getpid()
            if _global_loop._pid != current_pid:
                log.debug("Detected fork, clearing and reinitializing reactor state")
                cls.handle_fork()
                _global_loop = AsyncoreLoop()

    @classmethod
    def handle_fork(cls):
        global _dispatcher_map, _global_loop
        _dispatcher_map = {}
        if _global_loop:
            _global_loop._cleanup()
            _global_loop = None

    @classmethod
    def create_timer(cls, timeout, callback):
        timer = Timer(timeout, callback)
        _global_loop.add_timer(timer)
        return timer

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)

        self.deque = deque()
        self.deque_lock = Lock()

        self._connect_socket()

        # start the event loop if needed
        _global_loop.maybe_start()

        init_handler = WaitableTimer(
            timeout=0,
            callback=partial(asyncore.dispatcher.__init__,
                             self, self._socket, _dispatcher_map)
        )
        _global_loop.add_timer(init_handler)
        init_handler.wait(kwargs["connect_timeout"])

        self._writable = True
        self._readable = True

        self._send_options_message()

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s", id(self), self.endpoint)
        self._writable = False
        self._readable = False

        # We don't have to wait for this to be closed, we can just schedule it
        self.create_timer(0, partial(asyncore.dispatcher.close, self))

        log.debug("Closed socket to %s", self.endpoint)

        if not self.is_defunct:
            self.error_all_requests(
                ConnectionShutdown("Connection to %s was closed" % self.endpoint))

            #This happens when the connection is shutdown while waiting for the ReadyMessage
            if not self.connected_event.is_set():
                self.last_error = ConnectionShutdown("Connection to %s was closed" % self.endpoint)

            # don't leave in-progress operations hanging
            self.connected_event.set()

    def handle_error(self):
        self.defunct(sys.exc_info()[1])

    def handle_close(self):
        log.debug("Connection %s closed by server", self)
        self.close()

    def handle_write(self):
        while True:
            with self.deque_lock:
                try:
                    next_msg = self.deque.popleft()
                except IndexError:
                    self._writable = False
                    return

            try:
                sent = self.send(next_msg)
                self._readable = True
            except socket.error as err:
                if (err.args[0] in NONBLOCKING or
                        err.args[0] in (ssl.SSL_ERROR_WANT_READ, ssl.SSL_ERROR_WANT_WRITE)):
                    with self.deque_lock:
                        self.deque.appendleft(next_msg)
                else:
                    self.defunct(err)
                return
            else:
                if sent < len(next_msg):
                    with self.deque_lock:
                        self.deque.appendleft(next_msg[sent:])
                    if sent == 0:
                        return

    def handle_read(self):
        try:
            while True:
                buf = self.recv(self.in_buffer_size)
                self._iobuf.write(buf)
                if len(buf) < self.in_buffer_size:
                    break
        except socket.error as err:
            if isinstance(err, ssl.SSLError):
                if err.args[0] in (ssl.SSL_ERROR_WANT_READ, ssl.SSL_ERROR_WANT_WRITE):
                    if not self._iobuf.tell():
                        return
                else:
                    self.defunct(err)
                    return
            elif err.args[0] in NONBLOCKING:
                if not self._iobuf.tell():
                    return
            else:
                self.defunct(err)
                return

        if self._iobuf.tell():
            self.process_io_buffer()
            if not self._requests and not self.is_control_connection:
                self._readable = False

    def push(self, data):
        sabs = self.out_buffer_size
        if len(data) > sabs:
            chunks = []
            for i in range(0, len(data), sabs):
                chunks.append(data[i:i + sabs])
        else:
            chunks = [data]

        with self.deque_lock:
            self.deque.extend(chunks)
            self._writable = True
        _global_loop.wake_loop()

    def writable(self):
        return self._writable

    def readable(self):
        return self._readable or ((self.is_control_connection or self._continuous_paging_sessions) and not (self.is_defunct or self.is_closed))
