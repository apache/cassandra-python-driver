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
import gevent
import gevent.event
from gevent.queue import Queue
from gevent import socket
import gevent.ssl

import logging
import time


from cassandra.connection import Connection, ConnectionShutdown, Timer, TimerManager


log = logging.getLogger(__name__)


class GeventConnection(Connection):
    """
    An implementation of :class:`.Connection` that utilizes ``gevent``.

    This implementation assumes all gevent monkey patching is active. It is not tested with partial patching.
    """

    _read_watcher = None
    _write_watcher = None

    _socket_impl = gevent.socket
    _ssl_impl = gevent.ssl

    _timers = None
    _timeout_watcher = None
    _new_timer = None

    @classmethod
    def initialize_reactor(cls):
        if not cls._timers:
            cls._timers = TimerManager()
            cls._timeout_watcher = gevent.spawn(cls.service_timeouts)
            cls._new_timer = gevent.event.Event()

    @classmethod
    def create_timer(cls, timeout, callback):
        timer = Timer(timeout, callback)
        cls._timers.add_timer(timer)
        cls._new_timer.set()
        return timer

    @classmethod
    def service_timeouts(cls):
        timer_manager = cls._timers
        timer_event = cls._new_timer
        while True:
            next_end = timer_manager.service_timeouts()
            sleep_time = max(next_end - time.time(), 0) if next_end else 10000
            timer_event.wait(sleep_time)
            timer_event.clear()

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)

        self._write_queue = Queue()

        self._connect_socket()

        self._read_watcher = gevent.spawn(self.handle_read)
        self._write_watcher = gevent.spawn(self.handle_write)
        self._send_options_message()

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s" % (id(self), self.endpoint))
        if self._read_watcher:
            self._read_watcher.kill(block=False)
        if self._write_watcher:
            self._write_watcher.kill(block=False)
        if self._socket:
            self._socket.close()
        log.debug("Closed socket to %s" % (self.endpoint,))

        if not self.is_defunct:
            self.error_all_requests(
                ConnectionShutdown("Connection to %s was closed" % self.endpoint))
            # don't leave in-progress operations hanging
            self.connected_event.set()

    def handle_close(self):
        log.debug("connection closed by server")
        self.close()

    def handle_write(self):
        while True:
            try:
                next_msg = self._write_queue.get()
                self._socket.sendall(next_msg)
            except socket.error as err:
                log.debug("Exception in send for %s: %s", self, err)
                self.defunct(err)
                return

    def handle_read(self):
        while True:
            try:
                buf = self._socket.recv(self.in_buffer_size)
                self._iobuf.write(buf)
            except socket.error as err:
                log.debug("Exception in read for %s: %s", self, err)
                self.defunct(err)
                return  # leave the read loop

            if buf and self._iobuf.tell():
                self.process_io_buffer()
            else:
                log.debug("Connection %s closed by server", self)
                self.close()
                return

    def push(self, data):
        chunk_size = self.out_buffer_size
        for i in range(0, len(data), chunk_size):
            self._write_queue.put(data[i:i + chunk_size])
