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
Connection pooling and host management.
"""

from functools import total_ordering
import logging
import socket
import time
from threading import Lock, RLock, Condition
import weakref
try:
    from weakref import WeakSet
except ImportError:
    from cassandra.util import WeakSet  # NOQA

from cassandra import AuthenticationFailed
from cassandra.connection import ConnectionException, EndPoint, DefaultEndPoint
from cassandra.policies import HostDistance

log = logging.getLogger(__name__)


class NoConnectionsAvailable(Exception):
    """
    All existing connections to a given host are busy, or there are
    no open connections.
    """
    pass


@total_ordering
class Host(object):
    """
    Represents a single Cassandra node.
    """

    endpoint = None
    """
    The :class:`~.connection.EndPoint` to connect to the node.
    """

    broadcast_address = None
    """
    broadcast address configured for the node, *if available*:

    'system.local.broadcast_address' or 'system.peers.peer' (Cassandra 2-3)
    'system.local.broadcast_address' or 'system.peers_v2.peer' (Cassandra 4)

    This is not present in the ``system.local`` table for older versions of Cassandra. It
    is also not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    broadcast_port = None
    """
    broadcast port configured for the node, *if available*:

    'system.local.broadcast_port' or 'system.peers_v2.peer_port' (Cassandra 4)

    It is also not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    broadcast_rpc_address = None
    """
    The broadcast rpc address of the node:

    'system.local.rpc_address' or 'system.peers.rpc_address' (Cassandra 3)
    'system.local.rpc_address' or 'system.peers.native_transport_address (DSE  6+)'
    'system.local.rpc_address' or 'system.peers_v2.native_address (Cassandra 4)'
    """

    broadcast_rpc_port = None
    """
    The broadcast rpc port of the node, *if available*:
    
    'system.local.rpc_port' or 'system.peers.native_transport_port' (DSE 6+)
    'system.local.rpc_port' or 'system.peers_v2.native_port' (Cassandra 4)
    """

    listen_address = None
    """
    listen address configured for the node, *if available*:

    'system.local.listen_address'

    This is only available in the ``system.local`` table for newer versions of Cassandra. It is also not
    queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``. Usually the same as ``broadcast_address``
    unless configured differently in cassandra.yaml.
    """

    listen_port = None
    """
    listen port configured for the node, *if available*:

    'system.local.listen_port'

    This is only available in the ``system.local`` table for newer versions of Cassandra. It is also not
    queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    conviction_policy = None
    """
    A :class:`~.ConvictionPolicy` instance for determining when this node should
    be marked up or down.
    """

    is_up = None
    """
    :const:`True` if the node is considered up, :const:`False` if it is
    considered down, and :const:`None` if it is not known if the node is
    up or down.
    """

    release_version = None
    """
    release_version as queried from the control connection system tables
    """

    host_id = None
    """
    The unique identifier of the cassandra node
    """

    dse_version = None
    """
    dse_version as queried from the control connection system tables. Only populated when connecting to
    DSE with this property available. Not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    dse_workload = None
    """
    DSE workload queried from the control connection system tables. Only populated when connecting to
    DSE with this property available. Not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    This is a legacy attribute that does not portray multiple workloads in a uniform fashion.
    See also :attr:`~.Host.dse_workloads`.
    """

    dse_workloads = None
    """
    DSE workloads set, queried from the control connection system tables. Only populated when connecting to
    DSE with this property available (added in DSE 5.1).
    Not queried if :attr:`~.Cluster.token_metadata_enabled` is ``False``.
    """

    _datacenter = None
    _rack = None
    _reconnection_handler = None
    lock = None

    _currently_handling_node_up = False

    def __init__(self, endpoint, conviction_policy_factory, datacenter=None, rack=None, host_id=None):
        if endpoint is None:
            raise ValueError("endpoint may not be None")
        if conviction_policy_factory is None:
            raise ValueError("conviction_policy_factory may not be None")

        self.endpoint = endpoint if isinstance(endpoint, EndPoint) else DefaultEndPoint(endpoint)
        self.conviction_policy = conviction_policy_factory(self)
        self.host_id = host_id
        self.set_location_info(datacenter, rack)
        self.lock = RLock()

    @property
    def address(self):
        """
        The IP address of the endpoint. This is the RPC address the driver uses when connecting to the node.
        """
        # backward compatibility
        return self.endpoint.address

    @property
    def datacenter(self):
        """ The datacenter the node is in.  """
        return self._datacenter

    @property
    def rack(self):
        """ The rack the node is in.  """
        return self._rack

    def set_location_info(self, datacenter, rack):
        """
        Sets the datacenter and rack for this node. Intended for internal
        use (by the control connection, which periodically checks the
        ring topology) only.
        """
        self._datacenter = datacenter
        self._rack = rack

    def set_up(self):
        if not self.is_up:
            log.debug("Host %s is now marked up", self.endpoint)
        self.conviction_policy.reset()
        self.is_up = True

    def set_down(self):
        self.is_up = False

    def signal_connection_failure(self, connection_exc):
        return self.conviction_policy.add_failure(connection_exc)

    def is_currently_reconnecting(self):
        return self._reconnection_handler is not None

    def get_and_set_reconnection_handler(self, new_handler):
        """
        Atomically replaces the reconnection handler for this
        host.  Intended for internal use only.
        """
        with self.lock:
            old = self._reconnection_handler
            self._reconnection_handler = new_handler
            return old

    def __eq__(self, other):
        if isinstance(other, Host):
            return self.endpoint == other.endpoint
        else:  # TODO Backward compatibility, remove next major
            return self.endpoint.address == other

    def __hash__(self):
        return hash(self.endpoint)

    def __lt__(self, other):
        return self.endpoint < other.endpoint

    def __str__(self):
        return str(self.endpoint)

    def __repr__(self):
        dc = (" %s" % (self._datacenter,)) if self._datacenter else ""
        return "<%s: %s%s>" % (self.__class__.__name__, self.endpoint, dc)


class _ReconnectionHandler(object):
    """
    Abstract class for attempting reconnections with a given
    schedule and scheduler.
    """

    _cancelled = False

    def __init__(self, scheduler, schedule, callback, *callback_args, **callback_kwargs):
        self.scheduler = scheduler
        self.schedule = schedule
        self.callback = callback
        self.callback_args = callback_args
        self.callback_kwargs = callback_kwargs

    def start(self):
        if self._cancelled:
            log.debug("Reconnection handler was cancelled before starting")
            return

        first_delay = next(self.schedule)
        self.scheduler.schedule(first_delay, self.run)

    def run(self):
        if self._cancelled:
            return

        conn = None
        try:
            conn = self.try_reconnect()
        except Exception as exc:
            try:
                next_delay = next(self.schedule)
            except StopIteration:
                # the schedule has been exhausted
                next_delay = None

            # call on_exception for logging purposes even if next_delay is None
            if self.on_exception(exc, next_delay):
                if next_delay is None:
                    log.warning(
                        "Will not continue to retry reconnection attempts "
                        "due to an exhausted retry schedule")
                else:
                    self.scheduler.schedule(next_delay, self.run)
        else:
            if not self._cancelled:
                self.on_reconnection(conn)
                self.callback(*(self.callback_args), **(self.callback_kwargs))
        finally:
            if conn:
                conn.close()

    def cancel(self):
        self._cancelled = True

    def try_reconnect(self):
        """
        Subclasses must implement this method.  It should attempt to
        open a new Connection and return it; if a failure occurs, an
        Exception should be raised.
        """
        raise NotImplementedError()

    def on_reconnection(self, connection):
        """
        Called when a new Connection is successfully opened.  Nothing is
        done by default.
        """
        pass

    def on_exception(self, exc, next_delay):
        """
        Called when an Exception is raised when trying to connect.
        `exc` is the Exception that was raised and `next_delay` is the
        number of seconds (as a float) that the handler will wait before
        attempting to connect again.

        Subclasses should return :const:`False` if no more attempts to
        connection should be made, :const:`True` otherwise.  The default
        behavior is to always retry unless the error is an
        :exc:`.AuthenticationFailed` instance.
        """
        if isinstance(exc, AuthenticationFailed):
            return False
        else:
            return True


class _HostReconnectionHandler(_ReconnectionHandler):

    def __init__(self, host, connection_factory, is_host_addition, on_add, on_up, *args, **kwargs):
        _ReconnectionHandler.__init__(self, *args, **kwargs)
        self.is_host_addition = is_host_addition
        self.on_add = on_add
        self.on_up = on_up
        self.host = host
        self.connection_factory = connection_factory

    def try_reconnect(self):
        return self.connection_factory()

    def on_reconnection(self, connection):
        log.info("Successful reconnection to %s, marking node up if it isn't already", self.host)
        if self.is_host_addition:
            self.on_add(self.host)
        else:
            self.on_up(self.host)

    def on_exception(self, exc, next_delay):
        if isinstance(exc, AuthenticationFailed):
            return False
        else:
            log.warning("Error attempting to reconnect to %s, scheduling retry in %s seconds: %s",
                        self.host, next_delay, exc)
            log.debug("Reconnection error details", exc_info=True)
            return True


class HostConnection(object):
    """
    When using v3 of the native protocol, this is used instead of a connection
    pool per host (HostConnectionPool) due to the increased in-flight capacity
    of individual connections.
    """

    host = None
    host_distance = None
    is_shutdown = False
    shutdown_on_error = False

    _session = None
    _connection = None
    _lock = None
    _keyspace = None

    def __init__(self, host, host_distance, session):
        self.host = host
        self.host_distance = host_distance
        self._session = weakref.proxy(session)
        self._lock = Lock()
        # this is used in conjunction with the connection streams. Not using the connection lock because the connection can be replaced in the lifetime of the pool.
        self._stream_available_condition = Condition(self._lock)
        self._is_replacing = False
        # Contains connections which shouldn't be used anymore
        # and are waiting until all requests time out or complete
        # so that we can dispose of them.
        self._trash = set()

        if host_distance == HostDistance.IGNORED:
            log.debug("Not opening connection to ignored host %s", self.host)
            return
        elif host_distance == HostDistance.REMOTE and not session.cluster.connect_to_remote_hosts:
            log.debug("Not opening connection to remote host %s", self.host)
            return

        log.debug("Initializing connection for host %s", self.host)
        self._connection = session.cluster.connection_factory(host.endpoint, on_orphaned_stream_released=self.on_orphaned_stream_released)
        self._keyspace = session.keyspace
        if self._keyspace:
            self._connection.set_keyspace_blocking(self._keyspace)
        log.debug("Finished initializing connection for host %s", self.host)

    def _get_connection(self):
        if self.is_shutdown:
            raise ConnectionException(
                "Pool for %s is shutdown" % (self.host,), self.host)

        conn = self._connection
        if not conn:
            raise NoConnectionsAvailable()
        return conn

    def borrow_connection(self, timeout):
        conn = self._get_connection()
        if conn.orphaned_threshold_reached:
            with self._lock:
                if not self._is_replacing:
                    self._is_replacing = True
                    self._session.submit(self._replace, conn)
                    log.debug(
                        "Connection to host %s reached orphaned stream limit, replacing...",
                        self.host
                    )

        start = time.time()
        remaining = timeout
        while True:
            with conn.lock:
                if not (conn.orphaned_threshold_reached and conn.is_closed) and conn.in_flight < conn.max_request_id:
                    conn.in_flight += 1
                    return conn, conn.get_request_id()
            if timeout is not None:
                remaining = timeout - time.time() + start
                if remaining < 0:
                    break
            with self._stream_available_condition:
                if conn.orphaned_threshold_reached and conn.is_closed:
                    conn = self._get_connection()
                else:
                    self._stream_available_condition.wait(remaining)

        raise NoConnectionsAvailable("All request IDs are currently in use")

    def return_connection(self, connection, stream_was_orphaned=False):
        if not stream_was_orphaned:
            with connection.lock:
                connection.in_flight -= 1
            with self._stream_available_condition:
                self._stream_available_condition.notify()

        if connection.is_defunct or connection.is_closed:
            if connection.signaled_error and not self.shutdown_on_error:
                return

            is_down = False
            if not connection.signaled_error:
                log.debug("Defunct or closed connection (%s) returned to pool, potentially "
                          "marking host %s as down", id(connection), self.host)
                is_down = self._session.cluster.signal_connection_failure(
                    self.host, connection.last_error, is_host_addition=False)
                connection.signaled_error = True

            if self.shutdown_on_error and not is_down:
                is_down = True
                self._session.cluster.on_down(self.host, is_host_addition=False)

            if is_down:
                self.shutdown()
            else:
                self._connection = None
                with self._lock:
                    if self._is_replacing:
                        return
                    self._is_replacing = True
                    self._session.submit(self._replace, connection)
        else:
            if connection in self._trash:
                with connection.lock:
                    if connection.in_flight == len(connection.orphaned_request_ids):
                        with self._lock:
                            if connection in self._trash:
                                self._trash.remove(connection)
                                log.debug("Closing trashed connection (%s) to %s", id(connection), self.host)
                                connection.close()
                return

    def on_orphaned_stream_released(self):
        """
        Called when a response for an orphaned stream (timed out on the client
        side) was received.
        """
        with self._stream_available_condition:
            self._stream_available_condition.notify()

    def _replace(self, connection):
        with self._lock:
            if self.is_shutdown:
                return

        log.debug("Replacing connection (%s) to %s", id(connection), self.host)
        try:
            conn = self._session.cluster.connection_factory(self.host.endpoint, on_orphaned_stream_released=self.on_orphaned_stream_released)
            if self._keyspace:
                conn.set_keyspace_blocking(self._keyspace)
            self._connection = conn
        except Exception:
            log.warning("Failed reconnecting %s. Retrying." % (self.host.endpoint,))
            self._session.submit(self._replace, connection)
        else:
            with connection.lock:
                with self._lock:
                    if connection.orphaned_threshold_reached:
                        if connection.in_flight == len(connection.orphaned_request_ids):
                            connection.close()
                        else:
                            self._trash.add(connection)
                    self._is_replacing = False
                    self._stream_available_condition.notify()

    def shutdown(self):
        with self._lock:
            if self.is_shutdown:
                return
            else:
                self.is_shutdown = True
            self._stream_available_condition.notify_all()

        if self._connection:
            self._connection.close()
            self._connection = None

        trash_conns = None
        with self._lock:
            if self._trash:
                trash_conns = self._trash
                self._trash = set()

        if trash_conns is not None:
            for conn in self._trash:
                conn.close()

    def _set_keyspace_for_all_conns(self, keyspace, callback):
        if self.is_shutdown or not self._connection:
            return

        def connection_finished_setting_keyspace(conn, error):
            self.return_connection(conn)
            errors = [] if not error else [error]
            callback(self, errors)

        self._keyspace = keyspace
        self._connection.set_keyspace_async(keyspace, connection_finished_setting_keyspace)

    def get_connections(self):
        c = self._connection
        return [c] if c else []

    def get_state(self):
        connection = self._connection
        open_count = 1 if connection and not (connection.is_closed or connection.is_defunct) else 0
        in_flights = [connection.in_flight] if connection else []
        orphan_requests = [connection.orphaned_request_ids] if connection else []
        return {'shutdown': self.is_shutdown, 'open_count': open_count, \
            'in_flights': in_flights, 'orphan_requests': orphan_requests}

    @property
    def open_count(self):
        connection = self._connection
        return 1 if connection and not (connection.is_closed or connection.is_defunct) else 0

_MAX_SIMULTANEOUS_CREATION = 1
_MIN_TRASH_INTERVAL = 10


class HostConnectionPool(object):
    """
    Used to pool connections to a host for v1 and v2 native protocol.
    """

    host = None
    host_distance = None

    is_shutdown = False
    open_count = 0
    _scheduled_for_creation = 0
    _next_trash_allowed_at = 0
    _keyspace = None

    def __init__(self, host, host_distance, session):
        self.host = host
        self.host_distance = host_distance

        self._session = weakref.proxy(session)
        self._lock = RLock()
        self._conn_available_condition = Condition()

        log.debug("Initializing new connection pool for host %s", self.host)
        core_conns = session.cluster.get_core_connections_per_host(host_distance)
        self._connections = [session.cluster.connection_factory(host.endpoint, on_orphaned_stream_released=self.on_orphaned_stream_released)
                             for i in range(core_conns)]

        self._keyspace = session.keyspace
        if self._keyspace:
            for conn in self._connections:
                conn.set_keyspace_blocking(self._keyspace)

        self._trash = set()
        self._next_trash_allowed_at = time.time()
        self.open_count = core_conns
        log.debug("Finished initializing new connection pool for host %s", self.host)

    def borrow_connection(self, timeout):
        if self.is_shutdown:
            raise ConnectionException(
                "Pool for %s is shutdown" % (self.host,), self.host)

        conns = self._connections
        if not conns:
            # handled specially just for simpler code
            log.debug("Detected empty pool, opening core conns to %s", self.host)
            core_conns = self._session.cluster.get_core_connections_per_host(self.host_distance)
            with self._lock:
                # we check the length of self._connections again
                # along with self._scheduled_for_creation while holding the lock
                # in case multiple threads hit this condition at the same time
                to_create = core_conns - (len(self._connections) + self._scheduled_for_creation)
                for i in range(to_create):
                    self._scheduled_for_creation += 1
                    self._session.submit(self._create_new_connection)

            # in_flight is incremented by wait_for_conn
            conn = self._wait_for_conn(timeout)
            return conn
        else:
            # note: it would be nice to push changes to these config settings
            # to pools instead of doing a new lookup on every
            # borrow_connection() call
            max_reqs = self._session.cluster.get_max_requests_per_connection(self.host_distance)
            max_conns = self._session.cluster.get_max_connections_per_host(self.host_distance)

            least_busy = min(conns, key=lambda c: c.in_flight)
            request_id = None
            # to avoid another thread closing this connection while
            # trashing it (through the return_connection process), hold
            # the connection lock from this point until we've incremented
            # its in_flight count
            need_to_wait = False
            with least_busy.lock:
                if least_busy.in_flight < least_busy.max_request_id:
                    least_busy.in_flight += 1
                    request_id = least_busy.get_request_id()
                else:
                    # once we release the lock, wait for another connection
                    need_to_wait = True

            if need_to_wait:
                # wait_for_conn will increment in_flight on the conn
                least_busy, request_id = self._wait_for_conn(timeout)

            # if we have too many requests on this connection, but we still
            # have space to open a new connection against this host, go ahead
            # and schedule the creation of a new connection
            if least_busy.in_flight >= max_reqs and len(self._connections) < max_conns:
                self._maybe_spawn_new_connection()

            return least_busy, request_id

    def _maybe_spawn_new_connection(self):
        with self._lock:
            if self._scheduled_for_creation >= _MAX_SIMULTANEOUS_CREATION:
                return
            if self.open_count >= self._session.cluster.get_max_connections_per_host(self.host_distance):
                return
            self._scheduled_for_creation += 1

        log.debug("Submitting task for creation of new Connection to %s", self.host)
        self._session.submit(self._create_new_connection)

    def _create_new_connection(self):
        try:
            self._add_conn_if_under_max()
        except (ConnectionException, socket.error) as exc:
            log.warning("Failed to create new connection to %s: %s", self.host, exc)
        except Exception:
            log.exception("Unexpectedly failed to create new connection")
        finally:
            with self._lock:
                self._scheduled_for_creation -= 1

    def _add_conn_if_under_max(self):
        max_conns = self._session.cluster.get_max_connections_per_host(self.host_distance)
        with self._lock:
            if self.is_shutdown:
                return True

            if self.open_count >= max_conns:
                return True

            self.open_count += 1

        log.debug("Going to open new connection to host %s", self.host)
        try:
            conn = self._session.cluster.connection_factory(self.host.endpoint, on_orphaned_stream_released=self.on_orphaned_stream_released)
            if self._keyspace:
                conn.set_keyspace_blocking(self._session.keyspace)
            self._next_trash_allowed_at = time.time() + _MIN_TRASH_INTERVAL
            with self._lock:
                new_connections = self._connections[:] + [conn]
                self._connections = new_connections
            log.debug("Added new connection (%s) to pool for host %s, signaling availability",
                      id(conn), self.host)
            self._signal_available_conn()
            return True
        except (ConnectionException, socket.error) as exc:
            log.warning("Failed to add new connection to pool for host %s: %s", self.host, exc)
            with self._lock:
                self.open_count -= 1
            if self._session.cluster.signal_connection_failure(self.host, exc, is_host_addition=False):
                self.shutdown()
            return False
        except AuthenticationFailed:
            with self._lock:
                self.open_count -= 1
            return False

    def _await_available_conn(self, timeout):
        with self._conn_available_condition:
            self._conn_available_condition.wait(timeout)

    def _signal_available_conn(self):
        with self._conn_available_condition:
            self._conn_available_condition.notify()

    def _signal_all_available_conn(self):
        with self._conn_available_condition:
            self._conn_available_condition.notify_all()

    def _wait_for_conn(self, timeout):
        start = time.time()
        remaining = timeout

        while remaining > 0:
            # wait on our condition for the possibility that a connection
            # is useable
            self._await_available_conn(remaining)

            # self.shutdown() may trigger the above Condition
            if self.is_shutdown:
                raise ConnectionException("Pool is shutdown")

            conns = self._connections
            if conns:
                least_busy = min(conns, key=lambda c: c.in_flight)
                with least_busy.lock:
                    if least_busy.in_flight < least_busy.max_request_id:
                        least_busy.in_flight += 1
                        return least_busy, least_busy.get_request_id()

            remaining = timeout - (time.time() - start)

        raise NoConnectionsAvailable()

    def return_connection(self, connection, stream_was_orphaned=False):
        with connection.lock:
            if not stream_was_orphaned:
                connection.in_flight -= 1
            in_flight = connection.in_flight

        if connection.is_defunct or connection.is_closed:
            if not connection.signaled_error:
                log.debug("Defunct or closed connection (%s) returned to pool, potentially "
                          "marking host %s as down", id(connection), self.host)
                is_down = self._session.cluster.signal_connection_failure(
                    self.host, connection.last_error, is_host_addition=False)
                connection.signaled_error = True
                if is_down:
                    self.shutdown()
                else:
                    self._replace(connection)
        else:
            if connection in self._trash:
                with connection.lock:
                    if connection.in_flight == 0:
                        with self._lock:
                            if connection in self._trash:
                                self._trash.remove(connection)
                        log.debug("Closing trashed connection (%s) to %s", id(connection), self.host)
                        connection.close()
                return

            core_conns = self._session.cluster.get_core_connections_per_host(self.host_distance)
            min_reqs = self._session.cluster.get_min_requests_per_connection(self.host_distance)
            # we can use in_flight here without holding the connection lock
            # because the fact that in_flight dipped below the min at some
            # point is enough to start the trashing procedure
            if len(self._connections) > core_conns and in_flight <= min_reqs and \
                    time.time() >= self._next_trash_allowed_at:
                self._maybe_trash_connection(connection)
            else:
                self._signal_available_conn()

    def on_orphaned_stream_released(self):
        """
        Called when a response for an orphaned stream (timed out on the client
        side) was received.
        """
        self._signal_available_conn()

    def _maybe_trash_connection(self, connection):
        core_conns = self._session.cluster.get_core_connections_per_host(self.host_distance)
        did_trash = False
        with self._lock:
            if connection not in self._connections:
                return

            if self.open_count > core_conns:
                did_trash = True
                self.open_count -= 1
                new_connections = self._connections[:]
                new_connections.remove(connection)
                self._connections = new_connections

                with connection.lock:
                    if connection.in_flight == 0:
                        log.debug("Skipping trash and closing unused connection (%s) to %s", id(connection), self.host)
                        connection.close()

                        # skip adding it to the trash if we're already closing it
                        return

                self._trash.add(connection)

        if did_trash:
            self._next_trash_allowed_at = time.time() + _MIN_TRASH_INTERVAL
            log.debug("Trashed connection (%s) to %s", id(connection), self.host)

    def _replace(self, connection):
        should_replace = False
        with self._lock:
            if connection in self._connections:
                new_connections = self._connections[:]
                new_connections.remove(connection)
                self._connections = new_connections
                self.open_count -= 1
                should_replace = True

        if should_replace:
            log.debug("Replacing connection (%s) to %s", id(connection), self.host)
            connection.close()
            self._session.submit(self._retrying_replace)
        else:
            log.debug("Closing connection (%s) to %s", id(connection), self.host)
            connection.close()

    def _retrying_replace(self):
        replaced = False
        try:
            replaced = self._add_conn_if_under_max()
        except Exception:
            log.exception("Failed replacing connection to %s", self.host)
        if not replaced:
            log.debug("Failed replacing connection to %s. Retrying.", self.host)
            self._session.submit(self._retrying_replace)

    def shutdown(self):
        with self._lock:
            if self.is_shutdown:
                return
            else:
                self.is_shutdown = True

        self._signal_all_available_conn()
        for conn in self._connections:
            conn.close()
            self.open_count -= 1

        for conn in self._trash:
            conn.close()

    def ensure_core_connections(self):
        if self.is_shutdown:
            return

        core_conns = self._session.cluster.get_core_connections_per_host(self.host_distance)
        with self._lock:
            to_create = core_conns - (len(self._connections) + self._scheduled_for_creation)
            for i in range(to_create):
                self._scheduled_for_creation += 1
                self._session.submit(self._create_new_connection)

    def _set_keyspace_for_all_conns(self, keyspace, callback):
        """
        Asynchronously sets the keyspace for all connections.  When all
        connections have been set, `callback` will be called with two
        arguments: this pool, and a list of any errors that occurred.
        """
        remaining_callbacks = set(self._connections)
        errors = []

        if not remaining_callbacks:
            callback(self, errors)
            return

        def connection_finished_setting_keyspace(conn, error):
            self.return_connection(conn)
            remaining_callbacks.remove(conn)
            if error:
                errors.append(error)

            if not remaining_callbacks:
                callback(self, errors)

        self._keyspace = keyspace
        for conn in self._connections:
            conn.set_keyspace_async(keyspace, connection_finished_setting_keyspace)

    def get_connections(self):
        return self._connections

    def get_state(self):
        in_flights = [c.in_flight for c in self._connections]
        orphan_requests = [c.orphaned_request_ids for c in self._connections]
        return {'shutdown': self.is_shutdown, 'open_count': self.open_count, \
            'in_flights': in_flights, 'orphan_requests': orphan_requests}
