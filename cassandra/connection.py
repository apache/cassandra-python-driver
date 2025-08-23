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

from __future__ import absolute_import  # to enable import io from stdlib
from collections import defaultdict, deque
import errno
from functools import wraps, partial, total_ordering
from heapq import heappush, heappop
import io
import logging
import socket
import struct
import sys
from threading import Thread, Event, RLock, Condition
import time
import ssl
import weakref


if 'gevent.monkey' in sys.modules:
    from gevent.queue import Queue, Empty
else:
    from queue import Queue, Empty  # noqa

from cassandra import ConsistencyLevel, AuthenticationFailed, OperationTimedOut, ProtocolVersion
from cassandra.marshal import int32_pack
from cassandra.protocol import (ReadyMessage, AuthenticateMessage, OptionsMessage,
                                StartupMessage, ErrorMessage, CredentialsMessage,
                                QueryMessage, ResultMessage, ProtocolHandler,
                                InvalidRequestException, SupportedMessage,
                                AuthResponseMessage, AuthChallengeMessage,
                                AuthSuccessMessage, ProtocolException,
                                RegisterMessage, ReviseRequestMessage)
from cassandra.segment import SegmentCodec, CrcException
from cassandra.util import OrderedDict


log = logging.getLogger(__name__)

segment_codec_no_compression = SegmentCodec()
segment_codec_lz4 = None

# We use an ordered dictionary and specifically add lz4 before
# snappy so that lz4 will be preferred. Changing the order of this
# will change the compression preferences for the driver.
locally_supported_compressions = OrderedDict()

try:
    import lz4
except ImportError:
    pass
else:
    # The compress and decompress functions we need were moved from the lz4 to
    # the lz4.block namespace, so we try both here.
    try:
        from lz4 import block as lz4_block
    except ImportError:
        lz4_block = lz4

    try:
        lz4_block.compress
        lz4_block.decompress
    except AttributeError:
        raise ImportError(
            'lz4 not imported correctly. Imported object should have '
            '.compress and and .decompress attributes but does not. '
            'Please file a bug report on JIRA. (Imported object was '
            '{lz4_block})'.format(lz4_block=repr(lz4_block))
        )

    # Cassandra writes the uncompressed message length in big endian order,
    # but the lz4 lib requires little endian order, so we wrap these
    # functions to handle that

    def lz4_compress(byts):
        # write length in big-endian instead of little-endian
        return int32_pack(len(byts)) + lz4_block.compress(byts)[4:]

    def lz4_decompress(byts):
        # flip from big-endian to little-endian
        return lz4_block.decompress(byts[3::-1] + byts[4:])

    locally_supported_compressions['lz4'] = (lz4_compress, lz4_decompress)
    segment_codec_lz4 = SegmentCodec(lz4_compress, lz4_decompress)

try:
    import snappy
except ImportError:
    pass
else:
    # work around apparently buggy snappy decompress
    def decompress(byts):
        if byts == '\x00':
            return ''
        return snappy.decompress(byts)
    locally_supported_compressions['snappy'] = (snappy.compress, decompress)

DRIVER_NAME, DRIVER_VERSION = 'DataStax Python Driver', sys.modules['cassandra'].__version__

PROTOCOL_VERSION_MASK = 0x7f

HEADER_DIRECTION_FROM_CLIENT = 0x00
HEADER_DIRECTION_TO_CLIENT = 0x80
HEADER_DIRECTION_MASK = 0x80

frame_header_v1_v2 = struct.Struct('>BbBi')
frame_header_v3 = struct.Struct('>BhBi')


class EndPoint(object):
    """
    Represents the information to connect to a cassandra node.
    """

    @property
    def address(self):
        """
        The IP address of the node. This is the RPC address the driver uses when connecting to the node
        """
        raise NotImplementedError()

    @property
    def port(self):
        """
        The port of the node.
        """
        raise NotImplementedError()

    @property
    def ssl_options(self):
        """
        SSL options specific to this endpoint.
        """
        return None

    @property
    def socket_family(self):
        """
        The socket family of the endpoint.
        """
        return socket.AF_UNSPEC

    def resolve(self):
        """
        Resolve the endpoint to an address/port. This is called
        only on socket connection.
        """
        raise NotImplementedError()


class EndPointFactory(object):

    cluster = None

    def configure(self, cluster):
        """
        This is called by the cluster during its initialization.
        """
        self.cluster = cluster
        return self

    def create(self, row):
        """
        Create an EndPoint from a system.peers row.
        """
        raise NotImplementedError()


@total_ordering
class DefaultEndPoint(EndPoint):
    """
    Default EndPoint implementation, basically just an address and port.
    """

    def __init__(self, address, port=9042):
        self._address = address
        self._port = port

    @property
    def address(self):
        return self._address

    @property
    def port(self):
        return self._port

    def resolve(self):
        return self._address, self._port

    def __eq__(self, other):
        return isinstance(other, DefaultEndPoint) and \
               self.address == other.address and self.port == other.port

    def __hash__(self):
        return hash((self.address, self.port))

    def __lt__(self, other):
        return (self.address, self.port) < (other.address, other.port)

    def __str__(self):
        return str("%s:%d" % (self.address, self.port))

    def __repr__(self):
        return "<%s: %s:%d>" % (self.__class__.__name__, self.address, self.port)


class DefaultEndPointFactory(EndPointFactory):

    port = None
    """
    If no port is discovered in the row, this is the default port
    used for endpoint creation. 
    """

    def __init__(self, port=None):
        self.port = port

    def create(self, row):
        # TODO next major... move this class so we don't need this kind of hack
        from cassandra.metadata import _NodeInfo
        addr = _NodeInfo.get_broadcast_rpc_address(row)
        port = _NodeInfo.get_broadcast_rpc_port(row)
        if port is None:
            port = self.port if self.port else 9042

        # create the endpoint with the translated address
        # TODO next major, create a TranslatedEndPoint type
        return DefaultEndPoint(
            self.cluster.address_translator.translate(addr),
            port)


@total_ordering
class SniEndPoint(EndPoint):
    """SNI Proxy EndPoint implementation."""

    def __init__(self, proxy_address, server_name, port=9042, init_index=0):
        self._proxy_address = proxy_address
        self._index = init_index
        self._resolved_address = None  # resolved address
        self._port = port
        self._server_name = server_name
        self._ssl_options = {'server_hostname': server_name}

    @property
    def address(self):
        return self._proxy_address

    @property
    def port(self):
        return self._port

    @property
    def ssl_options(self):
        return self._ssl_options

    def resolve(self):
        try:
            resolved_addresses = self._resolve_proxy_addresses()
        except socket.gaierror:
            log.debug('Could not resolve sni proxy hostname "%s" '
                      'with port %d' % (self._proxy_address, self._port))
            raise

        # round-robin pick
        self._resolved_address = sorted(addr[4][0] for addr in resolved_addresses)[self._index % len(resolved_addresses)]
        self._index += 1

        return self._resolved_address, self._port

    def _resolve_proxy_addresses(self):
        return socket.getaddrinfo(self._proxy_address, self._port,
                                  socket.AF_UNSPEC, socket.SOCK_STREAM)

    def __eq__(self, other):
        return (isinstance(other, SniEndPoint) and
                self.address == other.address and self.port == other.port and
                self._server_name == other._server_name)

    def __hash__(self):
        return hash((self.address, self.port, self._server_name))

    def __lt__(self, other):
        return ((self.address, self.port, self._server_name) <
                (other.address, other.port, self._server_name))

    def __str__(self):
        return str("%s:%d:%s" % (self.address, self.port, self._server_name))

    def __repr__(self):
        return "<%s: %s:%d:%s>" % (self.__class__.__name__,
                                   self.address, self.port, self._server_name)


class SniEndPointFactory(EndPointFactory):

    def __init__(self, proxy_address, port):
        self._proxy_address = proxy_address
        self._port = port
        # Initial lookup index to prevent all SNI endpoints to be resolved
        # into the same starting IP address (which might not be available currently).
        # If SNI resolves to 3 IPs, first endpoint will connect to first
        # IP address, and subsequent resolutions to next IPs in round-robin
        # fusion.
        self._init_index = -1

    def create(self, row):
        host_id = row.get("host_id")
        if host_id is None:
            raise ValueError("No host_id to create the SniEndPoint")

        self._init_index += 1
        return SniEndPoint(self._proxy_address, str(host_id), self._port, self._init_index)

    def create_from_sni(self, sni):
        self._init_index += 1
        return SniEndPoint(self._proxy_address, sni, self._port, self._init_index)


@total_ordering
class UnixSocketEndPoint(EndPoint):
    """
    Unix Socket EndPoint implementation.
    """

    def __init__(self, unix_socket_path):
        self._unix_socket_path = unix_socket_path

    @property
    def address(self):
        return self._unix_socket_path

    @property
    def port(self):
        return None

    @property
    def socket_family(self):
        return socket.AF_UNIX

    def resolve(self):
        return self.address, None

    def __eq__(self, other):
        return (isinstance(other, UnixSocketEndPoint) and
                self._unix_socket_path == other._unix_socket_path)

    def __hash__(self):
        return hash(self._unix_socket_path)

    def __lt__(self, other):
        return self._unix_socket_path < other._unix_socket_path

    def __str__(self):
        return str("%s" % (self._unix_socket_path,))

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self._unix_socket_path)


class _Frame(object):
    def __init__(self, version, flags, stream, opcode, body_offset, end_pos):
        self.version = version
        self.flags = flags
        self.stream = stream
        self.opcode = opcode
        self.body_offset = body_offset
        self.end_pos = end_pos

    def __eq__(self, other):  # facilitates testing
        if isinstance(other, _Frame):
            return (self.version == other.version and
                    self.flags == other.flags and
                    self.stream == other.stream and
                    self.opcode == other.opcode and
                    self.body_offset == other.body_offset and
                    self.end_pos == other.end_pos)
        return NotImplemented

    def __str__(self):
        return "ver({0}); flags({1:04b}); stream({2}); op({3}); offset({4}); len({5})".format(self.version, self.flags, self.stream, self.opcode, self.body_offset, self.end_pos - self.body_offset)


NONBLOCKING = (errno.EAGAIN, errno.EWOULDBLOCK)


class ConnectionException(Exception):
    """
    An unrecoverable error was hit when attempting to use a connection,
    or the connection was already closed or defunct.
    """

    def __init__(self, message, endpoint=None):
        Exception.__init__(self, message)
        self.endpoint = endpoint

    @property
    def host(self):
        return self.endpoint.address


class ConnectionShutdown(ConnectionException):
    """
    Raised when a connection has been marked as defunct or has been closed.
    """
    pass


class ProtocolVersionUnsupported(ConnectionException):
    """
    Server rejected startup message due to unsupported protocol version
    """
    def __init__(self, endpoint, startup_version):
        msg = "Unsupported protocol version on %s: %d" % (endpoint, startup_version)
        super(ProtocolVersionUnsupported, self).__init__(msg, endpoint)
        self.startup_version = startup_version


class ConnectionBusy(Exception):
    """
    An attempt was made to send a message through a :class:`.Connection` that
    was already at the max number of in-flight operations.
    """
    pass


class ProtocolError(Exception):
    """
    Communication did not match the protocol that this driver expects.
    """
    pass


class CrcMismatchException(ConnectionException):
    pass


class ContinuousPagingState(object):
    """
     A class for specifying continuous paging state, only supported starting with DSE_V2.
    """

    num_pages_requested = None
    """
    How many pages we have already requested
    """

    num_pages_received = None
    """
    How many pages we have already received
    """

    max_queue_size = None
    """
    The max queue size chosen by the user via the options
    """

    def __init__(self, max_queue_size):
        self.num_pages_requested = max_queue_size  # the initial query requests max_queue_size
        self.num_pages_received = 0
        self.max_queue_size = max_queue_size


class ContinuousPagingSession(object):
    def __init__(self, stream_id, decoder, row_factory, connection, state):
        self.stream_id = stream_id
        self.decoder = decoder
        self.row_factory = row_factory
        self.connection = connection
        self._condition = Condition()
        self._stop = False
        self._page_queue = deque()
        self._state = state
        self.released = False

    def on_message(self, result):
        if isinstance(result, ResultMessage):
            self.on_page(result)
        elif isinstance(result, ErrorMessage):
            self.on_error(result)

    def on_page(self, result):
        with self._condition:
            if self._state:
                self._state.num_pages_received += 1
            self._page_queue.appendleft((result.column_names, result.parsed_rows, None))
            self._stop |= result.continuous_paging_last
            self._condition.notify()

        if result.continuous_paging_last:
            self.released = True

    def on_error(self, error):
        if isinstance(error, ErrorMessage):
            error = error.to_exception()

        log.debug("Got error %s for session %s", error, self.stream_id)

        with self._condition:
            self._page_queue.appendleft((None, None, error))
            self._stop = True
            self._condition.notify()

        self.released = True

    def results(self):
        try:
            self._condition.acquire()
            while True:
                while not self._page_queue and not self._stop:
                    self._condition.wait(timeout=5)
                while self._page_queue:
                    names, rows, err = self._page_queue.pop()
                    if err:
                        raise err
                    self.maybe_request_more()
                    self._condition.release()
                    for row in self.row_factory(names, rows):
                        yield row
                    self._condition.acquire()
                if self._stop:
                    break
        finally:
            try:
                self._condition.release()
            except RuntimeError:
                # This exception happens if the CP results are not entirely consumed
                # and the session is terminated by the runtime. See PYTHON-1054
                pass

    def maybe_request_more(self):
        if not self._state:
            return

        max_queue_size = self._state.max_queue_size
        num_in_flight = self._state.num_pages_requested - self._state.num_pages_received
        space_in_queue = max_queue_size - len(self._page_queue) - num_in_flight
        log.debug("Session %s from %s, space in CP queue: %s, requested: %s, received: %s, num_in_flight: %s",
                  self.stream_id, self.connection.host, space_in_queue, self._state.num_pages_requested,
                  self._state.num_pages_received, num_in_flight)

        if space_in_queue >= max_queue_size / 2:
            self.update_next_pages(space_in_queue)

    def update_next_pages(self, num_next_pages):
        try:
            self._state.num_pages_requested += num_next_pages
            log.debug("Updating backpressure for session %s from %s", self.stream_id, self.connection.host)
            with self.connection.lock:
                self.connection.send_msg(ReviseRequestMessage(ReviseRequestMessage.RevisionType.PAGING_BACKPRESSURE,
                                                              self.stream_id,
                                                              next_pages=num_next_pages),
                                         self.connection.get_request_id(),
                                         self._on_backpressure_response)
        except ConnectionShutdown as ex:
            log.debug("Failed to update backpressure for session %s from %s, connection is shutdown",
                      self.stream_id, self.connection.host)
            self.on_error(ex)

    def _on_backpressure_response(self, response):
        if isinstance(response, ResultMessage):
            log.debug("Paging session %s backpressure updated.", self.stream_id)
        else:
            log.error("Failed updating backpressure for session %s from %s: %s", self.stream_id, self.connection.host,
                      response.to_exception() if hasattr(response, 'to_exception') else response)
            self.on_error(response)

    def cancel(self):
        try:
            log.debug("Canceling paging session %s from %s", self.stream_id, self.connection.host)
            with self.connection.lock:
                self.connection.send_msg(ReviseRequestMessage(ReviseRequestMessage.RevisionType.PAGING_CANCEL,
                                                              self.stream_id),
                                         self.connection.get_request_id(),
                                         self._on_cancel_response)
        except ConnectionShutdown:
            log.debug("Failed to cancel session %s from %s, connection is shutdown",
                      self.stream_id, self.connection.host)

        with self._condition:
            self._stop = True
            self._condition.notify()

    def _on_cancel_response(self, response):
        if isinstance(response, ResultMessage):
            log.debug("Paging session %s canceled.", self.stream_id)
        else:
            log.error("Failed canceling streaming session %s from %s: %s", self.stream_id, self.connection.host,
                      response.to_exception() if hasattr(response, 'to_exception') else response)
        self.released = True


def defunct_on_error(f):

    @wraps(f)
    def wrapper(self, *args, **kwargs):
        try:
            return f(self, *args, **kwargs)
        except Exception as exc:
            self.defunct(exc)
    return wrapper


DEFAULT_CQL_VERSION = '3.0.0'


class _ConnectionIOBuffer(object):
    """
    Abstraction class to ease the use of the different connection io buffers. With
    protocol V5 and checksumming, the data is read, validated and copied to another
    cql frame buffer.
    """
    _io_buffer = None
    _cql_frame_buffer = None
    _connection = None
    _segment_consumed = False

    def __init__(self, connection):
        self._io_buffer = io.BytesIO()
        self._connection = weakref.proxy(connection)

    @property
    def io_buffer(self):
        return self._io_buffer

    @property
    def cql_frame_buffer(self):
        return self._cql_frame_buffer if self.is_checksumming_enabled else \
            self._io_buffer

    def set_checksumming_buffer(self):
        self.reset_io_buffer()
        self._cql_frame_buffer = io.BytesIO()

    @property
    def is_checksumming_enabled(self):
        return self._connection._is_checksumming_enabled

    @property
    def has_consumed_segment(self):
        return self._segment_consumed;

    def readable_io_bytes(self):
        return self.io_buffer.tell()

    def readable_cql_frame_bytes(self):
        return self.cql_frame_buffer.tell()

    def reset_io_buffer(self):
        self._io_buffer = io.BytesIO(self._io_buffer.read())
        self._io_buffer.seek(0, 2)  # 2 == SEEK_END

    def reset_cql_frame_buffer(self):
        if self.is_checksumming_enabled:
            self._cql_frame_buffer = io.BytesIO(self._cql_frame_buffer.read())
            self._cql_frame_buffer.seek(0, 2)  # 2 == SEEK_END
        else:
            self.reset_io_buffer()


class Connection(object):

    CALLBACK_ERR_THREAD_THRESHOLD = 100

    in_buffer_size = 4096
    out_buffer_size = 4096

    cql_version = None
    no_compact = False
    protocol_version = ProtocolVersion.MAX_SUPPORTED

    keyspace = None
    compression = True
    _compression_type = None
    compressor = None
    decompressor = None

    endpoint = None
    ssl_options = None
    ssl_context = None
    last_error = None

    # The current number of operations that are in flight. More precisely,
    # the number of request IDs that are currently in use.
    # This includes orphaned requests.
    in_flight = 0

    # Max concurrent requests allowed per connection. This is set optimistically high, allowing
    # all request ids to be used in protocol version 3+. Normally concurrency would be controlled
    # at a higher level by the application or concurrent.execute_concurrent. This attribute
    # is for lower-level integrations that want some upper bound without reimplementing.
    max_in_flight = 2 ** 15

    # A set of available request IDs.  When using the v3 protocol or higher,
    # this will not initially include all request IDs in order to save memory,
    # but the set will grow if it is exhausted.
    request_ids = None

    # Tracks the highest used request ID in order to help with growing the
    # request_ids set
    highest_request_id = 0

    # Tracks the request IDs which are no longer waited on (timed out), but
    # cannot be reused yet because the node might still send a response
    # on this stream
    orphaned_request_ids = None

    # Set to true if the orphaned stream ID count cross configured threshold
    # and the connection will be replaced
    orphaned_threshold_reached = False

    # If the number of orphaned streams reaches this threshold, this connection
    # will become marked and will be replaced with a new connection by the
    # owning pool (currently, only HostConnection supports this)
    orphaned_threshold = 3  * max_in_flight // 4

    is_defunct = False
    is_closed = False
    lock = None
    user_type_map = None

    msg_received = False

    is_unsupported_proto_version = False

    is_control_connection = False
    signaled_error = False  # used for flagging at the pool level

    allow_beta_protocol_version = False

    _current_frame = None

    _socket = None

    _socket_impl = socket

    _check_hostname = False
    _product_type = None

    _is_checksumming_enabled = False

    _on_orphaned_stream_released = None

    @property
    def _iobuf(self):
        # backward compatibility, to avoid any change in the reactors
        return self._io_buffer.io_buffer

    def __init__(self, host='127.0.0.1', port=9042, authenticator=None,
                 ssl_options=None, sockopts=None, compression=True,
                 cql_version=None, protocol_version=ProtocolVersion.MAX_SUPPORTED, is_control_connection=False,
                 user_type_map=None, connect_timeout=None, allow_beta_protocol_version=False, no_compact=False,
                 ssl_context=None, on_orphaned_stream_released=None):

        # TODO next major rename host to endpoint and remove port kwarg.
        self.endpoint = host if isinstance(host, EndPoint) else DefaultEndPoint(host, port)

        self.authenticator = authenticator
        self.ssl_options = ssl_options.copy() if ssl_options else {}
        self.ssl_context = ssl_context
        self.sockopts = sockopts
        self.compression = compression
        self.cql_version = cql_version
        self.protocol_version = protocol_version
        self.is_control_connection = is_control_connection
        self.user_type_map = user_type_map
        self.connect_timeout = connect_timeout
        self.allow_beta_protocol_version = allow_beta_protocol_version
        self.no_compact = no_compact
        self._push_watchers = defaultdict(set)
        self._requests = {}
        self._io_buffer = _ConnectionIOBuffer(self)
        self._continuous_paging_sessions = {}
        self._socket_writable = True
        self.orphaned_request_ids = set()
        self._on_orphaned_stream_released = on_orphaned_stream_released

        if ssl_options:
            self.ssl_options.update(self.endpoint.ssl_options or {})
        elif self.endpoint.ssl_options:
            self.ssl_options = self.endpoint.ssl_options

        # PYTHON-1331
        #
        # We always use SSLContext.wrap_socket() now but legacy configs may have other params that were passed to ssl.wrap_socket()...
        # and either could have 'check_hostname'.  Remove these params into a separate map and use them to build an SSLContext if
        # we need to do so.
        #
        # Note the use of pop() here; we are very deliberately removing these params from ssl_options if they're present.  After this
        # operation ssl_options should contain only args needed for the ssl_context.wrap_socket() call.
        if not self.ssl_context and self.ssl_options:
            self.ssl_context = self._build_ssl_context_from_options()

        if protocol_version >= 3:
            self.max_request_id = min(self.max_in_flight - 1, (2 ** 15) - 1)
            # Don't fill the deque with 2**15 items right away. Start with some and add
            # more if needed.
            initial_size = min(300, self.max_in_flight)
            self.request_ids = deque(range(initial_size))
            self.highest_request_id = initial_size - 1
        else:
            self.max_request_id = min(self.max_in_flight, (2 ** 7) - 1)
            self.request_ids = deque(range(self.max_request_id + 1))
            self.highest_request_id = self.max_request_id

        self.lock = RLock()
        self.connected_event = Event()

    @property
    def host(self):
        return self.endpoint.address

    @property
    def port(self):
        return self.endpoint.port

    @classmethod
    def initialize_reactor(cls):
        """
        Called once by Cluster.connect().  This should be used by implementations
        to set up any resources that will be shared across connections.
        """
        pass

    @classmethod
    def handle_fork(cls):
        """
        Called after a forking.  This should clean up any remaining reactor state
        from the parent process.
        """
        pass

    @classmethod
    def create_timer(cls, timeout, callback):
        raise NotImplementedError()

    @classmethod
    def factory(cls, endpoint, timeout, *args, **kwargs):
        """
        A factory function which returns connections which have
        succeeded in connecting and are ready for service (or
        raises an exception otherwise).
        """
        start = time.time()
        kwargs['connect_timeout'] = timeout
        conn = cls(endpoint, *args, **kwargs)
        elapsed = time.time() - start
        conn.connected_event.wait(timeout - elapsed)
        if conn.last_error:
            if conn.is_unsupported_proto_version:
                raise ProtocolVersionUnsupported(endpoint, conn.protocol_version)
            raise conn.last_error
        elif not conn.connected_event.is_set():
            conn.close()
            raise OperationTimedOut("Timed out creating connection (%s seconds)" % timeout)
        else:
            return conn

    def _build_ssl_context_from_options(self):

        # Extract a subset of names from self.ssl_options which apply to SSLContext creation
        ssl_context_opt_names = ['ssl_version', 'cert_reqs', 'check_hostname', 'keyfile', 'certfile', 'ca_certs', 'ciphers']
        opts = {k:self.ssl_options.get(k, None) for k in ssl_context_opt_names if k in self.ssl_options}

        # Python >= 3.10 requires either PROTOCOL_TLS_CLIENT or PROTOCOL_TLS_SERVER, so we'll get ahead of things by always
        # being explicit
        ssl_version = opts.get('ssl_version', None) or ssl.PROTOCOL_TLS_CLIENT
        cert_reqs = opts.get('cert_reqs', None) or ssl.CERT_REQUIRED
        rv = ssl.SSLContext(protocol=int(ssl_version))
        rv.check_hostname = bool(opts.get('check_hostname', False))
        rv.options = int(cert_reqs)

        certfile = opts.get('certfile', None)
        keyfile = opts.get('keyfile', None)
        if certfile:
            rv.load_cert_chain(certfile, keyfile)
        ca_certs = opts.get('ca_certs', None)
        if ca_certs:
            rv.load_verify_locations(ca_certs)
        ciphers = opts.get('ciphers', None)
        if ciphers:
            rv.set_ciphers(ciphers)

        return rv

    def _wrap_socket_from_context(self):

        # Extract a subset of names from self.ssl_options which apply to SSLContext.wrap_socket (or at least the parts
        # of it that don't involve building an SSLContext under the covers)
        wrap_socket_opt_names = ['server_side', 'do_handshake_on_connect', 'suppress_ragged_eofs', 'server_hostname']
        opts = {k:self.ssl_options.get(k, None) for k in wrap_socket_opt_names if k in self.ssl_options}

        # PYTHON-1186: set the server_hostname only if the SSLContext has
        # check_hostname enabled, and it is not already provided by the EndPoint ssl options
        #opts['server_hostname'] = self.endpoint.address
        if (self.ssl_context.check_hostname and 'server_hostname' not in opts):
            server_hostname = self.endpoint.address
            opts['server_hostname'] = server_hostname

        return self.ssl_context.wrap_socket(self._socket, **opts)

    def _initiate_connection(self, sockaddr):
        self._socket.connect(sockaddr)

    # PYTHON-1331
    #
    # Allow implementations specific to an event loop to add additional behaviours
    def _validate_hostname(self):
        pass

    def _get_socket_addresses(self):
        address, port = self.endpoint.resolve()

        if hasattr(socket, 'AF_UNIX') and self.endpoint.socket_family == socket.AF_UNIX:
            return [(socket.AF_UNIX, socket.SOCK_STREAM, 0, None, address)]

        addresses = socket.getaddrinfo(address, port, self.endpoint.socket_family, socket.SOCK_STREAM)
        if not addresses:
            raise ConnectionException("getaddrinfo returned empty list for %s" % (self.endpoint,))

        return addresses

    def _connect_socket(self):
        sockerr = None
        addresses = self._get_socket_addresses()
        for (af, socktype, proto, _, sockaddr) in addresses:
            try:
                self._socket = self._socket_impl.socket(af, socktype, proto)
                if self.ssl_context:
                    self._socket = self._wrap_socket_from_context()
                self._socket.settimeout(self.connect_timeout)
                self._initiate_connection(sockaddr)
                self._socket.settimeout(None)

                # PYTHON-1331
                #
                # Most checking is done via the check_hostname param on the SSLContext.
                # Subclasses can add additional behaviours via _validate_hostname() so
                # run that here.
                if self._check_hostname:
                    self._validate_hostname()
                sockerr = None
                break
            except socket.error as err:
                if self._socket:
                    self._socket.close()
                    self._socket = None
                sockerr = err

        if sockerr:
            raise socket.error(sockerr.errno, "Tried connecting to %s. Last error: %s" %
                               ([a[4] for a in addresses], sockerr.strerror or sockerr))

        if self.sockopts:
            for args in self.sockopts:
                self._socket.setsockopt(*args)

    def _enable_compression(self):
        if self._compressor:
            self.compressor = self._compressor

    def _enable_checksumming(self):
        self._io_buffer.set_checksumming_buffer()
        self._is_checksumming_enabled = True
        self._segment_codec = segment_codec_lz4 if self.compressor else segment_codec_no_compression
        log.debug("Enabling protocol checksumming on connection (%s).", id(self))

    def close(self):
        raise NotImplementedError()

    def defunct(self, exc):
        with self.lock:
            if self.is_defunct or self.is_closed:
                return
            self.is_defunct = True

        exc_info = sys.exc_info()
        # if we are not handling an exception, just use the passed exception, and don't try to format exc_info with the message
        if any(exc_info):
            log.debug("Defuncting connection (%s) to %s:",
                      id(self), self.endpoint, exc_info=exc_info)
        else:
            log.debug("Defuncting connection (%s) to %s: %s",
                      id(self), self.endpoint, exc)

        self.last_error = exc
        self.close()
        self.error_all_cp_sessions(exc)
        self.error_all_requests(exc)
        self.connected_event.set()
        return exc

    def error_all_cp_sessions(self, exc):
        stream_ids = list(self._continuous_paging_sessions.keys())
        for stream_id in stream_ids:
            self._continuous_paging_sessions[stream_id].on_error(exc)

    def error_all_requests(self, exc):
        with self.lock:
            requests = self._requests
            self._requests = {}

        if not requests:
            return

        new_exc = ConnectionShutdown(str(exc))

        def try_callback(cb):
            try:
                cb(new_exc)
            except Exception:
                log.warning("Ignoring unhandled exception while erroring requests for a "
                            "failed connection (%s) to host %s:",
                            id(self), self.endpoint, exc_info=True)

        # run first callback from this thread to ensure pool state before leaving
        cb, _, _ = requests.popitem()[1]
        try_callback(cb)

        if not requests:
            return

        # additional requests are optionally errored from a separate thread
        # The default callback and retry logic is fairly expensive -- we don't
        # want to tie up the event thread when there are many requests
        def err_all_callbacks():
            for cb, _, _ in requests.values():
                try_callback(cb)
        if len(requests) < Connection.CALLBACK_ERR_THREAD_THRESHOLD:
            err_all_callbacks()
        else:
            # daemon thread here because we want to stay decoupled from the cluster TPE
            # TODO: would it make sense to just have a driver-global TPE?
            t = Thread(target=err_all_callbacks)
            t.daemon = True
            t.start()

    def get_request_id(self):
        """
        This must be called while self.lock is held.
        """
        try:
            return self.request_ids.popleft()
        except IndexError:
            new_request_id = self.highest_request_id + 1
            # in_flight checks should guarantee this
            assert new_request_id <= self.max_request_id
            self.highest_request_id = new_request_id
            return self.highest_request_id

    def handle_pushed(self, response):
        log.debug("Message pushed from server: %r", response)
        for cb in self._push_watchers.get(response.event_type, []):
            try:
                cb(response.event_args)
            except Exception:
                log.exception("Pushed event handler errored, ignoring:")

    def send_msg(self, msg, request_id, cb, encoder=ProtocolHandler.encode_message, decoder=ProtocolHandler.decode_message, result_metadata=None):
        if self.is_defunct:
            raise ConnectionShutdown("Connection to %s is defunct" % self.endpoint)
        elif self.is_closed:
            raise ConnectionShutdown("Connection to %s is closed" % self.endpoint)
        elif not self._socket_writable:
            raise ConnectionBusy("Connection %s is overloaded" % self.endpoint)

        # queue the decoder function with the request
        # this allows us to inject custom functions per request to encode, decode messages
        self._requests[request_id] = (cb, decoder, result_metadata)
        msg = encoder(msg, request_id, self.protocol_version, compressor=self.compressor,
                      allow_beta_protocol_version=self.allow_beta_protocol_version)

        if self._is_checksumming_enabled:
            buffer = io.BytesIO()
            self._segment_codec.encode(buffer, msg)
            msg = buffer.getvalue()

        self.push(msg)
        return len(msg)

    def wait_for_response(self, msg, timeout=None, **kwargs):
        return self.wait_for_responses(msg, timeout=timeout, **kwargs)[0]

    def wait_for_responses(self, *msgs, **kwargs):
        """
        Returns a list of (success, response) tuples.  If success
        is False, response will be an Exception.  Otherwise, response
        will be the normal query response.

        If fail_on_error was left as True and one of the requests
        failed, the corresponding Exception will be raised.
        """
        if self.is_closed or self.is_defunct:
            raise ConnectionShutdown("Connection %s is already closed" % (self, ))
        timeout = kwargs.get('timeout')
        fail_on_error = kwargs.get('fail_on_error', True)
        waiter = ResponseWaiter(self, len(msgs), fail_on_error)

        # busy wait for sufficient space on the connection
        messages_sent = 0
        while True:
            needed = len(msgs) - messages_sent
            with self.lock:
                available = min(needed, self.max_request_id - self.in_flight + 1)
                request_ids = [self.get_request_id() for _ in range(available)]
                self.in_flight += available

            for i, request_id in enumerate(request_ids):
                self.send_msg(msgs[messages_sent + i],
                              request_id,
                              partial(waiter.got_response, index=messages_sent + i))
            messages_sent += available

            if messages_sent == len(msgs):
                break
            else:
                if timeout is not None:
                    timeout -= 0.01
                    if timeout <= 0.0:
                        raise OperationTimedOut()
                time.sleep(0.01)

        try:
            return waiter.deliver(timeout)
        except OperationTimedOut:
            raise
        except Exception as exc:
            self.defunct(exc)
            raise

    def register_watcher(self, event_type, callback, register_timeout=None):
        """
        Register a callback for a given event type.
        """
        self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            RegisterMessage(event_list=[event_type]),
            timeout=register_timeout)

    def register_watchers(self, type_callback_dict, register_timeout=None):
        """
        Register multiple callback/event type pairs, expressed as a dict.
        """
        for event_type, callback in type_callback_dict.items():
            self._push_watchers[event_type].add(callback)
        self.wait_for_response(
            RegisterMessage(event_list=type_callback_dict.keys()),
            timeout=register_timeout)

    def control_conn_disposed(self):
        self.is_control_connection = False
        self._push_watchers = {}

    @defunct_on_error
    def _read_frame_header(self):
        buf = self._io_buffer.cql_frame_buffer.getvalue()
        pos = len(buf)
        if pos:
            version = buf[0] & PROTOCOL_VERSION_MASK
            if version not in ProtocolVersion.SUPPORTED_VERSIONS:
                raise ProtocolError("This version of the driver does not support protocol version %d" % version)
            frame_header = frame_header_v3 if version >= 3 else frame_header_v1_v2
            # this frame header struct is everything after the version byte
            header_size = frame_header.size + 1
            if pos >= header_size:
                flags, stream, op, body_len = frame_header.unpack_from(buf, 1)
                if body_len < 0:
                    raise ProtocolError("Received negative body length: %r" % body_len)
                self._current_frame = _Frame(version, flags, stream, op, header_size, body_len + header_size)
        return pos

    @defunct_on_error
    def _process_segment_buffer(self):
        readable_bytes = self._io_buffer.readable_io_bytes()
        if readable_bytes >= self._segment_codec.header_length_with_crc:
            try:
                self._io_buffer.io_buffer.seek(0)
                segment_header = self._segment_codec.decode_header(self._io_buffer.io_buffer)

                if readable_bytes >= segment_header.segment_length:
                    segment = self._segment_codec.decode(self._iobuf, segment_header)
                    self._io_buffer._segment_consumed = True
                    self._io_buffer.cql_frame_buffer.write(segment.payload)
                else:
                    # not enough data to read the segment. reset the buffer pointer at the
                    # beginning to not lose what we previously read (header).
                    self._io_buffer._segment_consumed = False
                    self._io_buffer.io_buffer.seek(0)
            except CrcException as exc:
                # re-raise an exception that inherits from ConnectionException
                raise CrcMismatchException(str(exc), self.endpoint)
        else:
            self._io_buffer._segment_consumed = False

    def process_io_buffer(self):
        while True:
            if self._is_checksumming_enabled and self._io_buffer.readable_io_bytes():
                self._process_segment_buffer()
                self._io_buffer.reset_io_buffer()

            if self._is_checksumming_enabled and not self._io_buffer.has_consumed_segment:
                # We couldn't read an entire segment from the io buffer, so return
                # control to allow more bytes to be read off the wire
                return

            if not self._current_frame:
                pos = self._read_frame_header()
            else:
                pos = self._io_buffer.readable_cql_frame_bytes()

            if not self._current_frame or pos < self._current_frame.end_pos:
                if self._is_checksumming_enabled and self._io_buffer.readable_io_bytes():
                    # We have a multi-segments message, and we need to read more
                    # data to complete the current cql frame
                    continue

                # we don't have a complete header yet, or we
                # already saw a header, but we don't have a
                # complete message yet
                return
            else:
                frame = self._current_frame
                self._io_buffer.cql_frame_buffer.seek(frame.body_offset)
                msg = self._io_buffer.cql_frame_buffer.read(frame.end_pos - frame.body_offset)
                self.process_msg(frame, msg)
                self._io_buffer.reset_cql_frame_buffer()
                self._current_frame = None

    @defunct_on_error
    def process_msg(self, header, body):
        self.msg_received = True
        stream_id = header.stream
        if stream_id < 0:
            callback = None
            decoder = ProtocolHandler.decode_message
            result_metadata = None
        else:
            if stream_id in self._continuous_paging_sessions:
                paging_session = self._continuous_paging_sessions[stream_id]
                callback = paging_session.on_message
                decoder = paging_session.decoder
                result_metadata = None
            else:
                need_notify_of_release = False
                with self.lock:
                    if stream_id in self.orphaned_request_ids:
                        self.in_flight -= 1
                        self.orphaned_request_ids.remove(stream_id)
                        need_notify_of_release = True
                if need_notify_of_release and self._on_orphaned_stream_released:
                    self._on_orphaned_stream_released()

                try:
                    callback, decoder, result_metadata = self._requests.pop(stream_id)
                # This can only happen if the stream_id was
                # removed due to an OperationTimedOut
                except KeyError:
                    with self.lock:
                        self.request_ids.append(stream_id)
                    return

        try:
            response = decoder(header.version, self.user_type_map, stream_id,
                               header.flags, header.opcode, body, self.decompressor, result_metadata)
        except Exception as exc:
            log.exception("Error decoding response from Cassandra. "
                          "%s; buffer: %r", header, self._iobuf.getvalue())
            if callback is not None:
                callback(exc)
            self.defunct(exc)
            return

        try:
            if stream_id >= 0:
                if isinstance(response, ProtocolException):
                    if 'unsupported protocol version' in response.message:
                        self.is_unsupported_proto_version = True
                    else:
                        log.error("Closing connection %s due to protocol error: %s", self, response.summary_msg())
                    self.defunct(response)
                if callback is not None:
                    callback(response)
            else:
                self.handle_pushed(response)
        except Exception:
            log.exception("Callback handler errored, ignoring:")

        # done after callback because the callback might signal this as a paging session
        if stream_id >= 0:
            if stream_id in self._continuous_paging_sessions:
                if self._continuous_paging_sessions[stream_id].released:
                    self.remove_continuous_paging_session(stream_id)
            else:
                with self.lock:
                    self.request_ids.append(stream_id)

    def new_continuous_paging_session(self, stream_id, decoder, row_factory, state):
        session = ContinuousPagingSession(stream_id, decoder, row_factory, self, state)
        self._continuous_paging_sessions[stream_id] = session
        return session

    def remove_continuous_paging_session(self, stream_id):
        try:
            self._continuous_paging_sessions.pop(stream_id)
            with self.lock:
                log.debug("Returning cp session stream id %s", stream_id)
                self.request_ids.append(stream_id)
        except KeyError:
            pass

    @defunct_on_error
    def _send_options_message(self):
        log.debug("Sending initial options message for new connection (%s) to %s", id(self), self.endpoint)
        self.send_msg(OptionsMessage(), self.get_request_id(), self._handle_options_response)

    @defunct_on_error
    def _handle_options_response(self, options_response):
        if self.is_defunct:
            return

        if not isinstance(options_response, SupportedMessage):
            if isinstance(options_response, ConnectionException):
                raise options_response
            else:
                log.error("Did not get expected SupportedMessage response; "
                          "instead, got: %s", options_response)
                raise ConnectionException("Did not get expected SupportedMessage "
                                          "response; instead, got: %s"
                                          % (options_response,))

        log.debug("Received options response on new connection (%s) from %s",
                  id(self), self.endpoint)
        supported_cql_versions = options_response.cql_versions
        remote_supported_compressions = options_response.options['COMPRESSION']
        self._product_type = options_response.options.get('PRODUCT_TYPE', [None])[0]

        if self.cql_version:
            if self.cql_version not in supported_cql_versions:
                raise ProtocolError(
                    "cql_version %r is not supported by remote (w/ native "
                    "protocol). Supported versions: %r"
                    % (self.cql_version, supported_cql_versions))
        else:
            self.cql_version = supported_cql_versions[0]

        self._compressor = None
        compression_type = None
        if self.compression:
            overlap = (set(locally_supported_compressions.keys()) &
                       set(remote_supported_compressions))
            if len(overlap) == 0:
                log.debug("No available compression types supported on both ends."
                          " locally supported: %r. remotely supported: %r",
                          locally_supported_compressions.keys(),
                          remote_supported_compressions)
            else:
                compression_type = None
                if isinstance(self.compression, str):
                    # the user picked a specific compression type ('snappy' or 'lz4')
                    if self.compression not in remote_supported_compressions:
                        raise ProtocolError(
                            "The requested compression type (%s) is not supported by the Cassandra server at %s"
                            % (self.compression, self.endpoint))
                    compression_type = self.compression
                else:
                    # our locally supported compressions are ordered to prefer
                    # lz4, if available
                    for k in locally_supported_compressions.keys():
                        if k in overlap:
                            compression_type = k
                            break

                # If snappy compression is selected with v5+checksumming, the connection
                # will fail with OTO. Only lz4 is supported
                if (compression_type == 'snappy' and
                        ProtocolVersion.has_checksumming_support(self.protocol_version)):
                    log.debug("Snappy compression is not supported with protocol version %s and "
                              "checksumming. Consider installing lz4. Disabling compression.", self.protocol_version)
                    compression_type = None
                else:
                    # set the decompressor here, but set the compressor only after
                    # a successful Ready message
                    self._compression_type = compression_type
                    self._compressor, self.decompressor = \
                        locally_supported_compressions[compression_type]

        self._send_startup_message(compression_type, no_compact=self.no_compact)

    @defunct_on_error
    def _send_startup_message(self, compression=None, no_compact=False):
        log.debug("Sending StartupMessage on %s", self)
        opts = {'DRIVER_NAME': DRIVER_NAME,
                'DRIVER_VERSION': DRIVER_VERSION}
        if compression:
            opts['COMPRESSION'] = compression
        if no_compact:
            opts['NO_COMPACT'] = 'true'
        sm = StartupMessage(cqlversion=self.cql_version, options=opts)
        self.send_msg(sm, self.get_request_id(), cb=self._handle_startup_response)
        log.debug("Sent StartupMessage on %s", self)

    @defunct_on_error
    def _handle_startup_response(self, startup_response, did_authenticate=False):
        if self.is_defunct:
            return

        if isinstance(startup_response, ReadyMessage):
            if self.authenticator:
                log.warning("An authentication challenge was not sent, "
                            "this is suspicious because the driver expects "
                            "authentication (configured authenticator = %s)",
                            self.authenticator.__class__.__name__)

            log.debug("Got ReadyMessage on new connection (%s) from %s", id(self), self.endpoint)
            self._enable_compression()

            if ProtocolVersion.has_checksumming_support(self.protocol_version):
                self._enable_checksumming()

            self.connected_event.set()
        elif isinstance(startup_response, AuthenticateMessage):
            log.debug("Got AuthenticateMessage on new connection (%s) from %s: %s",
                      id(self), self.endpoint, startup_response.authenticator)

            if self.authenticator is None:
                log.error("Failed to authenticate to %s. If you are trying to connect to a DSE cluster, "
                          "consider using TransitionalModePlainTextAuthProvider "
                          "if DSE authentication is configured with transitional mode" % (self.host,))
                raise AuthenticationFailed('Remote end requires authentication')

            self._enable_compression()
            if ProtocolVersion.has_checksumming_support(self.protocol_version):
                self._enable_checksumming()

            if isinstance(self.authenticator, dict):
                log.debug("Sending credentials-based auth response on %s", self)
                cm = CredentialsMessage(creds=self.authenticator)
                callback = partial(self._handle_startup_response, did_authenticate=True)
                self.send_msg(cm, self.get_request_id(), cb=callback)
            else:
                log.debug("Sending SASL-based auth response on %s", self)
                self.authenticator.server_authenticator_class = startup_response.authenticator
                initial_response = self.authenticator.initial_response()
                initial_response = "" if initial_response is None else initial_response
                self.send_msg(AuthResponseMessage(initial_response), self.get_request_id(),
                              self._handle_auth_response)
        elif isinstance(startup_response, ErrorMessage):
            log.debug("Received ErrorMessage on new connection (%s) from %s: %s",
                      id(self), self.endpoint, startup_response.summary_msg())
            if did_authenticate:
                raise AuthenticationFailed(
                    "Failed to authenticate to %s: %s" %
                    (self.endpoint, startup_response.summary_msg()))
            else:
                raise ConnectionException(
                    "Failed to initialize new connection to %s: %s"
                    % (self.endpoint, startup_response.summary_msg()))
        elif isinstance(startup_response, ConnectionShutdown):
            log.debug("Connection to %s was closed during the startup handshake", (self.endpoint))
            raise startup_response
        else:
            msg = "Unexpected response during Connection setup: %r"
            log.error(msg, startup_response)
            raise ProtocolError(msg % (startup_response,))

    @defunct_on_error
    def _handle_auth_response(self, auth_response):
        if self.is_defunct:
            return

        if isinstance(auth_response, AuthSuccessMessage):
            log.debug("Connection %s successfully authenticated", self)
            self.authenticator.on_authentication_success(auth_response.token)
            if self._compressor:
                self.compressor = self._compressor
            self.connected_event.set()
        elif isinstance(auth_response, AuthChallengeMessage):
            response = self.authenticator.evaluate_challenge(auth_response.challenge)
            msg = AuthResponseMessage("" if response is None else response)
            log.debug("Responding to auth challenge on %s", self)
            self.send_msg(msg, self.get_request_id(), self._handle_auth_response)
        elif isinstance(auth_response, ErrorMessage):
            log.debug("Received ErrorMessage on new connection (%s) from %s: %s",
                      id(self), self.endpoint, auth_response.summary_msg())
            raise AuthenticationFailed(
                "Failed to authenticate to %s: %s" %
                (self.endpoint, auth_response.summary_msg()))
        elif isinstance(auth_response, ConnectionShutdown):
            log.debug("Connection to %s was closed during the authentication process", self.endpoint)
            raise auth_response
        else:
            msg = "Unexpected response during Connection authentication to %s: %r"
            log.error(msg, self.endpoint, auth_response)
            raise ProtocolError(msg % (self.endpoint, auth_response))

    def set_keyspace_blocking(self, keyspace):
        if not keyspace or keyspace == self.keyspace:
            return

        query = QueryMessage(query='USE "%s"' % (keyspace,),
                             consistency_level=ConsistencyLevel.ONE)
        try:
            result = self.wait_for_response(query)
        except InvalidRequestException as ire:
            # the keyspace probably doesn't exist
            raise ire.to_exception()
        except Exception as exc:
            conn_exc = ConnectionException(
                "Problem while setting keyspace: %r" % (exc,), self.endpoint)
            self.defunct(conn_exc)
            raise conn_exc

        if isinstance(result, ResultMessage):
            self.keyspace = keyspace
        else:
            conn_exc = ConnectionException(
                "Problem while setting keyspace: %r" % (result,), self.endpoint)
            self.defunct(conn_exc)
            raise conn_exc

    def set_keyspace_async(self, keyspace, callback):
        """
        Use this in order to avoid deadlocking the event loop thread.
        When the operation completes, `callback` will be called with
        two arguments: this connection and an Exception if an error
        occurred, otherwise :const:`None`.

        This method will always increment :attr:`.in_flight` attribute, even if
        it doesn't need to make a request, just to maintain an
        ":attr:`.in_flight` is incremented" invariant.
        """
        # Here we increment in_flight unconditionally, whether we need to issue
        # a request or not. This is bad, but allows callers -- specifically
        # _set_keyspace_for_all_conns -- to assume that we increment
        # self.in_flight during this call. This allows the passed callback to
        # safely call HostConnection{Pool,}.return_connection on this
        # Connection.
        #
        # We use a busy wait on the lock here because:
        # - we'll only spin if the connection is at max capacity, which is very
        #   unlikely for a set_keyspace call
        # - it allows us to avoid signaling a condition every time a request completes
        while True:
            with self.lock:
                if self.in_flight < self.max_request_id:
                    self.in_flight += 1
                    break
            time.sleep(0.001)

        if not keyspace or keyspace == self.keyspace:
            callback(self, None)
            return

        query = QueryMessage(query='USE "%s"' % (keyspace,),
                             consistency_level=ConsistencyLevel.ONE)

        def process_result(result):
            if isinstance(result, ResultMessage):
                self.keyspace = keyspace
                callback(self, None)
            elif isinstance(result, InvalidRequestException):
                callback(self, result.to_exception())
            else:
                callback(self, self.defunct(ConnectionException(
                    "Problem while setting keyspace: %r" % (result,), self.endpoint)))

        # We've incremented self.in_flight above, so we "have permission" to
        # acquire a new request id
        request_id = self.get_request_id()

        self.send_msg(query, request_id, process_result)

    @property
    def is_idle(self):
        return not self.msg_received

    def reset_idle(self):
        self.msg_received = False

    def __str__(self):
        status = ""
        if self.is_defunct:
            status = " (defunct)"
        elif self.is_closed:
            status = " (closed)"

        return "<%s(%r) %s%s>" % (self.__class__.__name__, id(self), self.endpoint, status)
    __repr__ = __str__


class ResponseWaiter(object):

    def __init__(self, connection, num_responses, fail_on_error):
        self.connection = connection
        self.pending = num_responses
        self.fail_on_error = fail_on_error
        self.error = None
        self.responses = [None] * num_responses
        self.event = Event()

    def got_response(self, response, index):
        with self.connection.lock:
            self.connection.in_flight -= 1
        if isinstance(response, Exception):
            if hasattr(response, 'to_exception'):
                response = response.to_exception()
            if self.fail_on_error:
                self.error = response
                self.event.set()
            else:
                self.responses[index] = (False, response)
        else:
            if not self.fail_on_error:
                self.responses[index] = (True, response)
            else:
                self.responses[index] = response

        self.pending -= 1
        if not self.pending:
            self.event.set()

    def deliver(self, timeout=None):
        """
        If fail_on_error was set to False, a list of (success, response)
        tuples will be returned.  If success is False, response will be
        an Exception.  Otherwise, response will be the normal query response.

        If fail_on_error was left as True and one of the requests
        failed, the corresponding Exception will be raised. Otherwise,
        the normal response will be returned.
        """
        self.event.wait(timeout)
        if self.error:
            raise self.error
        elif not self.event.is_set():
            raise OperationTimedOut()
        else:
            return self.responses


class HeartbeatFuture(object):
    def __init__(self, connection, owner):
        self._exception = None
        self._event = Event()
        self.connection = connection
        self.owner = owner
        log.debug("Sending options message heartbeat on idle connection (%s) %s",
                  id(connection), connection.endpoint)
        with connection.lock:
            if connection.in_flight < connection.max_request_id:
                connection.in_flight += 1
                connection.send_msg(OptionsMessage(), connection.get_request_id(), self._options_callback)
            else:
                self._exception = Exception("Failed to send heartbeat because connection 'in_flight' exceeds threshold")
                self._event.set()

    def wait(self, timeout):
        self._event.wait(timeout)
        if self._event.is_set():
            if self._exception:
                raise self._exception
        else:
            raise OperationTimedOut("Connection heartbeat timeout after %s seconds" % (timeout,), self.connection.endpoint)

    def _options_callback(self, response):
        if isinstance(response, SupportedMessage):
            log.debug("Received options response on connection (%s) from %s",
                      id(self.connection), self.connection.endpoint)
        else:
            if isinstance(response, ConnectionException):
                self._exception = response
            else:
                self._exception = ConnectionException("Received unexpected response to OptionsMessage: %s"
                                                      % (response,))
        self._event.set()


class ConnectionHeartbeat(Thread):

    def __init__(self, interval_sec, get_connection_holders, timeout):
        Thread.__init__(self, name="Connection heartbeat")
        self._interval = interval_sec
        self._timeout = timeout
        self._get_connection_holders = get_connection_holders
        self._shutdown_event = Event()
        self.daemon = True
        self.start()

    class ShutdownException(Exception):
        pass

    def run(self):
        self._shutdown_event.wait(self._interval)
        while not self._shutdown_event.is_set():
            start_time = time.time()

            futures = []
            failed_connections = []
            try:
                for connections, owner in [(o.get_connections(), o) for o in self._get_connection_holders()]:
                    for connection in connections:
                        self._raise_if_stopped()
                        if not (connection.is_defunct or connection.is_closed):
                            if connection.is_idle:
                                try:
                                    futures.append(HeartbeatFuture(connection, owner))
                                except Exception as e:
                                    log.warning("Failed sending heartbeat message on connection (%s) to %s",
                                                id(connection), connection.endpoint)
                                    failed_connections.append((connection, owner, e))
                            else:
                                connection.reset_idle()
                        else:
                            log.debug("Cannot send heartbeat message on connection (%s) to %s",
                                      id(connection), connection.endpoint)
                            # make sure the owner sees this defunct/closed connection
                            owner.return_connection(connection)
                    self._raise_if_stopped()

                # Wait max `self._timeout` seconds for all HeartbeatFutures to complete
                timeout = self._timeout
                start_time = time.time()
                for f in futures:
                    self._raise_if_stopped()
                    connection = f.connection
                    try:
                        f.wait(timeout)
                        # TODO: move this, along with connection locks in pool, down into Connection
                        with connection.lock:
                            connection.in_flight -= 1
                        connection.reset_idle()
                    except Exception as e:
                        log.warning("Heartbeat failed for connection (%s) to %s",
                                    id(connection), connection.endpoint)
                        failed_connections.append((f.connection, f.owner, e))

                    timeout = self._timeout - (time.time() - start_time)

                for connection, owner, exc in failed_connections:
                    self._raise_if_stopped()
                    if not connection.is_control_connection:
                        # Only HostConnection supports shutdown_on_error
                        owner.shutdown_on_error = True
                    connection.defunct(exc)
                    owner.return_connection(connection)
            except self.ShutdownException:
                pass
            except Exception:
                log.error("Failed connection heartbeat", exc_info=True)

            elapsed = time.time() - start_time
            self._shutdown_event.wait(max(self._interval - elapsed, 0.01))

    def stop(self):
        self._shutdown_event.set()
        self.join()

    def _raise_if_stopped(self):
        if self._shutdown_event.is_set():
            raise self.ShutdownException()


class Timer(object):

    canceled = False

    def __init__(self, timeout, callback):
        self.end = time.time() + timeout
        self.callback = callback

    def __lt__(self, other):
        return self.end < other.end

    def cancel(self):
        self.canceled = True

    def finish(self, time_now):
        if self.canceled:
            return True

        if time_now >= self.end:
            self.callback()
            return True

        return False


class TimerManager(object):

    def __init__(self):
        self._queue = []
        self._new_timers = []

    def add_timer(self, timer):
        """
        called from client thread with a Timer object
        """
        self._new_timers.append((timer.end, timer))

    def service_timeouts(self):
        """
        run callbacks on all expired timers
        Called from the event thread
        :return: next end time, or None
        """
        queue = self._queue
        if self._new_timers:
            new_timers = self._new_timers
            while new_timers:
                heappush(queue, new_timers.pop())

        if queue:
            now = time.time()
            while queue:
                try:
                    timer = queue[0][1]
                    if timer.finish(now):
                        heappop(queue)
                    else:
                        return timer.end
                except Exception:
                    log.exception("Exception while servicing timeout callback: ")

    @property
    def next_timeout(self):
        try:
            return self._queue[0][0]
        except IndexError:
            pass
