# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import  # to enable import io from stdlib
from functools import total_ordering
import logging
import socket

log = logging.getLogger(__name__)

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

