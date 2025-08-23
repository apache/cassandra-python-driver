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

from binascii import unhexlify
from bisect import bisect_left
from collections import defaultdict
from collections.abc import Mapping
from functools import total_ordering
from hashlib import md5
import json
import logging
import re
import sys
from threading import RLock
import struct
import random

murmur3 = None
try:
    from cassandra.murmur3 import murmur3
except ImportError as e:
    pass

from cassandra import SignatureDescriptor, ConsistencyLevel, InvalidRequest, Unauthorized
import cassandra.cqltypes as types
from cassandra.encoder import Encoder
from cassandra.marshal import varint_unpack
from cassandra.protocol import QueryMessage
from cassandra.query import dict_factory, bind_params
from cassandra.util import OrderedDict, Version
from cassandra.pool import HostDistance
from cassandra.connection import EndPoint

log = logging.getLogger(__name__)

cql_keywords = set((
    'add', 'aggregate', 'all', 'allow', 'alter', 'and', 'apply', 'as', 'asc', 'ascii', 'authorize', 'batch', 'begin',
    'bigint', 'blob', 'boolean', 'by', 'called', 'clustering', 'columnfamily', 'compact', 'contains', 'count',
    'counter', 'create', 'custom', 'date', 'decimal', 'default', 'delete', 'desc', 'describe', 'deterministic', 'distinct', 'double', 'drop',
    'entries', 'execute', 'exists', 'filtering', 'finalfunc', 'float', 'from', 'frozen', 'full', 'function',
    'functions', 'grant', 'if', 'in', 'index', 'inet', 'infinity', 'initcond', 'input', 'insert', 'int', 'into', 'is', 'json',
    'key', 'keys', 'keyspace', 'keyspaces', 'language', 'limit', 'list', 'login', 'map', 'materialized', 'mbean', 'mbeans', 'modify', 'monotonic',
    'nan', 'nologin', 'norecursive', 'nosuperuser', 'not', 'null', 'of', 'on', 'options', 'or', 'order', 'password', 'permission',
    'permissions', 'primary', 'rename', 'replace', 'returns', 'revoke', 'role', 'roles', 'schema', 'select', 'set',
    'sfunc', 'smallint', 'static', 'storage', 'stype', 'superuser', 'table', 'text', 'time', 'timestamp', 'timeuuid',
    'tinyint', 'to', 'token', 'trigger', 'truncate', 'ttl', 'tuple', 'type', 'unlogged', 'unset', 'update', 'use', 'user',
    'users', 'using', 'uuid', 'values', 'varchar', 'varint', 'view', 'where', 'with', 'writetime',

    # DSE specifics
    "node", "nodes", "plan", "active", "application", "applications", "java", "executor", "executors", "std_out", "std_err",
    "renew", "delegation", "no", "redact", "token", "lowercasestring", "cluster", "authentication", "schemes", "scheme",
    "internal", "ldap", "kerberos", "remote", "object", "method", "call", "calls", "search", "schema", "config", "rows",
    "columns", "profiles", "commit", "reload", "rebuild", "field", "workpool", "any", "submission", "indices",
    "restrict", "unrestrict"
))
"""
Set of keywords in CQL.

Derived from .../cassandra/src/java/org/apache/cassandra/cql3/Cql.g
"""

cql_keywords_unreserved = set((
    'aggregate', 'all', 'as', 'ascii', 'bigint', 'blob', 'boolean', 'called', 'clustering', 'compact', 'contains',
    'count', 'counter', 'custom', 'date', 'decimal', 'deterministic', 'distinct', 'double', 'exists', 'filtering', 'finalfunc', 'float',
    'frozen', 'function', 'functions', 'inet', 'initcond', 'input', 'int', 'json', 'key', 'keys', 'keyspaces',
    'language', 'list', 'login', 'map', 'monotonic', 'nologin', 'nosuperuser', 'options', 'password', 'permission', 'permissions',
    'returns', 'role', 'roles', 'sfunc', 'smallint', 'static', 'storage', 'stype', 'superuser', 'text', 'time',
    'timestamp', 'timeuuid', 'tinyint', 'trigger', 'ttl', 'tuple', 'type', 'user', 'users', 'uuid', 'values', 'varchar',
    'varint', 'writetime'
))
"""
Set of unreserved keywords in CQL.

Derived from .../cassandra/src/java/org/apache/cassandra/cql3/Cql.g
"""

cql_keywords_reserved = cql_keywords - cql_keywords_unreserved
"""
Set of reserved keywords in CQL.
"""

_encoder = Encoder()


class Metadata(object):
    """
    Holds a representation of the cluster schema and topology.
    """

    cluster_name = None
    """ The string name of the cluster. """

    keyspaces = None
    """
    A map from keyspace names to matching :class:`~.KeyspaceMetadata` instances.
    """

    partitioner = None
    """
    The string name of the partitioner for the cluster.
    """

    token_map = None
    """ A :class:`~.TokenMap` instance describing the ring topology. """

    dbaas = False
    """ A boolean indicating if connected to a DBaaS cluster """

    def __init__(self):
        self.keyspaces = {}
        self.dbaas = False
        self._hosts = {}
        self._hosts_lock = RLock()

    def export_schema_as_string(self):
        """
        Returns a string that can be executed as a query in order to recreate
        the entire schema.  The string is formatted to be human readable.
        """
        return "\n\n".join(ks.export_as_string() for ks in self.keyspaces.values())

    def refresh(self, connection, timeout, target_type=None, change_type=None, **kwargs):

        server_version = self.get_host(connection.endpoint).release_version
        dse_version = self.get_host(connection.endpoint).dse_version
        parser = get_schema_parser(connection, server_version, dse_version, timeout)

        if not target_type:
            self._rebuild_all(parser)
            return

        tt_lower = target_type.lower()
        try:
            parse_method = getattr(parser, 'get_' + tt_lower)
            meta = parse_method(self.keyspaces, **kwargs)
            if meta:
                update_method = getattr(self, '_update_' + tt_lower)
                if tt_lower == 'keyspace' and connection.protocol_version < 3:
                    # we didn't have 'type' target in legacy protocol versions, so we need to query those too
                    user_types = parser.get_types_map(self.keyspaces, **kwargs)
                    self._update_keyspace(meta, user_types)
                else:
                    update_method(meta)
            else:
                drop_method = getattr(self, '_drop_' + tt_lower)
                drop_method(**kwargs)
        except AttributeError:
            raise ValueError("Unknown schema target_type: '%s'" % target_type)

    def _rebuild_all(self, parser):
        current_keyspaces = set()
        for keyspace_meta in parser.get_all_keyspaces():
            current_keyspaces.add(keyspace_meta.name)
            old_keyspace_meta = self.keyspaces.get(keyspace_meta.name, None)
            self.keyspaces[keyspace_meta.name] = keyspace_meta
            if old_keyspace_meta:
                self._keyspace_updated(keyspace_meta.name)
            else:
                self._keyspace_added(keyspace_meta.name)

        # remove not-just-added keyspaces
        removed_keyspaces = [name for name in self.keyspaces.keys()
                             if name not in current_keyspaces]
        self.keyspaces = dict((name, meta) for name, meta in self.keyspaces.items()
                              if name in current_keyspaces)
        for ksname in removed_keyspaces:
            self._keyspace_removed(ksname)

    def _update_keyspace(self, keyspace_meta, new_user_types=None):
        ks_name = keyspace_meta.name
        old_keyspace_meta = self.keyspaces.get(ks_name, None)
        self.keyspaces[ks_name] = keyspace_meta
        if old_keyspace_meta:
            keyspace_meta.tables = old_keyspace_meta.tables
            keyspace_meta.user_types = new_user_types if new_user_types is not None else old_keyspace_meta.user_types
            keyspace_meta.indexes = old_keyspace_meta.indexes
            keyspace_meta.functions = old_keyspace_meta.functions
            keyspace_meta.aggregates = old_keyspace_meta.aggregates
            keyspace_meta.views = old_keyspace_meta.views
            if (keyspace_meta.replication_strategy != old_keyspace_meta.replication_strategy):
                self._keyspace_updated(ks_name)
        else:
            self._keyspace_added(ks_name)

    def _drop_keyspace(self, keyspace):
        if self.keyspaces.pop(keyspace, None):
            self._keyspace_removed(keyspace)

    def _update_table(self, meta):
        try:
            keyspace_meta = self.keyspaces[meta.keyspace_name]
            # this is unfortunate, but protocol v4 does not differentiate
            # between events for tables and views. <parser>.get_table will
            # return one or the other based on the query results.
            # Here we deal with that.
            if isinstance(meta, TableMetadata):
                keyspace_meta._add_table_metadata(meta)
            else:
                keyspace_meta._add_view_metadata(meta)
        except KeyError:
            # can happen if keyspace disappears while processing async event
            pass

    def _drop_table(self, keyspace, table):
        try:
            keyspace_meta = self.keyspaces[keyspace]
            keyspace_meta._drop_table_metadata(table)  # handles either table or view
        except KeyError:
            # can happen if keyspace disappears while processing async event
            pass

    def _update_type(self, type_meta):
        try:
            self.keyspaces[type_meta.keyspace].user_types[type_meta.name] = type_meta
        except KeyError:
            # can happen if keyspace disappears while processing async event
            pass

    def _drop_type(self, keyspace, type):
        try:
            self.keyspaces[keyspace].user_types.pop(type, None)
        except KeyError:
            # can happen if keyspace disappears while processing async event
            pass

    def _update_function(self, function_meta):
        try:
            self.keyspaces[function_meta.keyspace].functions[function_meta.signature] = function_meta
        except KeyError:
            # can happen if keyspace disappears while processing async event
            pass

    def _drop_function(self, keyspace, function):
        try:
            self.keyspaces[keyspace].functions.pop(function.signature, None)
        except KeyError:
            pass

    def _update_aggregate(self, aggregate_meta):
        try:
            self.keyspaces[aggregate_meta.keyspace].aggregates[aggregate_meta.signature] = aggregate_meta
        except KeyError:
            pass

    def _drop_aggregate(self, keyspace, aggregate):
        try:
            self.keyspaces[keyspace].aggregates.pop(aggregate.signature, None)
        except KeyError:
            pass

    def _keyspace_added(self, ksname):
        if self.token_map:
            self.token_map.rebuild_keyspace(ksname, build_if_absent=False)

    def _keyspace_updated(self, ksname):
        if self.token_map:
            self.token_map.rebuild_keyspace(ksname, build_if_absent=False)

    def _keyspace_removed(self, ksname):
        if self.token_map:
            self.token_map.remove_keyspace(ksname)

    def rebuild_token_map(self, partitioner, token_map):
        """
        Rebuild our view of the topology from fresh rows from the
        system topology tables.
        For internal use only.
        """
        self.partitioner = partitioner
        if partitioner.endswith('RandomPartitioner'):
            token_class = MD5Token
        elif partitioner.endswith('Murmur3Partitioner'):
            token_class = Murmur3Token
        elif partitioner.endswith('ByteOrderedPartitioner'):
            token_class = BytesToken
        else:
            self.token_map = None
            return

        token_to_host_owner = {}
        ring = []
        for host, token_strings in token_map.items():
            for token_string in token_strings:
                token = token_class.from_string(token_string)
                ring.append(token)
                token_to_host_owner[token] = host

        all_tokens = sorted(ring)
        self.token_map = TokenMap(
            token_class, token_to_host_owner, all_tokens, self)

    def get_replicas(self, keyspace, key):
        """
        Returns a list of :class:`.Host` instances that are replicas for a given
        partition key.
        """
        t = self.token_map
        if not t:
            return []
        try:
            return t.get_replicas(keyspace, t.token_class.from_key(key))
        except NoMurmur3:
            return []

    def can_support_partitioner(self):
        if self.partitioner.endswith('Murmur3Partitioner') and murmur3 is None:
            return False
        else:
            return True

    def add_or_return_host(self, host):
        """
        Returns a tuple (host, new), where ``host`` is a Host
        instance, and ``new`` is a bool indicating whether
        the host was newly added.
        """
        with self._hosts_lock:
            try:
                return self._hosts[host.endpoint], False
            except KeyError:
                self._hosts[host.endpoint] = host
                return host, True

    def remove_host(self, host):
        with self._hosts_lock:
            return bool(self._hosts.pop(host.endpoint, False))

    def get_host(self, endpoint_or_address, port=None):
        """
        Find a host in the metadata for a specific endpoint. If a string inet address and port are passed,
        iterate all hosts to match the :attr:`~.pool.Host.broadcast_rpc_address` and
        :attr:`~.pool.Host.broadcast_rpc_port`attributes.
        """
        if not isinstance(endpoint_or_address, EndPoint):
            return self._get_host_by_address(endpoint_or_address, port)

        return self._hosts.get(endpoint_or_address)

    def _get_host_by_address(self, address, port=None):
        for host in self._hosts.values():
            if (host.broadcast_rpc_address == address and
                    (port is None or host.broadcast_rpc_port is None or host.broadcast_rpc_port == port)):
                return host

        return None

    def all_hosts(self):
        """
        Returns a list of all known :class:`.Host` instances in the cluster.
        """
        with self._hosts_lock:
            return list(self._hosts.values())


REPLICATION_STRATEGY_CLASS_PREFIX = "org.apache.cassandra.locator."


def trim_if_startswith(s, prefix):
    if s.startswith(prefix):
        return s[len(prefix):]
    return s


_replication_strategies = {}


class ReplicationStrategyTypeType(type):
    def __new__(metacls, name, bases, dct):
        dct.setdefault('name', name)
        cls = type.__new__(metacls, name, bases, dct)
        if not name.startswith('_'):
            _replication_strategies[name] = cls
        return cls



class _ReplicationStrategy(object, metaclass=ReplicationStrategyTypeType):
    options_map = None

    @classmethod
    def create(cls, strategy_class, options_map):
        if not strategy_class:
            return None

        strategy_name = trim_if_startswith(strategy_class, REPLICATION_STRATEGY_CLASS_PREFIX)

        rs_class = _replication_strategies.get(strategy_name, None)
        if rs_class is None:
            rs_class = _UnknownStrategyBuilder(strategy_name)
            _replication_strategies[strategy_name] = rs_class

        try:
            rs_instance = rs_class(options_map)
        except Exception as exc:
            log.warning("Failed creating %s with options %s: %s", strategy_name, options_map, exc)
            return None

        return rs_instance

    def make_token_replica_map(self, token_to_host_owner, ring):
        raise NotImplementedError()

    def export_for_schema(self):
        raise NotImplementedError()


ReplicationStrategy = _ReplicationStrategy


class _UnknownStrategyBuilder(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, options_map):
        strategy_instance = _UnknownStrategy(self.name, options_map)
        return strategy_instance


class _UnknownStrategy(ReplicationStrategy):
    def __init__(self, name, options_map):
        self.name = name
        self.options_map = options_map.copy() if options_map is not None else dict()
        self.options_map['class'] = self.name

    def __eq__(self, other):
        return (isinstance(other, _UnknownStrategy) and
                self.name == other.name and
                self.options_map == other.options_map)

    def export_for_schema(self):
        """
        Returns a string version of these replication options which are
        suitable for use in a CREATE KEYSPACE statement.
        """
        if self.options_map:
            return dict((str(key), str(value)) for key, value in self.options_map.items())
        return "{'class': '%s'}" % (self.name, )

    def make_token_replica_map(self, token_to_host_owner, ring):
        return {}


class ReplicationFactor(object):
    """
    Represent the replication factor of a keyspace.
    """

    all_replicas = None
    """
    The number of total replicas.
    """

    full_replicas = None
    """
    The number of replicas that own a full copy of the data. This is the same
    than `all_replicas` when transient replication is not enabled.
    """

    transient_replicas = None
    """
    The number of transient replicas.

    Only set if the keyspace has transient replication enabled.
    """

    def __init__(self, all_replicas, transient_replicas=None):
        self.all_replicas = all_replicas
        self.transient_replicas = transient_replicas
        self.full_replicas = (all_replicas - transient_replicas) if transient_replicas else all_replicas

    @staticmethod
    def create(rf):
        """
        Given the inputted replication factor string, parse and return the ReplicationFactor instance.
        """
        transient_replicas = None
        try:
            all_replicas = int(rf)
        except ValueError:
            try:
                rf = rf.split('/')
                all_replicas, transient_replicas = int(rf[0]), int(rf[1])
            except Exception:
                raise ValueError("Unable to determine replication factor from: {}".format(rf))

        return ReplicationFactor(all_replicas, transient_replicas)

    def __str__(self):
        return ("%d/%d" % (self.all_replicas, self.transient_replicas) if self.transient_replicas
                else "%d" % self.all_replicas)

    def __eq__(self, other):
        if not isinstance(other, ReplicationFactor):
            return False

        return self.all_replicas == other.all_replicas and self.full_replicas == other.full_replicas


class SimpleStrategy(ReplicationStrategy):

    replication_factor_info = None
    """
    A :class:`cassandra.metadata.ReplicationFactor` instance.
    """

    @property
    def replication_factor(self):
        """
        The replication factor for this keyspace.

        For backward compatibility, this returns the
        :attr:`cassandra.metadata.ReplicationFactor.full_replicas` value of
        :attr:`cassandra.metadata.SimpleStrategy.replication_factor_info`.
        """
        return self.replication_factor_info.full_replicas

    def __init__(self, options_map):
        self.replication_factor_info = ReplicationFactor.create(options_map['replication_factor'])

    def make_token_replica_map(self, token_to_host_owner, ring):
        replica_map = {}
        for i in range(len(ring)):
            j, hosts = 0, list()
            while len(hosts) < self.replication_factor and j < len(ring):
                token = ring[(i + j) % len(ring)]
                host = token_to_host_owner[token]
                if host not in hosts:
                    hosts.append(host)
                j += 1

            replica_map[ring[i]] = hosts
        return replica_map

    def export_for_schema(self):
        """
        Returns a string version of these replication options which are
        suitable for use in a CREATE KEYSPACE statement.
        """
        return "{'class': 'SimpleStrategy', 'replication_factor': '%s'}" \
               % (str(self.replication_factor_info),)

    def __eq__(self, other):
        if not isinstance(other, SimpleStrategy):
            return False

        return str(self.replication_factor_info) == str(other.replication_factor_info)


class NetworkTopologyStrategy(ReplicationStrategy):

    dc_replication_factors_info = None
    """
    A map of datacenter names to the :class:`cassandra.metadata.ReplicationFactor` instance for that DC.
    """

    dc_replication_factors = None
    """
    A map of datacenter names to the replication factor for that DC.

    For backward compatibility, this maps to the :attr:`cassandra.metadata.ReplicationFactor.full_replicas`
    value of the :attr:`cassandra.metadata.NetworkTopologyStrategy.dc_replication_factors_info` dict.
    """

    def __init__(self, dc_replication_factors):
        self.dc_replication_factors_info = dict(
            (str(k), ReplicationFactor.create(v)) for k, v in dc_replication_factors.items())
        self.dc_replication_factors = dict(
            (dc, rf.full_replicas) for dc, rf in self.dc_replication_factors_info.items())

    def make_token_replica_map(self, token_to_host_owner, ring):
        dc_rf_map = dict(
            (dc, full_replicas) for dc, full_replicas in self.dc_replication_factors.items()
            if full_replicas > 0)

        # build a map of DCs to lists of indexes into `ring` for tokens that
        # belong to that DC
        dc_to_token_offset = defaultdict(list)
        dc_racks = defaultdict(set)
        hosts_per_dc = defaultdict(set)
        for i, token in enumerate(ring):
            host = token_to_host_owner[token]
            dc_to_token_offset[host.datacenter].append(i)
            if host.datacenter and host.rack:
                dc_racks[host.datacenter].add(host.rack)
                hosts_per_dc[host.datacenter].add(host)

        # A map of DCs to an index into the dc_to_token_offset value for that dc.
        # This is how we keep track of advancing around the ring for each DC.
        dc_to_current_index = defaultdict(int)

        replica_map = defaultdict(list)
        for i in range(len(ring)):
            replicas = replica_map[ring[i]]

            # go through each DC and find the replicas in that DC
            for dc in dc_to_token_offset.keys():
                if dc not in dc_rf_map:
                    continue

                # advance our per-DC index until we're up to at least the
                # current token in the ring
                token_offsets = dc_to_token_offset[dc]
                index = dc_to_current_index[dc]
                num_tokens = len(token_offsets)
                while index < num_tokens and token_offsets[index] < i:
                    index += 1
                dc_to_current_index[dc] = index

                replicas_remaining = dc_rf_map[dc]
                replicas_this_dc = 0
                skipped_hosts = []
                racks_placed = set()
                racks_this_dc = dc_racks[dc]
                hosts_this_dc = len(hosts_per_dc[dc])

                for token_offset_index in range(index, index+num_tokens):
                    if token_offset_index >= len(token_offsets):
                        token_offset_index = token_offset_index - len(token_offsets)

                    token_offset = token_offsets[token_offset_index]
                    host = token_to_host_owner[ring[token_offset]]
                    if replicas_remaining == 0 or replicas_this_dc == hosts_this_dc:
                        break

                    if host in replicas:
                        continue

                    if host.rack in racks_placed and len(racks_placed) < len(racks_this_dc):
                        skipped_hosts.append(host)
                        continue

                    replicas.append(host)
                    replicas_this_dc += 1
                    replicas_remaining -= 1
                    racks_placed.add(host.rack)

                    if len(racks_placed) == len(racks_this_dc):
                        for host in skipped_hosts:
                            if replicas_remaining == 0:
                                break
                            replicas.append(host)
                            replicas_remaining -= 1
                        del skipped_hosts[:]

        return replica_map

    def export_for_schema(self):
        """
        Returns a string version of these replication options which are
        suitable for use in a CREATE KEYSPACE statement.
        """
        ret = "{'class': 'NetworkTopologyStrategy'"
        for dc, rf in sorted(self.dc_replication_factors_info.items()):
            ret += ", '%s': '%s'" % (dc, str(rf))
        return ret + "}"

    def __eq__(self, other):
        if not isinstance(other, NetworkTopologyStrategy):
            return False

        return self.dc_replication_factors_info == other.dc_replication_factors_info


class LocalStrategy(ReplicationStrategy):
    def __init__(self, options_map):
        pass

    def make_token_replica_map(self, token_to_host_owner, ring):
        return {}

    def export_for_schema(self):
        """
        Returns a string version of these replication options which are
        suitable for use in a CREATE KEYSPACE statement.
        """
        return "{'class': 'LocalStrategy'}"

    def __eq__(self, other):
        return isinstance(other, LocalStrategy)


class KeyspaceMetadata(object):
    """
    A representation of the schema for a single keyspace.
    """

    name = None
    """ The string name of the keyspace. """

    durable_writes = True
    """
    A boolean indicating whether durable writes are enabled for this keyspace
    or not.
    """

    replication_strategy = None
    """
    A :class:`.ReplicationStrategy` subclass object.
    """

    tables = None
    """
    A map from table names to instances of :class:`~.TableMetadata`.
    """

    indexes = None
    """
    A dict mapping index names to :class:`.IndexMetadata` instances.
    """

    user_types = None
    """
    A map from user-defined type names to instances of :class:`~cassandra.metadata.UserType`.

    .. versionadded:: 2.1.0
    """

    functions = None
    """
    A map from user-defined function signatures to instances of :class:`~cassandra.metadata.Function`.

    .. versionadded:: 2.6.0
    """

    aggregates = None
    """
    A map from user-defined aggregate signatures to instances of :class:`~cassandra.metadata.Aggregate`.

    .. versionadded:: 2.6.0
    """

    views = None
    """
    A dict mapping view names to :class:`.MaterializedViewMetadata` instances.
    """

    virtual = False
    """
    A boolean indicating if this is a virtual keyspace or not. Always ``False``
    for clusters running Cassandra pre-4.0 and DSE pre-6.7 versions.

    .. versionadded:: 3.15
    """

    graph_engine = None
    """
    A string indicating whether a graph engine is enabled for this keyspace (Core/Classic).
    """

    _exc_info = None
    """ set if metadata parsing failed """

    def __init__(self, name, durable_writes, strategy_class, strategy_options, graph_engine=None):
        self.name = name
        self.durable_writes = durable_writes
        self.replication_strategy = ReplicationStrategy.create(strategy_class, strategy_options)
        self.tables = {}
        self.indexes = {}
        self.user_types = {}
        self.functions = {}
        self.aggregates = {}
        self.views = {}
        self.graph_engine = graph_engine

    @property
    def is_graph_enabled(self):
        return self.graph_engine is not None

    def export_as_string(self):
        """
        Returns a CQL query string that can be used to recreate the entire keyspace,
        including user-defined types and tables.
        """
        # Make sure tables with vertex are exported before tables with edges
        tables_with_vertex = [t for t in self.tables.values() if hasattr(t, 'vertex') and t.vertex]
        other_tables = [t for t in self.tables.values() if t not in tables_with_vertex]

        cql = "\n\n".join(
            [self.as_cql_query() + ';'] +
            self.user_type_strings() +
            [f.export_as_string() for f in self.functions.values()] +
            [a.export_as_string() for a in self.aggregates.values()] +
            [t.export_as_string() for t in tables_with_vertex + other_tables])

        if self._exc_info:
            import traceback
            ret = "/*\nWarning: Keyspace %s is incomplete because of an error processing metadata.\n" % \
                  (self.name)
            for line in traceback.format_exception(*self._exc_info):
                ret += line
            ret += "\nApproximate structure, for reference:\n(this should not be used to reproduce this schema)\n\n%s\n*/" % cql
            return ret
        if self.virtual:
            return ("/*\nWarning: Keyspace {ks} is a virtual keyspace and cannot be recreated with CQL.\n"
                    "Structure, for reference:*/\n"
                    "{cql}\n"
                    "").format(ks=self.name, cql=cql)
        return cql

    def as_cql_query(self):
        """
        Returns a CQL query string that can be used to recreate just this keyspace,
        not including user-defined types and tables.
        """
        if self.virtual:
            return "// VIRTUAL KEYSPACE {}".format(protect_name(self.name))
        ret = "CREATE KEYSPACE %s WITH replication = %s " % (
            protect_name(self.name),
            self.replication_strategy.export_for_schema())
        ret = ret + (' AND durable_writes = %s' % ("true" if self.durable_writes else "false"))
        if self.graph_engine is not None:
            ret = ret + (" AND graph_engine = '%s'" % self.graph_engine)
        return ret

    def user_type_strings(self):
        user_type_strings = []
        user_types = self.user_types.copy()
        keys = sorted(user_types.keys())
        for k in keys:
            if k in user_types:
                self.resolve_user_types(k, user_types, user_type_strings)
        return user_type_strings

    def resolve_user_types(self, key, user_types, user_type_strings):
        user_type = user_types.pop(key)
        for type_name in user_type.field_types:
            for sub_type in types.cql_types_from_string(type_name):
                if sub_type in user_types:
                    self.resolve_user_types(sub_type, user_types, user_type_strings)
        user_type_strings.append(user_type.export_as_string())

    def _add_table_metadata(self, table_metadata):
        old_indexes = {}
        old_meta = self.tables.get(table_metadata.name, None)
        if old_meta:
            # views are not queried with table, so they must be transferred to new
            table_metadata.views = old_meta.views
            # indexes will be updated with what is on the new metadata
            old_indexes = old_meta.indexes

        # note the intentional order of add before remove
        # this makes sure the maps are never absent something that existed before this update
        for index_name, index_metadata in table_metadata.indexes.items():
            self.indexes[index_name] = index_metadata

        for index_name in (n for n in old_indexes if n not in table_metadata.indexes):
            self.indexes.pop(index_name, None)

        self.tables[table_metadata.name] = table_metadata

    def _drop_table_metadata(self, table_name):
        table_meta = self.tables.pop(table_name, None)
        if table_meta:
            for index_name in table_meta.indexes:
                self.indexes.pop(index_name, None)
            for view_name in table_meta.views:
                self.views.pop(view_name, None)
            return
        # we can't tell table drops from views, so drop both
        # (name is unique among them, within a keyspace)
        view_meta = self.views.pop(table_name, None)
        if view_meta:
            try:
                self.tables[view_meta.base_table_name].views.pop(table_name, None)
            except KeyError:
                pass

    def _add_view_metadata(self, view_metadata):
        try:
            self.tables[view_metadata.base_table_name].views[view_metadata.name] = view_metadata
            self.views[view_metadata.name] = view_metadata
        except KeyError:
            pass


class UserType(object):
    """
    A user defined type, as created by ``CREATE TYPE`` statements.

    User-defined types were introduced in Cassandra 2.1.

    .. versionadded:: 2.1.0
    """

    keyspace = None
    """
    The string name of the keyspace in which this type is defined.
    """

    name = None
    """
    The name of this type.
    """

    field_names = None
    """
    An ordered list of the names for each field in this user-defined type.
    """

    field_types = None
    """
    An ordered list of the types for each field in this user-defined type.
    """

    def __init__(self, keyspace, name, field_names, field_types):
        self.keyspace = keyspace
        self.name = name
        # non-frozen collections can return None
        self.field_names = field_names or []
        self.field_types = field_types or []

    def as_cql_query(self, formatted=False):
        """
        Returns a CQL query that can be used to recreate this type.
        If `formatted` is set to :const:`True`, extra whitespace will
        be added to make the query more readable.
        """
        ret = "CREATE TYPE %s.%s (%s" % (
            protect_name(self.keyspace),
            protect_name(self.name),
            "\n" if formatted else "")

        if formatted:
            field_join = ",\n"
            padding = "    "
        else:
            field_join = ", "
            padding = ""

        fields = []
        for field_name, field_type in zip(self.field_names, self.field_types):
            fields.append("%s %s" % (protect_name(field_name), field_type))

        ret += field_join.join("%s%s" % (padding, field) for field in fields)
        ret += "\n)" if formatted else ")"
        return ret

    def export_as_string(self):
        return self.as_cql_query(formatted=True) + ';'


class Aggregate(object):
    """
    A user defined aggregate function, as created by ``CREATE AGGREGATE`` statements.

    Aggregate functions were introduced in Cassandra 2.2

    .. versionadded:: 2.6.0
    """

    keyspace = None
    """
    The string name of the keyspace in which this aggregate is defined
    """

    name = None
    """
    The name of this aggregate
    """

    argument_types = None
    """
    An ordered list of the types for each argument to the aggregate
    """

    final_func = None
    """
    Name of a final function
    """

    initial_condition = None
    """
    Initial condition of the aggregate
    """

    return_type = None
    """
    Return type of the aggregate
    """

    state_func = None
    """
    Name of a state function
    """

    state_type = None
    """
    Type of the aggregate state
    """

    deterministic = None
    """
    Flag indicating if this function is guaranteed to produce the same result
    for a particular input and state. This is available only with DSE >=6.0.
    """

    def __init__(self, keyspace, name, argument_types, state_func,
                 state_type, final_func, initial_condition, return_type,
                 deterministic):
        self.keyspace = keyspace
        self.name = name
        self.argument_types = argument_types
        self.state_func = state_func
        self.state_type = state_type
        self.final_func = final_func
        self.initial_condition = initial_condition
        self.return_type = return_type
        self.deterministic = deterministic

    def as_cql_query(self, formatted=False):
        """
        Returns a CQL query that can be used to recreate this aggregate.
        If `formatted` is set to :const:`True`, extra whitespace will
        be added to make the query more readable.
        """
        sep = '\n    ' if formatted else ' '
        keyspace = protect_name(self.keyspace)
        name = protect_name(self.name)
        type_list = ', '.join([types.strip_frozen(arg_type) for arg_type in self.argument_types])
        state_func = protect_name(self.state_func)
        state_type = types.strip_frozen(self.state_type)

        ret = "CREATE AGGREGATE %(keyspace)s.%(name)s(%(type_list)s)%(sep)s" \
              "SFUNC %(state_func)s%(sep)s" \
              "STYPE %(state_type)s" % locals()

        ret += ''.join((sep, 'FINALFUNC ', protect_name(self.final_func))) if self.final_func else ''
        ret += ''.join((sep, 'INITCOND ', self.initial_condition)) if self.initial_condition is not None else ''
        ret += '{}DETERMINISTIC'.format(sep) if self.deterministic else ''

        return ret

    def export_as_string(self):
        return self.as_cql_query(formatted=True) + ';'

    @property
    def signature(self):
        return SignatureDescriptor.format_signature(self.name, self.argument_types)


class Function(object):
    """
    A user defined function, as created by ``CREATE FUNCTION`` statements.

    User-defined functions were introduced in Cassandra 2.2

    .. versionadded:: 2.6.0
    """

    keyspace = None
    """
    The string name of the keyspace in which this function is defined
    """

    name = None
    """
    The name of this function
    """

    argument_types = None
    """
    An ordered list of the types for each argument to the function
    """

    argument_names = None
    """
    An ordered list of the names of each argument to the function
    """

    return_type = None
    """
    Return type of the function
    """

    language = None
    """
    Language of the function body
    """

    body = None
    """
    Function body string
    """

    called_on_null_input = None
    """
    Flag indicating whether this function should be called for rows with null values
    (convenience function to avoid handling nulls explicitly if the result will just be null)
    """

    deterministic = None
    """
    Flag indicating if this function is guaranteed to produce the same result
    for a particular input. This is available only for DSE >=6.0.
    """

    monotonic = None
    """
    Flag indicating if this function is guaranteed to increase or decrease
    monotonically on any of its arguments. This is available only for DSE >=6.0.
    """

    monotonic_on = None
    """
    A list containing the argument or arguments over which this function is
    monotonic. This is available only for DSE >=6.0.
    """

    def __init__(self, keyspace, name, argument_types, argument_names,
                 return_type, language, body, called_on_null_input,
                 deterministic, monotonic, monotonic_on):
        self.keyspace = keyspace
        self.name = name
        self.argument_types = argument_types
        # argument_types (frozen<list<>>) will always be a list
        # argument_name is not frozen in C* < 3.0 and may return None
        self.argument_names = argument_names or []
        self.return_type = return_type
        self.language = language
        self.body = body
        self.called_on_null_input = called_on_null_input
        self.deterministic = deterministic
        self.monotonic = monotonic
        self.monotonic_on = monotonic_on

    def as_cql_query(self, formatted=False):
        """
        Returns a CQL query that can be used to recreate this function.
        If `formatted` is set to :const:`True`, extra whitespace will
        be added to make the query more readable.
        """
        sep = '\n    ' if formatted else ' '
        keyspace = protect_name(self.keyspace)
        name = protect_name(self.name)
        arg_list = ', '.join(["%s %s" % (protect_name(n), types.strip_frozen(t))
                             for n, t in zip(self.argument_names, self.argument_types)])
        typ = self.return_type
        lang = self.language
        body = self.body
        on_null = "CALLED" if self.called_on_null_input else "RETURNS NULL"
        deterministic_token = ('DETERMINISTIC{}'.format(sep)
                               if self.deterministic else
                               '')
        monotonic_tokens = ''  # default for nonmonotonic function
        if self.monotonic:
            # monotonic on all arguments; ignore self.monotonic_on
            monotonic_tokens = 'MONOTONIC{}'.format(sep)
        elif self.monotonic_on:
            # if monotonic == False and monotonic_on is nonempty, we know that
            # monotonicity was specified with MONOTONIC ON <arg>, so there's
            # exactly 1 value there
            monotonic_tokens = 'MONOTONIC ON {}{}'.format(self.monotonic_on[0],
                                                          sep)

        return "CREATE FUNCTION %(keyspace)s.%(name)s(%(arg_list)s)%(sep)s" \
               "%(on_null)s ON NULL INPUT%(sep)s" \
               "RETURNS %(typ)s%(sep)s" \
               "%(deterministic_token)s" \
               "%(monotonic_tokens)s" \
               "LANGUAGE %(lang)s%(sep)s" \
               "AS $$%(body)s$$" % locals()

    def export_as_string(self):
        return self.as_cql_query(formatted=True) + ';'

    @property
    def signature(self):
        return SignatureDescriptor.format_signature(self.name, self.argument_types)


class TableMetadata(object):
    """
    A representation of the schema for a single table.
    """

    keyspace_name = None
    """ String name of this Table's keyspace """

    name = None
    """ The string name of the table. """

    partition_key = None
    """
    A list of :class:`.ColumnMetadata` instances representing the columns in
    the partition key for this table.  This will always hold at least one
    column.
    """

    clustering_key = None
    """
    A list of :class:`.ColumnMetadata` instances representing the columns
    in the clustering key for this table.  These are all of the
    :attr:`.primary_key` columns that are not in the :attr:`.partition_key`.

    Note that a table may have no clustering keys, in which case this will
    be an empty list.
    """

    @property
    def primary_key(self):
        """
        A list of :class:`.ColumnMetadata` representing the components of
        the primary key for this table.
        """
        return self.partition_key + self.clustering_key

    columns = None
    """
    A dict mapping column names to :class:`.ColumnMetadata` instances.
    """

    indexes = None
    """
    A dict mapping index names to :class:`.IndexMetadata` instances.
    """

    is_compact_storage = False

    options = None
    """
    A dict mapping table option names to their specific settings for this
    table.
    """

    compaction_options = {
        "min_compaction_threshold": "min_threshold",
        "max_compaction_threshold": "max_threshold",
        "compaction_strategy_class": "class"}

    triggers = None
    """
    A dict mapping trigger names to :class:`.TriggerMetadata` instances.
    """

    views = None
    """
    A dict mapping view names to :class:`.MaterializedViewMetadata` instances.
    """

    _exc_info = None
    """ set if metadata parsing failed """

    virtual = False
    """
    A boolean indicating if this is a virtual table or not. Always ``False``
    for clusters running Cassandra pre-4.0 and DSE pre-6.7 versions.

    .. versionadded:: 3.15
    """

    @property
    def is_cql_compatible(self):
        """
        A boolean indicating if this table can be represented as CQL in export
        """
        if self.virtual:
            return False
        comparator = getattr(self, 'comparator', None)
        if comparator:
            # no compact storage with more than one column beyond PK if there
            # are clustering columns
            incompatible = (self.is_compact_storage and
                            len(self.columns) > len(self.primary_key) + 1 and
                            len(self.clustering_key) >= 1)

            return not incompatible
        return True

    extensions = None
    """
    Metadata describing configuration for table extensions
    """

    def __init__(self, keyspace_name, name, partition_key=None, clustering_key=None, columns=None, triggers=None, options=None, virtual=False):
        self.keyspace_name = keyspace_name
        self.name = name
        self.partition_key = [] if partition_key is None else partition_key
        self.clustering_key = [] if clustering_key is None else clustering_key
        self.columns = OrderedDict() if columns is None else columns
        self.indexes = {}
        self.options = {} if options is None else options
        self.comparator = None
        self.triggers = OrderedDict() if triggers is None else triggers
        self.views = {}
        self.virtual = virtual

    def export_as_string(self):
        """
        Returns a string of CQL queries that can be used to recreate this table
        along with all indexes on it.  The returned string is formatted to
        be human readable.
        """
        if self._exc_info:
            import traceback
            ret = "/*\nWarning: Table %s.%s is incomplete because of an error processing metadata.\n" % \
                  (self.keyspace_name, self.name)
            for line in traceback.format_exception(*self._exc_info):
                ret += line
            ret += "\nApproximate structure, for reference:\n(this should not be used to reproduce this schema)\n\n%s\n*/" % self._all_as_cql()
        elif not self.is_cql_compatible:
            # If we can't produce this table with CQL, comment inline
            ret = "/*\nWarning: Table %s.%s omitted because it has constructs not compatible with CQL (was created via legacy API).\n" % \
                  (self.keyspace_name, self.name)
            ret += "\nApproximate structure, for reference:\n(this should not be used to reproduce this schema)\n\n%s\n*/" % self._all_as_cql()
        elif self.virtual:
            ret = ('/*\nWarning: Table {ks}.{tab} is a virtual table and cannot be recreated with CQL.\n'
                   'Structure, for reference:\n'
                   '{cql}\n*/').format(ks=self.keyspace_name, tab=self.name, cql=self._all_as_cql())

        else:
            ret = self._all_as_cql()

        return ret

    def _all_as_cql(self):
        ret = self.as_cql_query(formatted=True)
        ret += ";"

        for index in self.indexes.values():
            ret += "\n%s;" % index.as_cql_query()

        for trigger_meta in self.triggers.values():
            ret += "\n%s;" % (trigger_meta.as_cql_query(),)

        for view_meta in self.views.values():
            ret += "\n\n%s;" % (view_meta.as_cql_query(formatted=True),)

        if self.extensions:
            registry = _RegisteredExtensionType._extension_registry
            for k in registry.keys() & self.extensions:  # no viewkeys on OrderedMapSerializeKey
                ext = registry[k]
                cql = ext.after_table_cql(self, k, self.extensions[k])
                if cql:
                    ret += "\n\n%s" % (cql,)

        return ret

    def as_cql_query(self, formatted=False):
        """
        Returns a CQL query that can be used to recreate this table (index
        creations are not included).  If `formatted` is set to :const:`True`,
        extra whitespace will be added to make the query human readable.
        """
        ret = "%s TABLE %s.%s (%s" % (
            ('VIRTUAL' if self.virtual else 'CREATE'),
            protect_name(self.keyspace_name),
            protect_name(self.name),
            "\n" if formatted else "")

        if formatted:
            column_join = ",\n"
            padding = "    "
        else:
            column_join = ", "
            padding = ""

        columns = []
        for col in self.columns.values():
            columns.append("%s %s%s" % (protect_name(col.name), col.cql_type, ' static' if col.is_static else ''))

        if len(self.partition_key) == 1 and not self.clustering_key:
            columns[0] += " PRIMARY KEY"

        ret += column_join.join("%s%s" % (padding, col) for col in columns)

        # primary key
        if len(self.partition_key) > 1 or self.clustering_key:
            ret += "%s%sPRIMARY KEY (" % (column_join, padding)

            if len(self.partition_key) > 1:
                ret += "(%s)" % ", ".join(protect_name(col.name) for col in self.partition_key)
            else:
                ret += protect_name(self.partition_key[0].name)

            if self.clustering_key:
                ret += ", %s" % ", ".join(protect_name(col.name) for col in self.clustering_key)

            ret += ")"

        # properties
        ret += "%s) WITH " % ("\n" if formatted else "")
        ret += self._property_string(formatted, self.clustering_key, self.options, self.is_compact_storage)

        return ret

    @classmethod
    def _property_string(cls, formatted, clustering_key, options_map, is_compact_storage=False):
        properties = []
        if is_compact_storage:
            properties.append("COMPACT STORAGE")

        if clustering_key:
            cluster_str = "CLUSTERING ORDER BY "

            inner = []
            for col in clustering_key:
                ordering = "DESC" if col.is_reversed else "ASC"
                inner.append("%s %s" % (protect_name(col.name), ordering))

            cluster_str += "(%s)" % ", ".join(inner)
            properties.append(cluster_str)

        properties.extend(cls._make_option_strings(options_map))

        join_str = "\n    AND " if formatted else " AND "
        return join_str.join(properties)

    @classmethod
    def _make_option_strings(cls, options_map):
        ret = []
        options_copy = dict(options_map.items())

        actual_options = json.loads(options_copy.pop('compaction_strategy_options', '{}'))
        value = options_copy.pop("compaction_strategy_class", None)
        actual_options.setdefault("class", value)

        compaction_option_strings = ["'%s': '%s'" % (k, v) for k, v in actual_options.items()]
        ret.append('compaction = {%s}' % ', '.join(compaction_option_strings))

        for system_table_name in cls.compaction_options.keys():
            options_copy.pop(system_table_name, None)  # delete if present
        options_copy.pop('compaction_strategy_option', None)

        if not options_copy.get('compression'):
            params = json.loads(options_copy.pop('compression_parameters', '{}'))
            param_strings = ["'%s': '%s'" % (k, v) for k, v in params.items()]
            ret.append('compression = {%s}' % ', '.join(param_strings))

        for name, value in options_copy.items():
            if value is not None:
                if name == "comment":
                    value = value or ""
                ret.append("%s = %s" % (name, protect_value(value)))

        return list(sorted(ret))


class TableMetadataV3(TableMetadata):
    """
    For C* 3.0+. `option_maps` take a superset of map names, so if  nothing
    changes structurally, new option maps can just be appended to the list.
    """
    compaction_options = {}

    option_maps = [
        'compaction', 'compression', 'caching',
        'nodesync'  # added DSE 6.0
    ]

    @property
    def is_cql_compatible(self):
        return True

    @classmethod
    def _make_option_strings(cls, options_map):
        ret = []
        options_copy = dict(options_map.items())

        for option in cls.option_maps:
            value = options_copy.get(option)
            if isinstance(value, Mapping):
                del options_copy[option]
                params = ("'%s': '%s'" % (k, v) for k, v in value.items())
                ret.append("%s = {%s}" % (option, ', '.join(params)))

        for name, value in options_copy.items():
            if value is not None:
                if name == "comment":
                    value = value or ""
                ret.append("%s = %s" % (name, protect_value(value)))

        return list(sorted(ret))


class TableMetadataDSE68(TableMetadataV3):

    vertex = None
    """A :class:`.VertexMetadata` instance, if graph enabled"""

    edge = None
    """A :class:`.EdgeMetadata` instance, if graph enabled"""

    def as_cql_query(self, formatted=False):
        ret = super(TableMetadataDSE68, self).as_cql_query(formatted)

        if self.vertex:
            ret += " AND VERTEX LABEL %s" % protect_name(self.vertex.label_name)

        if self.edge:
            ret += " AND EDGE LABEL %s" % protect_name(self.edge.label_name)

            ret += self._export_edge_as_cql(
                self.edge.from_label,
                self.edge.from_partition_key_columns,
                self.edge.from_clustering_columns, "FROM")

            ret += self._export_edge_as_cql(
                self.edge.to_label,
                self.edge.to_partition_key_columns,
                self.edge.to_clustering_columns, "TO")

        return ret

    @staticmethod
    def _export_edge_as_cql(label_name, partition_keys,
                            clustering_columns, keyword):
        ret = " %s %s(" % (keyword, protect_name(label_name))

        if len(partition_keys) == 1:
            ret += protect_name(partition_keys[0])
        else:
            ret += "(%s)" % ", ".join([protect_name(k) for k in partition_keys])

        if clustering_columns:
            ret += ", %s" % ", ".join([protect_name(k) for k in clustering_columns])
        ret += ")"

        return ret


class TableExtensionInterface(object):
    """
    Defines CQL/DDL for Cassandra table extensions.
    """
    # limited API for now. Could be expanded as new extension types materialize -- "extend_option_strings", for example
    @classmethod
    def after_table_cql(cls, ext_key, ext_blob):
        """
        Called to produce CQL/DDL to follow the table definition.
        Should contain requisite terminating semicolon(s).
        """
        pass


class _RegisteredExtensionType(type):

    _extension_registry = {}

    def __new__(mcs, name, bases, dct):
        cls = super(_RegisteredExtensionType, mcs).__new__(mcs, name, bases, dct)
        if name != 'RegisteredTableExtension':
            mcs._extension_registry[cls.name] = cls
        return cls


class RegisteredTableExtension(TableExtensionInterface, metaclass=_RegisteredExtensionType):
    """
    Extending this class registers it by name (associated by key in the `system_schema.tables.extensions` map).
    """
    name = None
    """
    Name of the extension (key in the map)
    """


def protect_name(name):
    return maybe_escape_name(name)


def protect_names(names):
    return [protect_name(n) for n in names]


def protect_value(value):
    if value is None:
        return 'NULL'
    if isinstance(value, (int, float, bool)):
        return str(value).lower()
    return "'%s'" % value.replace("'", "''")


valid_cql3_word_re = re.compile(r'^[a-z][0-9a-z_]*$')


def is_valid_name(name):
    if name is None:
        return False
    if name.lower() in cql_keywords_reserved:
        return False
    return valid_cql3_word_re.match(name) is not None


def maybe_escape_name(name):
    if is_valid_name(name):
        return name
    return escape_name(name)


def escape_name(name):
    return '"%s"' % (name.replace('"', '""'),)


class ColumnMetadata(object):
    """
    A representation of a single column in a table.
    """

    table = None
    """ The :class:`.TableMetadata` this column belongs to. """

    name = None
    """ The string name of this column. """

    cql_type = None
    """
    The CQL type for the column.
    """

    is_static = False
    """
    If this column is static (available in Cassandra 2.1+), this will
    be :const:`True`, otherwise :const:`False`.
    """

    is_reversed = False
    """
    If this column is reversed (DESC) as in clustering order
    """

    _cass_type = None

    def __init__(self, table_metadata, column_name, cql_type, is_static=False, is_reversed=False):
        self.table = table_metadata
        self.name = column_name
        self.cql_type = cql_type
        self.is_static = is_static
        self.is_reversed = is_reversed

    def __str__(self):
        return "%s %s" % (self.name, self.cql_type)


class IndexMetadata(object):
    """
    A representation of a secondary index on a column.
    """
    keyspace_name = None
    """ A string name of the keyspace. """

    table_name = None
    """ A string name of the table this index is on. """

    name = None
    """ A string name for the index. """

    kind = None
    """ A string representing the kind of index (COMPOSITE, CUSTOM,...). """

    index_options = {}
    """ A dict of index options. """

    def __init__(self, keyspace_name, table_name, index_name, kind, index_options):
        self.keyspace_name = keyspace_name
        self.table_name = table_name
        self.name = index_name
        self.kind = kind
        self.index_options = index_options

    def as_cql_query(self):
        """
        Returns a CQL query that can be used to recreate this index.
        """
        options = dict(self.index_options)
        index_target = options.pop("target")
        if self.kind != "CUSTOM":
            return "CREATE INDEX %s ON %s.%s (%s)" % (
                protect_name(self.name),
                protect_name(self.keyspace_name),
                protect_name(self.table_name),
                index_target)
        else:
            class_name = options.pop("class_name")
            ret = "CREATE CUSTOM INDEX %s ON %s.%s (%s) USING '%s'" % (
                protect_name(self.name),
                protect_name(self.keyspace_name),
                protect_name(self.table_name),
                index_target,
                class_name)
            if options:
                # PYTHON-1008: `ret` will always be a unicode
                opts_cql_encoded = _encoder.cql_encode_all_types(options, as_text_type=True)
                ret += " WITH OPTIONS = %s" % opts_cql_encoded
            return ret

    def export_as_string(self):
        """
        Returns a CQL query string that can be used to recreate this index.
        """
        return self.as_cql_query() + ';'


class TokenMap(object):
    """
    Information about the layout of the ring.
    """

    token_class = None
    """
    A subclass of :class:`.Token`, depending on what partitioner the cluster uses.
    """

    token_to_host_owner = None
    """
    A map of :class:`.Token` objects to the :class:`.Host` that owns that token.
    """

    tokens_to_hosts_by_ks = None
    """
    A map of keyspace names to a nested map of :class:`.Token` objects to
    sets of :class:`.Host` objects.
    """

    ring = None
    """
    An ordered list of :class:`.Token` instances in the ring.
    """

    _metadata = None

    def __init__(self, token_class, token_to_host_owner, all_tokens, metadata):
        self.token_class = token_class
        self.ring = all_tokens
        self.token_to_host_owner = token_to_host_owner

        self.tokens_to_hosts_by_ks = {}
        self._metadata = metadata
        self._rebuild_lock = RLock()

    def rebuild_keyspace(self, keyspace, build_if_absent=False):
        with self._rebuild_lock:
            try:
                current = self.tokens_to_hosts_by_ks.get(keyspace, None)
                if (build_if_absent and current is None) or (not build_if_absent and current is not None):
                    ks_meta = self._metadata.keyspaces.get(keyspace)
                    if ks_meta:
                        replica_map = self.replica_map_for_keyspace(self._metadata.keyspaces[keyspace])
                        self.tokens_to_hosts_by_ks[keyspace] = replica_map
            except Exception:
                # should not happen normally, but we don't want to blow up queries because of unexpected meta state
                # bypass until new map is generated
                self.tokens_to_hosts_by_ks[keyspace] = {}
                log.exception("Failed creating a token map for keyspace '%s' with %s. PLEASE REPORT THIS: https://datastax-oss.atlassian.net/projects/PYTHON", keyspace, self.token_to_host_owner)

    def replica_map_for_keyspace(self, ks_metadata):
        strategy = ks_metadata.replication_strategy
        if strategy:
            return strategy.make_token_replica_map(self.token_to_host_owner, self.ring)
        else:
            return None

    def remove_keyspace(self, keyspace):
        self.tokens_to_hosts_by_ks.pop(keyspace, None)

    def get_replicas(self, keyspace, token):
        """
        Get  a set of :class:`.Host` instances representing all of the
        replica nodes for a given :class:`.Token`.
        """
        tokens_to_hosts = self.tokens_to_hosts_by_ks.get(keyspace, None)
        if tokens_to_hosts is None:
            self.rebuild_keyspace(keyspace, build_if_absent=True)
            tokens_to_hosts = self.tokens_to_hosts_by_ks.get(keyspace, None)

        if tokens_to_hosts:
            # The values in self.ring correspond to the end of the
            # token range up to and including the value listed.
            point = bisect_left(self.ring, token)
            if point == len(self.ring):
                return tokens_to_hosts[self.ring[0]]
            else:
                return tokens_to_hosts[self.ring[point]]
        return []


@total_ordering
class Token(object):
    """
    Abstract class representing a token.
    """

    def __init__(self, token):
        self.value = token

    @classmethod
    def hash_fn(cls, key):
        return key

    @classmethod
    def from_key(cls, key):
        return cls(cls.hash_fn(key))

    @classmethod
    def from_string(cls, token_string):
        raise NotImplementedError()

    def __eq__(self, other):
        return self.value == other.value

    def __lt__(self, other):
        return self.value < other.value

    def __hash__(self):
        return hash(self.value)

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.value)
    __str__ = __repr__


MIN_LONG = -(2 ** 63)
MAX_LONG = (2 ** 63) - 1


class NoMurmur3(Exception):
    pass


class HashToken(Token):

    @classmethod
    def from_string(cls, token_string):
        """ `token_string` should be the string representation from the server. """
        # The hash partitioners just store the deciman value
        return cls(int(token_string))


class Murmur3Token(HashToken):
    """
    A token for ``Murmur3Partitioner``.
    """

    @classmethod
    def hash_fn(cls, key):
        if murmur3 is not None:
            h = int(murmur3(key))
            return h if h != MIN_LONG else MAX_LONG
        else:
            raise NoMurmur3()

    def __init__(self, token):
        """ `token` is an int or string representing the token. """
        self.value = int(token)


class MD5Token(HashToken):
    """
    A token for ``RandomPartitioner``.
    """

    @classmethod
    def hash_fn(cls, key):
        if isinstance(key, str):
            key = key.encode('UTF-8')
        return abs(varint_unpack(md5(key).digest()))


class BytesToken(Token):
    """
    A token for ``ByteOrderedPartitioner``.
    """

    @classmethod
    def from_string(cls, token_string):
        """ `token_string` should be the string representation from the server. """
        # unhexlify works fine with unicode input in everythin but pypy3, where it Raises "TypeError: 'str' does not support the buffer interface"
        if isinstance(token_string, str):
            token_string = token_string.encode('ascii')
        # The BOP stores a hex string
        return cls(unhexlify(token_string))


class TriggerMetadata(object):
    """
    A representation of a trigger for a table.
    """

    table = None
    """ The :class:`.TableMetadata` this trigger belongs to. """

    name = None
    """ The string name of this trigger. """

    options = None
    """
    A dict mapping trigger option names to their specific settings for this
    table.
    """
    def __init__(self, table_metadata, trigger_name, options=None):
        self.table = table_metadata
        self.name = trigger_name
        self.options = options

    def as_cql_query(self):
        ret = "CREATE TRIGGER %s ON %s.%s USING %s" % (
            protect_name(self.name),
            protect_name(self.table.keyspace_name),
            protect_name(self.table.name),
            protect_value(self.options['class'])
        )
        return ret

    def export_as_string(self):
        return self.as_cql_query() + ';'


class _SchemaParser(object):

    def __init__(self, connection, timeout):
        self.connection = connection
        self.timeout = timeout

    def _handle_results(self, success, result, expected_failures=tuple()):
        """
        Given a bool and a ResultSet (the form returned per result from
        Connection.wait_for_responses), return a dictionary containing the
        results. Used to process results from asynchronous queries to system
        tables.

        ``expected_failures`` will usually be used to allow callers to ignore
        ``InvalidRequest`` errors caused by a missing system keyspace. For
        example, some DSE versions report a 4.X server version, but do not have
        virtual tables. Thus, running against 4.X servers, SchemaParserV4 uses
        expected_failures to make a best-effort attempt to read those
        keyspaces, but treat them as empty if they're not found.

        :param success: A boolean representing whether or not the query
        succeeded
        :param result: The resultset in question.
        :expected_failures: An Exception class or an iterable thereof. If the
        query failed, but raised an instance of an expected failure class, this
        will ignore the failure and return an empty list.
        """
        if not success and isinstance(result, expected_failures):
            return []
        elif success:
            return dict_factory(result.column_names, result.parsed_rows) if result else []
        else:
            raise result

    def _query_build_row(self, query_string, build_func):
        result = self._query_build_rows(query_string, build_func)
        return result[0] if result else None

    def _query_build_rows(self, query_string, build_func):
        query = QueryMessage(query=query_string, consistency_level=ConsistencyLevel.ONE)
        responses = self.connection.wait_for_responses((query), timeout=self.timeout, fail_on_error=False)
        (success, response) = responses[0]
        if success:
            result = dict_factory(response.column_names, response.parsed_rows)
            return [build_func(row) for row in result]
        elif isinstance(response, InvalidRequest):
            log.debug("user types table not found")
            return []
        else:
            raise response


class SchemaParserV22(_SchemaParser):
    """
    For C* 2.2+
    """
    _SELECT_KEYSPACES = "SELECT * FROM system.schema_keyspaces"
    _SELECT_COLUMN_FAMILIES = "SELECT * FROM system.schema_columnfamilies"
    _SELECT_COLUMNS = "SELECT * FROM system.schema_columns"
    _SELECT_TRIGGERS = "SELECT * FROM system.schema_triggers"
    _SELECT_TYPES = "SELECT * FROM system.schema_usertypes"
    _SELECT_FUNCTIONS = "SELECT * FROM system.schema_functions"
    _SELECT_AGGREGATES = "SELECT * FROM system.schema_aggregates"

    _table_name_col = 'columnfamily_name'

    _function_agg_arument_type_col = 'signature'

    recognized_table_options = (
        "comment",
        "read_repair_chance",
        "dclocal_read_repair_chance",  # kept to be safe, but see _build_table_options()
        "local_read_repair_chance",
        "replicate_on_write",
        "gc_grace_seconds",
        "bloom_filter_fp_chance",
        "caching",
        "compaction_strategy_class",
        "compaction_strategy_options",
        "min_compaction_threshold",
        "max_compaction_threshold",
        "compression_parameters",
        "min_index_interval",
        "max_index_interval",
        "index_interval",
        "speculative_retry",
        "rows_per_partition_to_cache",
        "memtable_flush_period_in_ms",
        "populate_io_cache_on_flush",
        "compression",
        "default_time_to_live")

    def __init__(self, connection, timeout):
        super(SchemaParserV22, self).__init__(connection, timeout)
        self.keyspaces_result = []
        self.tables_result = []
        self.columns_result = []
        self.triggers_result = []
        self.types_result = []
        self.functions_result = []
        self.aggregates_result = []

        self.keyspace_table_rows = defaultdict(list)
        self.keyspace_table_col_rows = defaultdict(lambda: defaultdict(list))
        self.keyspace_type_rows = defaultdict(list)
        self.keyspace_func_rows = defaultdict(list)
        self.keyspace_agg_rows = defaultdict(list)
        self.keyspace_table_trigger_rows = defaultdict(lambda: defaultdict(list))

    def get_all_keyspaces(self):
        self._query_all()

        for row in self.keyspaces_result:
            keyspace_meta = self._build_keyspace_metadata(row)

            try:
                for table_row in self.keyspace_table_rows.get(keyspace_meta.name, []):
                    table_meta = self._build_table_metadata(table_row)
                    keyspace_meta._add_table_metadata(table_meta)

                for usertype_row in self.keyspace_type_rows.get(keyspace_meta.name, []):
                    usertype = self._build_user_type(usertype_row)
                    keyspace_meta.user_types[usertype.name] = usertype

                for fn_row in self.keyspace_func_rows.get(keyspace_meta.name, []):
                    fn = self._build_function(fn_row)
                    keyspace_meta.functions[fn.signature] = fn

                for agg_row in self.keyspace_agg_rows.get(keyspace_meta.name, []):
                    agg = self._build_aggregate(agg_row)
                    keyspace_meta.aggregates[agg.signature] = agg
            except Exception:
                log.exception("Error while parsing metadata for keyspace %s. Metadata model will be incomplete.", keyspace_meta.name)
                keyspace_meta._exc_info = sys.exc_info()

            yield keyspace_meta

    def get_table(self, keyspaces, keyspace, table):
        cl = ConsistencyLevel.ONE
        where_clause = bind_params(" WHERE keyspace_name = %%s AND %s = %%s" % (self._table_name_col,), (keyspace, table), _encoder)
        cf_query = QueryMessage(query=self._SELECT_COLUMN_FAMILIES + where_clause, consistency_level=cl)
        col_query = QueryMessage(query=self._SELECT_COLUMNS + where_clause, consistency_level=cl)
        triggers_query = QueryMessage(query=self._SELECT_TRIGGERS + where_clause, consistency_level=cl)
        (cf_success, cf_result), (col_success, col_result), (triggers_success, triggers_result) \
            = self.connection.wait_for_responses(cf_query, col_query, triggers_query, timeout=self.timeout, fail_on_error=False)
        table_result = self._handle_results(cf_success, cf_result)
        col_result = self._handle_results(col_success, col_result)

        # the triggers table doesn't exist in C* 1.2
        triggers_result = self._handle_results(triggers_success, triggers_result,
                                               expected_failures=InvalidRequest)

        if table_result:
            return self._build_table_metadata(table_result[0], col_result, triggers_result)

    def get_type(self, keyspaces, keyspace, type):
        where_clause = bind_params(" WHERE keyspace_name = %s AND type_name = %s", (keyspace, type), _encoder)
        return self._query_build_row(self._SELECT_TYPES + where_clause, self._build_user_type)

    def get_types_map(self, keyspaces, keyspace):
        where_clause = bind_params(" WHERE keyspace_name = %s", (keyspace,), _encoder)
        types = self._query_build_rows(self._SELECT_TYPES + where_clause, self._build_user_type)
        return dict((t.name, t) for t in types)

    def get_function(self, keyspaces, keyspace, function):
        where_clause = bind_params(" WHERE keyspace_name = %%s AND function_name = %%s AND %s = %%s" % (self._function_agg_arument_type_col,),
                                   (keyspace, function.name, function.argument_types), _encoder)
        return self._query_build_row(self._SELECT_FUNCTIONS + where_clause, self._build_function)

    def get_aggregate(self, keyspaces, keyspace, aggregate):
        where_clause = bind_params(" WHERE keyspace_name = %%s AND aggregate_name = %%s AND %s = %%s" % (self._function_agg_arument_type_col,),
                                   (keyspace, aggregate.name, aggregate.argument_types), _encoder)

        return self._query_build_row(self._SELECT_AGGREGATES + where_clause, self._build_aggregate)

    def get_keyspace(self, keyspaces, keyspace):
        where_clause = bind_params(" WHERE keyspace_name = %s", (keyspace,), _encoder)
        return self._query_build_row(self._SELECT_KEYSPACES + where_clause, self._build_keyspace_metadata)

    @classmethod
    def _build_keyspace_metadata(cls, row):
        try:
            ksm = cls._build_keyspace_metadata_internal(row)
        except Exception:
            name = row["keyspace_name"]
            ksm = KeyspaceMetadata(name, False, 'UNKNOWN', {})
            ksm._exc_info = sys.exc_info()  # capture exc_info before log because nose (test) logging clears it in certain circumstances
            log.exception("Error while parsing metadata for keyspace %s row(%s)", name, row)
        return ksm

    @staticmethod
    def _build_keyspace_metadata_internal(row):
        name = row["keyspace_name"]
        durable_writes = row["durable_writes"]
        strategy_class = row["strategy_class"]
        strategy_options = json.loads(row["strategy_options"])
        return KeyspaceMetadata(name, durable_writes, strategy_class, strategy_options)

    @classmethod
    def _build_user_type(cls, usertype_row):
        field_types = list(map(cls._schema_type_to_cql, usertype_row['field_types']))
        return UserType(usertype_row['keyspace_name'], usertype_row['type_name'],
                        usertype_row['field_names'], field_types)

    @classmethod
    def _build_function(cls, function_row):
        return_type = cls._schema_type_to_cql(function_row['return_type'])
        deterministic = function_row.get('deterministic', False)
        monotonic = function_row.get('monotonic', False)
        monotonic_on = function_row.get('monotonic_on', ())
        return Function(function_row['keyspace_name'], function_row['function_name'],
                        function_row[cls._function_agg_arument_type_col], function_row['argument_names'],
                        return_type, function_row['language'], function_row['body'],
                        function_row['called_on_null_input'],
                        deterministic, monotonic, monotonic_on)

    @classmethod
    def _build_aggregate(cls, aggregate_row):
        cass_state_type = types.lookup_casstype(aggregate_row['state_type'])
        initial_condition = aggregate_row['initcond']
        if initial_condition is not None:
            initial_condition = _encoder.cql_encode_all_types(cass_state_type.deserialize(initial_condition, 3))
        state_type = _cql_from_cass_type(cass_state_type)
        return_type = cls._schema_type_to_cql(aggregate_row['return_type'])
        return Aggregate(aggregate_row['keyspace_name'], aggregate_row['aggregate_name'],
                         aggregate_row['signature'], aggregate_row['state_func'], state_type,
                         aggregate_row['final_func'], initial_condition, return_type,
                         aggregate_row.get('deterministic', False))

    def _build_table_metadata(self, row, col_rows=None, trigger_rows=None):
        keyspace_name = row["keyspace_name"]
        cfname = row[self._table_name_col]

        col_rows = col_rows or self.keyspace_table_col_rows[keyspace_name][cfname]
        trigger_rows = trigger_rows or self.keyspace_table_trigger_rows[keyspace_name][cfname]

        if not col_rows:  # CASSANDRA-8487
            log.warning("Building table metadata with no column meta for %s.%s",
                        keyspace_name, cfname)

        table_meta = TableMetadata(keyspace_name, cfname)

        try:
            comparator = types.lookup_casstype(row["comparator"])
            table_meta.comparator = comparator

            is_dct_comparator = issubclass(comparator, types.DynamicCompositeType)
            is_composite_comparator = issubclass(comparator, types.CompositeType)
            column_name_types = comparator.subtypes if is_composite_comparator else (comparator,)

            num_column_name_components = len(column_name_types)
            last_col = column_name_types[-1]

            column_aliases = row.get("column_aliases", None)

            clustering_rows = [r for r in col_rows
                               if r.get('type', None) == "clustering_key"]
            if len(clustering_rows) > 1:
                clustering_rows = sorted(clustering_rows, key=lambda row: row.get('component_index'))

            if column_aliases is not None:
                column_aliases = json.loads(column_aliases)

            if not column_aliases:  # json load failed or column_aliases empty PYTHON-562
                column_aliases = [r.get('column_name') for r in clustering_rows]

            if is_composite_comparator:
                if issubclass(last_col, types.ColumnToCollectionType):
                    # collections
                    is_compact = False
                    has_value = False
                    clustering_size = num_column_name_components - 2
                elif (len(column_aliases) == num_column_name_components - 1 and
                      issubclass(last_col, types.UTF8Type)):
                    # aliases?
                    is_compact = False
                    has_value = False
                    clustering_size = num_column_name_components - 1
                else:
                    # compact table
                    is_compact = True
                    has_value = column_aliases or not col_rows
                    clustering_size = num_column_name_components

                    # Some thrift tables define names in composite types (see PYTHON-192)
                    if not column_aliases and hasattr(comparator, 'fieldnames'):
                        column_aliases = filter(None, comparator.fieldnames)
            else:
                is_compact = True
                if column_aliases or not col_rows or is_dct_comparator:
                    has_value = True
                    clustering_size = num_column_name_components
                else:
                    has_value = False
                    clustering_size = 0

            # partition key
            partition_rows = [r for r in col_rows
                              if r.get('type', None) == "partition_key"]

            if len(partition_rows) > 1:
                partition_rows = sorted(partition_rows, key=lambda row: row.get('component_index'))

            key_aliases = row.get("key_aliases")
            if key_aliases is not None:
                key_aliases = json.loads(key_aliases) if key_aliases else []
            else:
                # In 2.0+, we can use the 'type' column. In 3.0+, we have to use it.
                key_aliases = [r.get('column_name') for r in partition_rows]

            key_validator = row.get("key_validator")
            if key_validator is not None:
                key_type = types.lookup_casstype(key_validator)
                key_types = key_type.subtypes if issubclass(key_type, types.CompositeType) else [key_type]
            else:
                key_types = [types.lookup_casstype(r.get('validator')) for r in partition_rows]

            for i, col_type in enumerate(key_types):
                if len(key_aliases) > i:
                    column_name = key_aliases[i]
                elif i == 0:
                    column_name = "key"
                else:
                    column_name = "key%d" % i

                col = ColumnMetadata(table_meta, column_name, col_type.cql_parameterized_type())
                table_meta.columns[column_name] = col
                table_meta.partition_key.append(col)

            # clustering key
            for i in range(clustering_size):
                if len(column_aliases) > i:
                    column_name = column_aliases[i]
                else:
                    column_name = "column%d" % (i + 1)

                data_type = column_name_types[i]
                cql_type = _cql_from_cass_type(data_type)
                is_reversed = types.is_reversed_casstype(data_type)
                col = ColumnMetadata(table_meta, column_name, cql_type, is_reversed=is_reversed)
                table_meta.columns[column_name] = col
                table_meta.clustering_key.append(col)

            # value alias (if present)
            if has_value:
                value_alias_rows = [r for r in col_rows
                                    if r.get('type', None) == "compact_value"]

                if not key_aliases:  # TODO are we checking the right thing here?
                    value_alias = "value"
                else:
                    value_alias = row.get("value_alias", None)
                    if value_alias is None and value_alias_rows:  # CASSANDRA-8487
                        # In 2.0+, we can use the 'type' column. In 3.0+, we have to use it.
                        value_alias = value_alias_rows[0].get('column_name')

                default_validator = row.get("default_validator")
                if default_validator:
                    validator = types.lookup_casstype(default_validator)
                else:
                    if value_alias_rows:  # CASSANDRA-8487
                        validator = types.lookup_casstype(value_alias_rows[0].get('validator'))

                cql_type = _cql_from_cass_type(validator)
                col = ColumnMetadata(table_meta, value_alias, cql_type)
                if value_alias:  # CASSANDRA-8487
                    table_meta.columns[value_alias] = col

            # other normal columns
            for col_row in col_rows:
                column_meta = self._build_column_metadata(table_meta, col_row)
                if column_meta.name is not None:
                    table_meta.columns[column_meta.name] = column_meta
                    index_meta = self._build_index_metadata(column_meta, col_row)
                    if index_meta:
                        table_meta.indexes[index_meta.name] = index_meta

            for trigger_row in trigger_rows:
                trigger_meta = self._build_trigger_metadata(table_meta, trigger_row)
                table_meta.triggers[trigger_meta.name] = trigger_meta

            table_meta.options = self._build_table_options(row)
            table_meta.is_compact_storage = is_compact
        except Exception:
            table_meta._exc_info = sys.exc_info()
            log.exception("Error while parsing metadata for table %s.%s row(%s) columns(%s)", keyspace_name, cfname, row, col_rows)

        return table_meta

    def _build_table_options(self, row):
        """ Setup the mostly-non-schema table options, like caching settings """
        options = dict((o, row.get(o)) for o in self.recognized_table_options if o in row)

        # the option name when creating tables is "dclocal_read_repair_chance",
        # but the column name in system.schema_columnfamilies is
        # "local_read_repair_chance".  We'll store this as dclocal_read_repair_chance,
        # since that's probably what users are expecting (and we need it for the
        # CREATE TABLE statement anyway).
        if "local_read_repair_chance" in options:
            val = options.pop("local_read_repair_chance")
            options["dclocal_read_repair_chance"] = val

        return options

    @classmethod
    def _build_column_metadata(cls, table_metadata, row):
        name = row["column_name"]
        type_string = row["validator"]
        data_type = types.lookup_casstype(type_string)
        cql_type = _cql_from_cass_type(data_type)
        is_static = row.get("type", None) == "static"
        is_reversed = types.is_reversed_casstype(data_type)
        column_meta = ColumnMetadata(table_metadata, name, cql_type, is_static, is_reversed)
        column_meta._cass_type = data_type
        return column_meta

    @staticmethod
    def _build_index_metadata(column_metadata, row):
        index_name = row.get("index_name")
        kind = row.get("index_type")
        if index_name or kind:
            options = row.get("index_options")
            options = json.loads(options) if options else {}
            options = options or {}  # if the json parsed to None, init empty dict

            # generate a CQL index identity string
            target = protect_name(column_metadata.name)
            if kind != "CUSTOM":
                if "index_keys" in options:
                    target = 'keys(%s)' % (target,)
                elif "index_values" in options:
                    # don't use any "function" for collection values
                    pass
                else:
                    # it might be a "full" index on a frozen collection, but
                    # we need to check the data type to verify that, because
                    # there is no special index option for full-collection
                    # indexes.
                    data_type = column_metadata._cass_type
                    collection_types = ('map', 'set', 'list')
                    if data_type.typename == "frozen" and data_type.subtypes[0].typename in collection_types:
                        # no index option for full-collection index
                        target = 'full(%s)' % (target,)
            options['target'] = target
            return IndexMetadata(column_metadata.table.keyspace_name, column_metadata.table.name, index_name, kind, options)

    @staticmethod
    def _build_trigger_metadata(table_metadata, row):
        name = row["trigger_name"]
        options = row["trigger_options"]
        trigger_meta = TriggerMetadata(table_metadata, name, options)
        return trigger_meta

    def _query_all(self):
        cl = ConsistencyLevel.ONE
        queries = [
            QueryMessage(query=self._SELECT_KEYSPACES, consistency_level=cl),
            QueryMessage(query=self._SELECT_COLUMN_FAMILIES, consistency_level=cl),
            QueryMessage(query=self._SELECT_COLUMNS, consistency_level=cl),
            QueryMessage(query=self._SELECT_TYPES, consistency_level=cl),
            QueryMessage(query=self._SELECT_FUNCTIONS, consistency_level=cl),
            QueryMessage(query=self._SELECT_AGGREGATES, consistency_level=cl),
            QueryMessage(query=self._SELECT_TRIGGERS, consistency_level=cl)
        ]

        ((ks_success, ks_result),
         (table_success, table_result),
         (col_success, col_result),
         (types_success, types_result),
         (functions_success, functions_result),
         (aggregates_success, aggregates_result),
         (triggers_success, triggers_result)) = (
             self.connection.wait_for_responses(*queries, timeout=self.timeout,
                                                fail_on_error=False)
        )

        self.keyspaces_result = self._handle_results(ks_success, ks_result)
        self.tables_result = self._handle_results(table_success, table_result)
        self.columns_result = self._handle_results(col_success, col_result)

        # if we're connected to Cassandra < 2.0, the triggers table will not exist
        if triggers_success:
            self.triggers_result = dict_factory(triggers_result.column_names, triggers_result.parsed_rows)
        else:
            if isinstance(triggers_result, InvalidRequest):
                log.debug("triggers table not found")
            elif isinstance(triggers_result, Unauthorized):
                log.warning("this version of Cassandra does not allow access to schema_triggers metadata with authorization enabled (CASSANDRA-7967); "
                            "The driver will operate normally, but will not reflect triggers in the local metadata model, or schema strings.")
            else:
                raise triggers_result

        # if we're connected to Cassandra < 2.1, the usertypes table will not exist
        if types_success:
            self.types_result = dict_factory(types_result.column_names, types_result.parsed_rows)
        else:
            if isinstance(types_result, InvalidRequest):
                log.debug("user types table not found")
                self.types_result = {}
            else:
                raise types_result

        # functions were introduced in Cassandra 2.2
        if functions_success:
            self.functions_result = dict_factory(functions_result.column_names, functions_result.parsed_rows)
        else:
            if isinstance(functions_result, InvalidRequest):
                log.debug("user functions table not found")
            else:
                raise functions_result

        # aggregates were introduced in Cassandra 2.2
        if aggregates_success:
            self.aggregates_result = dict_factory(aggregates_result.column_names, aggregates_result.parsed_rows)
        else:
            if isinstance(aggregates_result, InvalidRequest):
                log.debug("user aggregates table not found")
            else:
                raise aggregates_result

        self._aggregate_results()

    def _aggregate_results(self):
        m = self.keyspace_table_rows
        for row in self.tables_result:
            m[row["keyspace_name"]].append(row)

        m = self.keyspace_table_col_rows
        for row in self.columns_result:
            ksname = row["keyspace_name"]
            cfname = row[self._table_name_col]
            m[ksname][cfname].append(row)

        m = self.keyspace_type_rows
        for row in self.types_result:
            m[row["keyspace_name"]].append(row)

        m = self.keyspace_func_rows
        for row in self.functions_result:
            m[row["keyspace_name"]].append(row)

        m = self.keyspace_agg_rows
        for row in self.aggregates_result:
            m[row["keyspace_name"]].append(row)

        m = self.keyspace_table_trigger_rows
        for row in self.triggers_result:
            ksname = row["keyspace_name"]
            cfname = row[self._table_name_col]
            m[ksname][cfname].append(row)

    @staticmethod
    def _schema_type_to_cql(type_string):
        cass_type = types.lookup_casstype(type_string)
        return _cql_from_cass_type(cass_type)


class SchemaParserV3(SchemaParserV22):
    """
    For C* 3.0+
    """
    _SELECT_KEYSPACES = "SELECT * FROM system_schema.keyspaces"
    _SELECT_TABLES = "SELECT * FROM system_schema.tables"
    _SELECT_COLUMNS = "SELECT * FROM system_schema.columns"
    _SELECT_INDEXES = "SELECT * FROM system_schema.indexes"
    _SELECT_TRIGGERS = "SELECT * FROM system_schema.triggers"
    _SELECT_TYPES = "SELECT * FROM system_schema.types"
    _SELECT_FUNCTIONS = "SELECT * FROM system_schema.functions"
    _SELECT_AGGREGATES = "SELECT * FROM system_schema.aggregates"
    _SELECT_VIEWS = "SELECT * FROM system_schema.views"

    _table_name_col = 'table_name'

    _function_agg_arument_type_col = 'argument_types'

    _table_metadata_class = TableMetadataV3

    recognized_table_options = (
        'bloom_filter_fp_chance',
        'caching',
        'cdc',
        'comment',
        'compaction',
        'compression',
        'crc_check_chance',
        'dclocal_read_repair_chance',
        'default_time_to_live',
        'gc_grace_seconds',
        'max_index_interval',
        'memtable_flush_period_in_ms',
        'min_index_interval',
        'read_repair_chance',
        'speculative_retry')

    def __init__(self, connection, timeout):
        super(SchemaParserV3, self).__init__(connection, timeout)
        self.indexes_result = []
        self.keyspace_table_index_rows = defaultdict(lambda: defaultdict(list))
        self.keyspace_view_rows = defaultdict(list)

    def get_all_keyspaces(self):
        for keyspace_meta in super(SchemaParserV3, self).get_all_keyspaces():
            for row in self.keyspace_view_rows[keyspace_meta.name]:
                view_meta = self._build_view_metadata(row)
                keyspace_meta._add_view_metadata(view_meta)
            yield keyspace_meta

    def get_table(self, keyspaces, keyspace, table):
        cl = ConsistencyLevel.ONE
        where_clause = bind_params(" WHERE keyspace_name = %%s AND %s = %%s" % (self._table_name_col), (keyspace, table), _encoder)
        cf_query = QueryMessage(query=self._SELECT_TABLES + where_clause, consistency_level=cl)
        col_query = QueryMessage(query=self._SELECT_COLUMNS + where_clause, consistency_level=cl)
        indexes_query = QueryMessage(query=self._SELECT_INDEXES + where_clause, consistency_level=cl)
        triggers_query = QueryMessage(query=self._SELECT_TRIGGERS + where_clause, consistency_level=cl)

        # in protocol v4 we don't know if this event is a view or a table, so we look for both
        where_clause = bind_params(" WHERE keyspace_name = %s AND view_name = %s", (keyspace, table), _encoder)
        view_query = QueryMessage(query=self._SELECT_VIEWS + where_clause,
                                  consistency_level=cl)
        ((cf_success, cf_result), (col_success, col_result),
         (indexes_sucess, indexes_result), (triggers_success, triggers_result),
         (view_success, view_result)) = (
             self.connection.wait_for_responses(
                 cf_query, col_query, indexes_query, triggers_query,
                 view_query, timeout=self.timeout, fail_on_error=False)
        )
        table_result = self._handle_results(cf_success, cf_result)
        col_result = self._handle_results(col_success, col_result)
        if table_result:
            indexes_result = self._handle_results(indexes_sucess, indexes_result)
            triggers_result = self._handle_results(triggers_success, triggers_result)
            return self._build_table_metadata(table_result[0], col_result, triggers_result, indexes_result)

        view_result = self._handle_results(view_success, view_result)
        if view_result:
            return self._build_view_metadata(view_result[0], col_result)

    @staticmethod
    def _build_keyspace_metadata_internal(row):
        name = row["keyspace_name"]
        durable_writes = row["durable_writes"]
        strategy_options = dict(row["replication"])
        strategy_class = strategy_options.pop("class")
        return KeyspaceMetadata(name, durable_writes, strategy_class, strategy_options)

    @staticmethod
    def _build_aggregate(aggregate_row):
        return Aggregate(aggregate_row['keyspace_name'], aggregate_row['aggregate_name'],
                         aggregate_row['argument_types'], aggregate_row['state_func'], aggregate_row['state_type'],
                         aggregate_row['final_func'], aggregate_row['initcond'], aggregate_row['return_type'],
                         aggregate_row.get('deterministic', False))

    def _build_table_metadata(self, row, col_rows=None, trigger_rows=None, index_rows=None, virtual=False):
        keyspace_name = row["keyspace_name"]
        table_name = row[self._table_name_col]

        col_rows = col_rows or self.keyspace_table_col_rows[keyspace_name][table_name]
        trigger_rows = trigger_rows or self.keyspace_table_trigger_rows[keyspace_name][table_name]
        index_rows = index_rows or self.keyspace_table_index_rows[keyspace_name][table_name]

        table_meta = self._table_metadata_class(keyspace_name, table_name, virtual=virtual)
        try:
            table_meta.options = self._build_table_options(row)
            flags = row.get('flags', set())
            if flags:
                is_dense = 'dense' in flags
                compact_static = not is_dense and 'super' not in flags and 'compound' not in flags
                table_meta.is_compact_storage = is_dense or 'super' in flags or 'compound' not in flags
            elif virtual:
                compact_static = False
                table_meta.is_compact_storage = False
                is_dense = False
            else:
                compact_static = True
                table_meta.is_compact_storage = True
                is_dense = False

            self._build_table_columns(table_meta, col_rows, compact_static, is_dense, virtual)

            for trigger_row in trigger_rows:
                trigger_meta = self._build_trigger_metadata(table_meta, trigger_row)
                table_meta.triggers[trigger_meta.name] = trigger_meta

            for index_row in index_rows:
                index_meta = self._build_index_metadata(table_meta, index_row)
                if index_meta:
                    table_meta.indexes[index_meta.name] = index_meta

            table_meta.extensions = row.get('extensions', {})
        except Exception:
            table_meta._exc_info = sys.exc_info()
            log.exception("Error while parsing metadata for table %s.%s row(%s) columns(%s)", keyspace_name, table_name, row, col_rows)

        return table_meta

    def _build_table_options(self, row):
        """ Setup the mostly-non-schema table options, like caching settings """
        return dict((o, row.get(o)) for o in self.recognized_table_options if o in row)

    def _build_table_columns(self, meta, col_rows, compact_static=False, is_dense=False, virtual=False):
        # partition key
        partition_rows = [r for r in col_rows
                          if r.get('kind', None) == "partition_key"]
        if len(partition_rows) > 1:
            partition_rows = sorted(partition_rows, key=lambda row: row.get('position'))
        for r in partition_rows:
            # we have to add meta here (and not in the later loop) because TableMetadata.columns is an
            # OrderedDict, and it assumes keys are inserted first, in order, when exporting CQL
            column_meta = self._build_column_metadata(meta, r)
            meta.columns[column_meta.name] = column_meta
            meta.partition_key.append(meta.columns[r.get('column_name')])

        # clustering key
        if not compact_static:
            clustering_rows = [r for r in col_rows
                               if r.get('kind', None) == "clustering"]
            if len(clustering_rows) > 1:
                clustering_rows = sorted(clustering_rows, key=lambda row: row.get('position'))
            for r in clustering_rows:
                column_meta = self._build_column_metadata(meta, r)
                meta.columns[column_meta.name] = column_meta
                meta.clustering_key.append(meta.columns[r.get('column_name')])

        for col_row in (r for r in col_rows
                        if r.get('kind', None) not in ('partition_key', 'clustering_key')):
            column_meta = self._build_column_metadata(meta, col_row)
            if is_dense and column_meta.cql_type == types.cql_empty_type:
                continue
            if compact_static and not column_meta.is_static:
                # for compact static tables, we omit the clustering key and value, and only add the logical columns.
                # They are marked not static so that it generates appropriate CQL
                continue
            if compact_static:
                column_meta.is_static = False
            meta.columns[column_meta.name] = column_meta

    def _build_view_metadata(self, row, col_rows=None):
        keyspace_name = row["keyspace_name"]
        view_name = row["view_name"]
        base_table_name = row["base_table_name"]
        include_all_columns = row["include_all_columns"]
        where_clause = row["where_clause"]
        col_rows = col_rows or self.keyspace_table_col_rows[keyspace_name][view_name]
        view_meta = MaterializedViewMetadata(keyspace_name, view_name, base_table_name,
                                             include_all_columns, where_clause, self._build_table_options(row))
        self._build_table_columns(view_meta, col_rows)
        view_meta.extensions = row.get('extensions', {})

        return view_meta

    @staticmethod
    def _build_column_metadata(table_metadata, row):
        name = row["column_name"]
        cql_type = row["type"]
        is_static = row.get("kind", None) == "static"
        is_reversed = row["clustering_order"].upper() == "DESC"
        column_meta = ColumnMetadata(table_metadata, name, cql_type, is_static, is_reversed)
        return column_meta

    @staticmethod
    def _build_index_metadata(table_metadata, row):
        index_name = row.get("index_name")
        kind = row.get("kind")
        if index_name or kind:
            index_options = row.get("options")
            return IndexMetadata(table_metadata.keyspace_name, table_metadata.name, index_name, kind, index_options)
        else:
            return None

    @staticmethod
    def _build_trigger_metadata(table_metadata, row):
        name = row["trigger_name"]
        options = row["options"]
        trigger_meta = TriggerMetadata(table_metadata, name, options)
        return trigger_meta

    def _query_all(self):
        cl = ConsistencyLevel.ONE
        queries = [
            QueryMessage(query=self._SELECT_KEYSPACES, consistency_level=cl),
            QueryMessage(query=self._SELECT_TABLES, consistency_level=cl),
            QueryMessage(query=self._SELECT_COLUMNS, consistency_level=cl),
            QueryMessage(query=self._SELECT_TYPES, consistency_level=cl),
            QueryMessage(query=self._SELECT_FUNCTIONS, consistency_level=cl),
            QueryMessage(query=self._SELECT_AGGREGATES, consistency_level=cl),
            QueryMessage(query=self._SELECT_TRIGGERS, consistency_level=cl),
            QueryMessage(query=self._SELECT_INDEXES, consistency_level=cl),
            QueryMessage(query=self._SELECT_VIEWS, consistency_level=cl)
        ]

        ((ks_success, ks_result),
         (table_success, table_result),
         (col_success, col_result),
         (types_success, types_result),
         (functions_success, functions_result),
         (aggregates_success, aggregates_result),
         (triggers_success, triggers_result),
         (indexes_success, indexes_result),
         (views_success, views_result)) = self.connection.wait_for_responses(
             *queries, timeout=self.timeout, fail_on_error=False
        )

        self.keyspaces_result = self._handle_results(ks_success, ks_result)
        self.tables_result = self._handle_results(table_success, table_result)
        self.columns_result = self._handle_results(col_success, col_result)
        self.triggers_result = self._handle_results(triggers_success, triggers_result)
        self.types_result = self._handle_results(types_success, types_result)
        self.functions_result = self._handle_results(functions_success, functions_result)
        self.aggregates_result = self._handle_results(aggregates_success, aggregates_result)
        self.indexes_result = self._handle_results(indexes_success, indexes_result)
        self.views_result = self._handle_results(views_success, views_result)

        self._aggregate_results()

    def _aggregate_results(self):
        super(SchemaParserV3, self)._aggregate_results()

        m = self.keyspace_table_index_rows
        for row in self.indexes_result:
            ksname = row["keyspace_name"]
            cfname = row[self._table_name_col]
            m[ksname][cfname].append(row)

        m = self.keyspace_view_rows
        for row in self.views_result:
            m[row["keyspace_name"]].append(row)

    @staticmethod
    def _schema_type_to_cql(type_string):
        return type_string


class SchemaParserDSE60(SchemaParserV3):
    """
    For DSE 6.0+
    """
    recognized_table_options = (SchemaParserV3.recognized_table_options +
                                ("nodesync",))


class SchemaParserV4(SchemaParserV3):

    recognized_table_options = (
        'additional_write_policy',
        'bloom_filter_fp_chance',
        'caching',
        'cdc',
        'comment',
        'compaction',
        'compression',
        'crc_check_chance',
        'default_time_to_live',
        'gc_grace_seconds',
        'max_index_interval',
        'memtable_flush_period_in_ms',
        'min_index_interval',
        'read_repair',
        'speculative_retry')

    _SELECT_VIRTUAL_KEYSPACES = 'SELECT * from system_virtual_schema.keyspaces'
    _SELECT_VIRTUAL_TABLES = 'SELECT * from system_virtual_schema.tables'
    _SELECT_VIRTUAL_COLUMNS = 'SELECT * from system_virtual_schema.columns'

    def __init__(self, connection, timeout):
        super(SchemaParserV4, self).__init__(connection, timeout)
        self.virtual_keyspaces_rows = defaultdict(list)
        self.virtual_tables_rows = defaultdict(list)
        self.virtual_columns_rows = defaultdict(lambda: defaultdict(list))

    def _query_all(self):
        cl = ConsistencyLevel.ONE
        # todo: this duplicates V3; we should find a way for _query_all methods
        # to extend each other.
        queries = [
            # copied from V3
            QueryMessage(query=self._SELECT_KEYSPACES, consistency_level=cl),
            QueryMessage(query=self._SELECT_TABLES, consistency_level=cl),
            QueryMessage(query=self._SELECT_COLUMNS, consistency_level=cl),
            QueryMessage(query=self._SELECT_TYPES, consistency_level=cl),
            QueryMessage(query=self._SELECT_FUNCTIONS, consistency_level=cl),
            QueryMessage(query=self._SELECT_AGGREGATES, consistency_level=cl),
            QueryMessage(query=self._SELECT_TRIGGERS, consistency_level=cl),
            QueryMessage(query=self._SELECT_INDEXES, consistency_level=cl),
            QueryMessage(query=self._SELECT_VIEWS, consistency_level=cl),
            # V4-only queries
            QueryMessage(query=self._SELECT_VIRTUAL_KEYSPACES, consistency_level=cl),
            QueryMessage(query=self._SELECT_VIRTUAL_TABLES, consistency_level=cl),
            QueryMessage(query=self._SELECT_VIRTUAL_COLUMNS, consistency_level=cl)
        ]

        responses = self.connection.wait_for_responses(
            *queries, timeout=self.timeout, fail_on_error=False)
        (
            # copied from V3
            (ks_success, ks_result),
            (table_success, table_result),
            (col_success, col_result),
            (types_success, types_result),
            (functions_success, functions_result),
            (aggregates_success, aggregates_result),
            (triggers_success, triggers_result),
            (indexes_success, indexes_result),
            (views_success, views_result),
            # V4-only responses
            (virtual_ks_success, virtual_ks_result),
            (virtual_table_success, virtual_table_result),
            (virtual_column_success, virtual_column_result)
        ) = responses

        # copied from V3
        self.keyspaces_result = self._handle_results(ks_success, ks_result)
        self.tables_result = self._handle_results(table_success, table_result)
        self.columns_result = self._handle_results(col_success, col_result)
        self.triggers_result = self._handle_results(triggers_success, triggers_result)
        self.types_result = self._handle_results(types_success, types_result)
        self.functions_result = self._handle_results(functions_success, functions_result)
        self.aggregates_result = self._handle_results(aggregates_success, aggregates_result)
        self.indexes_result = self._handle_results(indexes_success, indexes_result)
        self.views_result = self._handle_results(views_success, views_result)
        # V4-only results
        # These tables don't exist in some DSE versions reporting 4.X so we can
        # ignore them if we got an error
        self.virtual_keyspaces_result = self._handle_results(
            virtual_ks_success, virtual_ks_result,
            expected_failures=(InvalidRequest,)
        )
        self.virtual_tables_result = self._handle_results(
            virtual_table_success, virtual_table_result,
            expected_failures=(InvalidRequest,)
        )
        self.virtual_columns_result = self._handle_results(
            virtual_column_success, virtual_column_result,
            expected_failures=(InvalidRequest,)
        )

        self._aggregate_results()

    def _aggregate_results(self):
        super(SchemaParserV4, self)._aggregate_results()

        m = self.virtual_tables_rows
        for row in self.virtual_tables_result:
            m[row["keyspace_name"]].append(row)

        m = self.virtual_columns_rows
        for row in self.virtual_columns_result:
            ks_name = row['keyspace_name']
            tab_name = row[self._table_name_col]
            m[ks_name][tab_name].append(row)

    def get_all_keyspaces(self):
        for x in super(SchemaParserV4, self).get_all_keyspaces():
            yield x

        for row in self.virtual_keyspaces_result:
            ks_name = row['keyspace_name']
            keyspace_meta = self._build_keyspace_metadata(row)
            keyspace_meta.virtual = True

            for table_row in self.virtual_tables_rows.get(ks_name, []):
                table_name = table_row[self._table_name_col]

                col_rows = self.virtual_columns_rows[ks_name][table_name]
                keyspace_meta._add_table_metadata(
                    self._build_table_metadata(table_row,
                                               col_rows=col_rows,
                                               virtual=True)
                )
            yield keyspace_meta

    @staticmethod
    def _build_keyspace_metadata_internal(row):
        # necessary fields that aren't int virtual ks
        row["durable_writes"] = row.get("durable_writes", None)
        row["replication"] = row.get("replication", {})
        row["replication"]["class"] = row["replication"].get("class", None)
        return super(SchemaParserV4, SchemaParserV4)._build_keyspace_metadata_internal(row)


class SchemaParserDSE67(SchemaParserV4):
    """
    For DSE 6.7+
    """
    recognized_table_options = (SchemaParserV4.recognized_table_options +
                                ("nodesync",))


class SchemaParserDSE68(SchemaParserDSE67):
    """
    For DSE 6.8+
    """

    _SELECT_VERTICES = "SELECT * FROM system_schema.vertices"
    _SELECT_EDGES = "SELECT * FROM system_schema.edges"

    _table_metadata_class = TableMetadataDSE68

    def __init__(self, connection, timeout):
        super(SchemaParserDSE68, self).__init__(connection, timeout)
        self.keyspace_table_vertex_rows = defaultdict(lambda: defaultdict(list))
        self.keyspace_table_edge_rows = defaultdict(lambda: defaultdict(list))

    def get_all_keyspaces(self):
        for keyspace_meta in super(SchemaParserDSE68, self).get_all_keyspaces():
            self._build_graph_metadata(keyspace_meta)
            yield keyspace_meta

    def get_table(self, keyspaces, keyspace, table):
        table_meta = super(SchemaParserDSE68, self).get_table(keyspaces, keyspace, table)
        cl = ConsistencyLevel.ONE
        where_clause = bind_params(" WHERE keyspace_name = %%s AND %s = %%s" % (self._table_name_col), (keyspace, table), _encoder)
        vertices_query = QueryMessage(query=self._SELECT_VERTICES + where_clause, consistency_level=cl)
        edges_query = QueryMessage(query=self._SELECT_EDGES + where_clause, consistency_level=cl)

        (vertices_success, vertices_result), (edges_success, edges_result) \
            = self.connection.wait_for_responses(vertices_query, edges_query, timeout=self.timeout, fail_on_error=False)
        vertices_result = self._handle_results(vertices_success, vertices_result)
        edges_result = self._handle_results(edges_success, edges_result)

        try:
            if vertices_result:
                table_meta.vertex = self._build_table_vertex_metadata(vertices_result[0])
            elif edges_result:
                table_meta.edge = self._build_table_edge_metadata(keyspaces[keyspace], edges_result[0])
        except Exception:
            table_meta.vertex = None
            table_meta.edge = None
            table_meta._exc_info = sys.exc_info()
            log.exception("Error while parsing graph metadata for table %s.%s.", keyspace, table)

        return table_meta

    @staticmethod
    def _build_keyspace_metadata_internal(row):
        name = row["keyspace_name"]
        durable_writes = row.get("durable_writes", None)
        replication = dict(row.get("replication")) if 'replication' in row else {}
        replication_class = replication.pop("class") if 'class' in replication else None
        graph_engine = row.get("graph_engine", None)
        return KeyspaceMetadata(name, durable_writes, replication_class, replication, graph_engine)

    def _build_graph_metadata(self, keyspace_meta):

        def _build_table_graph_metadata(table_meta):
            for row in self.keyspace_table_vertex_rows[keyspace_meta.name][table_meta.name]:
                table_meta.vertex = self._build_table_vertex_metadata(row)

            for row in self.keyspace_table_edge_rows[keyspace_meta.name][table_meta.name]:
                table_meta.edge = self._build_table_edge_metadata(keyspace_meta, row)

        try:
            # Make sure we process vertices before edges
            for table_meta in [t for t in keyspace_meta.tables.values()
                               if t.name in self.keyspace_table_vertex_rows[keyspace_meta.name]]:
                _build_table_graph_metadata(table_meta)

            # all other tables...
            for table_meta in [t for t in keyspace_meta.tables.values()
                               if t.name not in self.keyspace_table_vertex_rows[keyspace_meta.name]]:
                _build_table_graph_metadata(table_meta)
        except Exception:
            # schema error, remove all graph metadata for this keyspace
            for t in keyspace_meta.tables.values():
                t.edge = t.vertex = None
            keyspace_meta._exc_info = sys.exc_info()
            log.exception("Error while parsing graph metadata for keyspace %s", keyspace_meta.name)

    @staticmethod
    def _build_table_vertex_metadata(row):
        return VertexMetadata(row.get("keyspace_name"), row.get("table_name"),
                              row.get("label_name"))

    @staticmethod
    def _build_table_edge_metadata(keyspace_meta, row):
        from_table = row.get("from_table")
        from_table_meta = keyspace_meta.tables.get(from_table)
        from_label = from_table_meta.vertex.label_name
        to_table = row.get("to_table")
        to_table_meta = keyspace_meta.tables.get(to_table)
        to_label = to_table_meta.vertex.label_name

        return EdgeMetadata(
            row.get("keyspace_name"), row.get("table_name"),
            row.get("label_name"), from_table, from_label,
            row.get("from_partition_key_columns"),
            row.get("from_clustering_columns"), to_table, to_label,
            row.get("to_partition_key_columns"),
            row.get("to_clustering_columns"))

    def _query_all(self):
        cl = ConsistencyLevel.ONE
        queries = [
            # copied from v4
            QueryMessage(query=self._SELECT_KEYSPACES, consistency_level=cl),
            QueryMessage(query=self._SELECT_TABLES, consistency_level=cl),
            QueryMessage(query=self._SELECT_COLUMNS, consistency_level=cl),
            QueryMessage(query=self._SELECT_TYPES, consistency_level=cl),
            QueryMessage(query=self._SELECT_FUNCTIONS, consistency_level=cl),
            QueryMessage(query=self._SELECT_AGGREGATES, consistency_level=cl),
            QueryMessage(query=self._SELECT_TRIGGERS, consistency_level=cl),
            QueryMessage(query=self._SELECT_INDEXES, consistency_level=cl),
            QueryMessage(query=self._SELECT_VIEWS, consistency_level=cl),
            QueryMessage(query=self._SELECT_VIRTUAL_KEYSPACES, consistency_level=cl),
            QueryMessage(query=self._SELECT_VIRTUAL_TABLES, consistency_level=cl),
            QueryMessage(query=self._SELECT_VIRTUAL_COLUMNS, consistency_level=cl),
            # dse6.8 only
            QueryMessage(query=self._SELECT_VERTICES, consistency_level=cl),
            QueryMessage(query=self._SELECT_EDGES, consistency_level=cl)
        ]

        responses = self.connection.wait_for_responses(
            *queries, timeout=self.timeout, fail_on_error=False)
        (
            # copied from V4
            (ks_success, ks_result),
            (table_success, table_result),
            (col_success, col_result),
            (types_success, types_result),
            (functions_success, functions_result),
            (aggregates_success, aggregates_result),
            (triggers_success, triggers_result),
            (indexes_success, indexes_result),
            (views_success, views_result),
            (virtual_ks_success, virtual_ks_result),
            (virtual_table_success, virtual_table_result),
            (virtual_column_success, virtual_column_result),
            # dse6.8 responses
            (vertices_success, vertices_result),
            (edges_success, edges_result)
        ) = responses

        # copied from V4
        self.keyspaces_result = self._handle_results(ks_success, ks_result)
        self.tables_result = self._handle_results(table_success, table_result)
        self.columns_result = self._handle_results(col_success, col_result)
        self.triggers_result = self._handle_results(triggers_success, triggers_result)
        self.types_result = self._handle_results(types_success, types_result)
        self.functions_result = self._handle_results(functions_success, functions_result)
        self.aggregates_result = self._handle_results(aggregates_success, aggregates_result)
        self.indexes_result = self._handle_results(indexes_success, indexes_result)
        self.views_result = self._handle_results(views_success, views_result)

        # These tables don't exist in some DSE versions reporting 4.X so we can
        # ignore them if we got an error
        self.virtual_keyspaces_result = self._handle_results(
            virtual_ks_success, virtual_ks_result,
            expected_failures=(InvalidRequest,)
        )
        self.virtual_tables_result = self._handle_results(
            virtual_table_success, virtual_table_result,
            expected_failures=(InvalidRequest,)
        )
        self.virtual_columns_result = self._handle_results(
            virtual_column_success, virtual_column_result,
            expected_failures=(InvalidRequest,)
        )

        # dse6.8-only results
        self.vertices_result = self._handle_results(vertices_success, vertices_result)
        self.edges_result = self._handle_results(edges_success, edges_result)

        self._aggregate_results()

    def _aggregate_results(self):
        super(SchemaParserDSE68, self)._aggregate_results()

        m = self.keyspace_table_vertex_rows
        for row in self.vertices_result:
            ksname = row["keyspace_name"]
            cfname = row['table_name']
            m[ksname][cfname].append(row)

        m = self.keyspace_table_edge_rows
        for row in self.edges_result:
            ksname = row["keyspace_name"]
            cfname = row['table_name']
            m[ksname][cfname].append(row)


class MaterializedViewMetadata(object):
    """
    A representation of a materialized view on a table
    """

    keyspace_name = None
    """ A string name of the keyspace of this view."""

    name = None
    """ A string name of the view."""

    base_table_name = None
    """ A string name of the base table for this view."""

    partition_key = None
    """
    A list of :class:`.ColumnMetadata` instances representing the columns in
    the partition key for this view.  This will always hold at least one
    column.
    """

    clustering_key = None
    """
    A list of :class:`.ColumnMetadata` instances representing the columns
    in the clustering key for this view.

    Note that a table may have no clustering keys, in which case this will
    be an empty list.
    """

    columns = None
    """
    A dict mapping column names to :class:`.ColumnMetadata` instances.
    """

    include_all_columns = None
    """ A flag indicating whether the view was created AS SELECT * """

    where_clause = None
    """ String WHERE clause for the view select statement. From server metadata """

    options = None
    """
    A dict mapping table option names to their specific settings for this
    view.
    """

    extensions = None
    """
    Metadata describing configuration for table extensions
    """

    def __init__(self, keyspace_name, view_name, base_table_name, include_all_columns, where_clause, options):
        self.keyspace_name = keyspace_name
        self.name = view_name
        self.base_table_name = base_table_name
        self.partition_key = []
        self.clustering_key = []
        self.columns = OrderedDict()
        self.include_all_columns = include_all_columns
        self.where_clause = where_clause
        self.options = options or {}

    def as_cql_query(self, formatted=False):
        """
        Returns a CQL query that can be used to recreate this function.
        If `formatted` is set to :const:`True`, extra whitespace will
        be added to make the query more readable.
        """
        sep = '\n    ' if formatted else ' '
        keyspace = protect_name(self.keyspace_name)
        name = protect_name(self.name)

        selected_cols = '*' if self.include_all_columns else ', '.join(protect_name(col.name) for col in self.columns.values())
        base_table = protect_name(self.base_table_name)
        where_clause = self.where_clause

        part_key = ', '.join(protect_name(col.name) for col in self.partition_key)
        if len(self.partition_key) > 1:
            pk = "((%s)" % part_key
        else:
            pk = "(%s" % part_key
        if self.clustering_key:
            pk += ", %s" % ', '.join(protect_name(col.name) for col in self.clustering_key)
        pk += ")"

        properties = TableMetadataV3._property_string(formatted, self.clustering_key, self.options)

        ret = ("CREATE MATERIALIZED VIEW %(keyspace)s.%(name)s AS%(sep)s"
               "SELECT %(selected_cols)s%(sep)s"
               "FROM %(keyspace)s.%(base_table)s%(sep)s"
               "WHERE %(where_clause)s%(sep)s"
               "PRIMARY KEY %(pk)s%(sep)s"
               "WITH %(properties)s") % locals()

        if self.extensions:
            registry = _RegisteredExtensionType._extension_registry
            for k in registry.keys() & self.extensions:  # no viewkeys on OrderedMapSerializeKey
                ext = registry[k]
                cql = ext.after_table_cql(self, k, self.extensions[k])
                if cql:
                    ret += "\n\n%s" % (cql,)
        return ret

    def export_as_string(self):
        return self.as_cql_query(formatted=True) + ";"


class VertexMetadata(object):
    """
    A representation of a vertex on a table
    """

    keyspace_name = None
    """ A string name of the keyspace. """

    table_name = None
    """ A string name of the table this vertex is on. """

    label_name = None
    """ A string name of the label of this vertex."""

    def __init__(self, keyspace_name, table_name, label_name):
        self.keyspace_name = keyspace_name
        self.table_name = table_name
        self.label_name = label_name


class EdgeMetadata(object):
    """
    A representation of an edge on a table
    """

    keyspace_name = None
    """A string name of the keyspace """

    table_name = None
    """A string name of the table this edge is on"""

    label_name = None
    """A string name of the label of this edge"""

    from_table = None
    """A string name of the from table of this edge (incoming vertex)"""

    from_label = None
    """A string name of the from table label of this edge (incoming vertex)"""

    from_partition_key_columns = None
    """The columns that match the partition key of the incoming vertex table."""

    from_clustering_columns = None
    """The columns that match the clustering columns of the incoming vertex table."""

    to_table = None
    """A string name of the to table of this edge (outgoing vertex)"""

    to_label = None
    """A string name of the to table label of this edge (outgoing vertex)"""

    to_partition_key_columns = None
    """The columns that match the partition key of the outgoing vertex table."""

    to_clustering_columns = None
    """The columns that match the clustering columns of the outgoing vertex table."""

    def __init__(
            self, keyspace_name, table_name, label_name, from_table,
            from_label, from_partition_key_columns, from_clustering_columns,
            to_table, to_label, to_partition_key_columns,
            to_clustering_columns):
        self.keyspace_name = keyspace_name
        self.table_name = table_name
        self.label_name = label_name
        self.from_table = from_table
        self.from_label = from_label
        self.from_partition_key_columns = from_partition_key_columns
        self.from_clustering_columns = from_clustering_columns
        self.to_table = to_table
        self.to_label = to_label
        self.to_partition_key_columns = to_partition_key_columns
        self.to_clustering_columns = to_clustering_columns


def get_schema_parser(connection, server_version, dse_version, timeout):
    version = Version(server_version)
    if dse_version:
        v = Version(dse_version)
        if v >= Version('6.8.0'):
            return SchemaParserDSE68(connection, timeout)
        elif v >= Version('6.7.0'):
            return SchemaParserDSE67(connection, timeout)
        elif v >= Version('6.0.0'):
            return SchemaParserDSE60(connection, timeout)

    if version >= Version('4-a'):
        return SchemaParserV4(connection, timeout)
    elif version >= Version('3.0.0'):
        return SchemaParserV3(connection, timeout)
    else:
        # we could further specialize by version. Right now just refactoring the
        # multi-version parser we have as of C* 2.2.0rc1.
        return SchemaParserV22(connection, timeout)


def _cql_from_cass_type(cass_type):
    """
    A string representation of the type for this column, such as "varchar"
    or "map<string, int>".
    """
    if issubclass(cass_type, types.ReversedType):
        return cass_type.subtypes[0].cql_parameterized_type()
    else:
        return cass_type.cql_parameterized_type()


class RLACTableExtension(RegisteredTableExtension):
    name = "DSE_RLACA"

    @classmethod
    def after_table_cql(cls, table_meta, ext_key, ext_blob):
        return "RESTRICT ROWS ON %s.%s USING %s;" % (protect_name(table_meta.keyspace_name),
                                                     protect_name(table_meta.name),
                                                     protect_name(ext_blob.decode('utf-8')))
NO_VALID_REPLICA = object()


def group_keys_by_replica(session, keyspace, table, keys):
    """
    Returns a :class:`dict` with the keys grouped per host. This can be
    used to more accurately group by IN clause or to batch the keys per host.

    If a valid replica is not found for a particular key it will be grouped under
    :class:`~.NO_VALID_REPLICA`

    Example usage::
        result = group_keys_by_replica(
                     session, "system", "peers",
                     (("127.0.0.1", ), ("127.0.0.2", ))
                 )
    """
    cluster = session.cluster

    partition_keys = cluster.metadata.keyspaces[keyspace].tables[table].partition_key

    serializers = list(types._cqltypes[partition_key.cql_type] for partition_key in partition_keys)
    keys_per_host = defaultdict(list)
    distance = cluster._default_load_balancing_policy.distance

    for key in keys:
        serialized_key = [serializer.serialize(pk, cluster.protocol_version)
                          for serializer, pk in zip(serializers, key)]
        if len(serialized_key) == 1:
            routing_key = serialized_key[0]
        else:
            routing_key = b"".join(struct.pack(">H%dsB" % len(p), len(p), p, 0) for p in serialized_key)
        all_replicas = cluster.metadata.get_replicas(keyspace, routing_key)
        # First check if there are local replicas
        valid_replicas = [host for host in all_replicas if
                          host.is_up and distance(host) == HostDistance.LOCAL]
        if not valid_replicas:
            valid_replicas = [host for host in all_replicas if host.is_up]

        if valid_replicas:
            keys_per_host[random.choice(valid_replicas)].append(key)
        else:
            # We will group under this statement all the keys for which
            # we haven't found a valid replica
            keys_per_host[NO_VALID_REPLICA].append(key)

    return dict(keys_per_host)


# TODO next major reorg
class _NodeInfo(object):
    """
    Internal utility functions to determine the different host addresses/ports
    from a local or peers row.
    """

    @staticmethod
    def get_broadcast_rpc_address(row):
        # TODO next major, change the parsing logic to avoid any
        #  overriding of a non-null value
        addr = row.get("rpc_address")
        if "native_address" in row:
            addr = row.get("native_address")
        if "native_transport_address" in row:
            addr = row.get("native_transport_address")
        if not addr or addr in ["0.0.0.0", "::"]:
            addr = row.get("peer")

        return addr

    @staticmethod
    def get_broadcast_rpc_port(row):
        port = row.get("rpc_port")
        if port is None or port == 0:
            port = row.get("native_port")

        return port if port and port > 0 else None

    @staticmethod
    def get_broadcast_address(row):
        addr = row.get("broadcast_address")
        if addr is None:
            addr = row.get("peer")

        return addr

    @staticmethod
    def get_broadcast_port(row):
        port = row.get("broadcast_port")
        if port is None or port == 0:
            port = row.get("peer_port")

        return port if port and port > 0 else None
