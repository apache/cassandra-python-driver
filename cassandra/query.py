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
This module holds classes for working with prepared statements and
specifying consistency levels and retry policies for individual
queries.
"""

from collections import namedtuple
from datetime import datetime, timedelta, timezone
import re
import struct
import time
import warnings

from cassandra import ConsistencyLevel, OperationTimedOut
from cassandra.util import unix_time_from_uuid1
from cassandra.encoder import Encoder
import cassandra.encoder
from cassandra.policies import ColDesc
from cassandra.protocol import _UNSET_VALUE
from cassandra.util import OrderedDict, _sanitize_identifiers

import logging
log = logging.getLogger(__name__)

UNSET_VALUE = _UNSET_VALUE
"""
Specifies an unset value when binding a prepared statement.

Unset values are ignored, allowing prepared statements to be used without specify

See https://issues.apache.org/jira/browse/CASSANDRA-7304 for further details on semantics.

.. versionadded:: 2.6.0

Only valid when using native protocol v4+
"""

NON_ALPHA_REGEX = re.compile('[^a-zA-Z0-9]')
START_BADCHAR_REGEX = re.compile('^[^a-zA-Z0-9]*')
END_BADCHAR_REGEX = re.compile('[^a-zA-Z0-9_]*$')

_clean_name_cache = {}


def _clean_column_name(name):
    try:
        return _clean_name_cache[name]
    except KeyError:
        clean = NON_ALPHA_REGEX.sub("_", START_BADCHAR_REGEX.sub("", END_BADCHAR_REGEX.sub("", name)))
        _clean_name_cache[name] = clean
        return clean


def tuple_factory(colnames, rows):
    """
    Returns each row as a tuple

    Example::

        >>> from cassandra.query import tuple_factory
        >>> session = cluster.connect('mykeyspace')
        >>> session.row_factory = tuple_factory
        >>> rows = session.execute("SELECT name, age FROM users LIMIT 1")
        >>> print(rows[0])
        ('Bob', 42)

    .. versionchanged:: 2.0.0
        moved from ``cassandra.decoder`` to ``cassandra.query``
    """
    return rows

class PseudoNamedTupleRow(object):
    """
    Helper class for pseudo_named_tuple_factory. These objects provide an
    __iter__ interface, as well as index- and attribute-based access to values,
    but otherwise do not attempt to implement the full namedtuple or iterable
    interface.
    """
    def __init__(self, ordered_dict):
        self._dict = ordered_dict
        self._tuple = tuple(ordered_dict.values())

    def __getattr__(self, name):
        return self._dict[name]

    def __getitem__(self, idx):
        return self._tuple[idx]

    def __iter__(self):
        return iter(self._tuple)

    def __repr__(self):
        return '{t}({od})'.format(t=self.__class__.__name__,
                                  od=self._dict)


def pseudo_namedtuple_factory(colnames, rows):
    """
    Returns each row as a :class:`.PseudoNamedTupleRow`. This is the fallback
    factory for cases where :meth:`.named_tuple_factory` fails to create rows.
    """
    return [PseudoNamedTupleRow(od)
            for od in ordered_dict_factory(colnames, rows)]


def named_tuple_factory(colnames, rows):
    """
    Returns each row as a `namedtuple <https://docs.python.org/2/library/collections.html#collections.namedtuple>`_.
    This is the default row factory.

    Example::

        >>> from cassandra.query import named_tuple_factory
        >>> session = cluster.connect('mykeyspace')
        >>> session.row_factory = named_tuple_factory
        >>> rows = session.execute("SELECT name, age FROM users LIMIT 1")
        >>> user = rows[0]

        >>> # you can access field by their name:
        >>> print("name: %s, age: %d" % (user.name, user.age))
        name: Bob, age: 42

        >>> # or you can access fields by their position (like a tuple)
        >>> name, age = user
        >>> print("name: %s, age: %d" % (name, age))
        name: Bob, age: 42
        >>> name = user[0]
        >>> age = user[1]
        >>> print("name: %s, age: %d" % (name, age))
        name: Bob, age: 42

    .. versionchanged:: 2.0.0
        moved from ``cassandra.decoder`` to ``cassandra.query``
    """
    clean_column_names = map(_clean_column_name, colnames)
    try:
        Row = namedtuple('Row', clean_column_names)
    except SyntaxError:
        warnings.warn(
            "Failed creating namedtuple for a result because there were too "
            "many columns. This is due to a Python limitation that affects "
            "namedtuple in Python 3.0-3.6 (see issue18896). The row will be "
            "created with {substitute_factory_name}, which lacks some namedtuple "
            "features and is slower. To avoid slower performance accessing "
            "values on row objects, Upgrade to Python 3.7, or use a different "
            "row factory. (column names: {colnames})".format(
                substitute_factory_name=pseudo_namedtuple_factory.__name__,
                colnames=colnames
            )
        )
        return pseudo_namedtuple_factory(colnames, rows)
    except Exception:
        clean_column_names = list(map(_clean_column_name, colnames))  # create list because py3 map object will be consumed by first attempt
        log.warning("Failed creating named tuple for results with column names %s (cleaned: %s) "
                    "(see Python 'namedtuple' documentation for details on name rules). "
                    "Results will be returned with positional names. "
                    "Avoid this by choosing different names, using SELECT \"<col name>\" AS aliases, "
                    "or specifying a different row_factory on your Session" %
                    (colnames, clean_column_names))
        Row = namedtuple('Row', _sanitize_identifiers(clean_column_names))

    return [Row(*row) for row in rows]


def dict_factory(colnames, rows):
    """
    Returns each row as a dict.

    Example::

        >>> from cassandra.query import dict_factory
        >>> session = cluster.connect('mykeyspace')
        >>> session.row_factory = dict_factory
        >>> rows = session.execute("SELECT name, age FROM users LIMIT 1")
        >>> print(rows[0])
        {u'age': 42, u'name': u'Bob'}

    .. versionchanged:: 2.0.0
        moved from ``cassandra.decoder`` to ``cassandra.query``
    """
    return [dict(zip(colnames, row)) for row in rows]


def ordered_dict_factory(colnames, rows):
    """
    Like :meth:`~cassandra.query.dict_factory`, but returns each row as an OrderedDict,
    so the order of the columns is preserved.

    .. versionchanged:: 2.0.0
        moved from ``cassandra.decoder`` to ``cassandra.query``
    """
    return [OrderedDict(zip(colnames, row)) for row in rows]


FETCH_SIZE_UNSET = object()


class Statement(object):
    """
    An abstract class representing a single query. There are three subclasses:
    :class:`.SimpleStatement`, :class:`.BoundStatement`, and :class:`.BatchStatement`.
    These can be passed to :meth:`.Session.execute()`.
    """

    retry_policy = None
    """
    An instance of a :class:`cassandra.policies.RetryPolicy` or one of its
    subclasses.  This controls when a query will be retried and how it
    will be retried.
    """

    consistency_level = None
    """
    The :class:`.ConsistencyLevel` to be used for this operation.  Defaults
    to :const:`None`, which means that the default consistency level for
    the Session this is executed in will be used.
    """

    fetch_size = FETCH_SIZE_UNSET
    """
    How many rows will be fetched at a time.  This overrides the default
    of :attr:`.Session.default_fetch_size`

    This only takes effect when protocol version 2 or higher is used.
    See :attr:`.Cluster.protocol_version` for details.

    .. versionadded:: 2.0.0
    """

    keyspace = None
    """
    The string name of the keyspace this query acts on. This is used when
    :class:`~.TokenAwarePolicy` is configured in the profile load balancing policy.

    It is set implicitly on :class:`.BoundStatement`, and :class:`.BatchStatement`,
    but must be set explicitly on :class:`.SimpleStatement`.

    .. versionadded:: 2.1.3
    """

    custom_payload = None
    """
    :ref:`custom_payload` to be passed to the server.

    These are only allowed when using protocol version 4 or higher.

    .. versionadded:: 2.6.0
    """

    is_idempotent = False
    """
    Flag indicating whether this statement is safe to run multiple times in speculative execution.
    """

    _serial_consistency_level = None
    _routing_key = None

    def __init__(self, retry_policy=None, consistency_level=None, routing_key=None,
                 serial_consistency_level=None, fetch_size=FETCH_SIZE_UNSET, keyspace=None, custom_payload=None,
                 is_idempotent=False):
        if retry_policy and not hasattr(retry_policy, 'on_read_timeout'):  # just checking one method to detect positional parameter errors
            raise ValueError('retry_policy should implement cassandra.policies.RetryPolicy')
        if retry_policy is not None:
            self.retry_policy = retry_policy
        if consistency_level is not None:
            self.consistency_level = consistency_level
        self._routing_key = routing_key
        if serial_consistency_level is not None:
            self.serial_consistency_level = serial_consistency_level
        if fetch_size is not FETCH_SIZE_UNSET:
            self.fetch_size = fetch_size
        if keyspace is not None:
            self.keyspace = keyspace
        if custom_payload is not None:
            self.custom_payload = custom_payload
        self.is_idempotent = is_idempotent

    def _key_parts_packed(self, parts):
        for p in parts:
            l = len(p)
            yield struct.pack(">H%dsB" % l, l, p, 0)

    def _get_routing_key(self):
        return self._routing_key

    def _set_routing_key(self, key):
        if isinstance(key, (list, tuple)):
            if len(key) == 1:
                self._routing_key = key[0]
            else:
                self._routing_key = b"".join(self._key_parts_packed(key))
        else:
            self._routing_key = key

    def _del_routing_key(self):
        self._routing_key = None

    routing_key = property(
        _get_routing_key,
        _set_routing_key,
        _del_routing_key,
        """
        The :attr:`~.TableMetadata.partition_key` portion of the primary key,
        which can be used to determine which nodes are replicas for the query.

        If the partition key is a composite, a list or tuple must be passed in.
        Each key component should be in its packed (binary) format, so all
        components should be strings.
        """)

    def _get_serial_consistency_level(self):
        return self._serial_consistency_level

    def _set_serial_consistency_level(self, serial_consistency_level):
        if (serial_consistency_level is not None and
                not ConsistencyLevel.is_serial(serial_consistency_level)):
            raise ValueError(
                "serial_consistency_level must be either ConsistencyLevel.SERIAL "
                "or ConsistencyLevel.LOCAL_SERIAL")
        self._serial_consistency_level = serial_consistency_level

    def _del_serial_consistency_level(self):
        self._serial_consistency_level = None

    serial_consistency_level = property(
        _get_serial_consistency_level,
        _set_serial_consistency_level,
        _del_serial_consistency_level,
        """
        The serial consistency level is only used by conditional updates
        (``INSERT``, ``UPDATE`` and ``DELETE`` with an ``IF`` condition).  For
        those, the ``serial_consistency_level`` defines the consistency level of
        the serial phase (or "paxos" phase) while the normal
        :attr:`~.consistency_level` defines the consistency for the "learn" phase,
        i.e. what type of reads will be guaranteed to see the update right away.
        For example, if a conditional write has a :attr:`~.consistency_level` of
        :attr:`~.ConsistencyLevel.QUORUM` (and is successful), then a
        :attr:`~.ConsistencyLevel.QUORUM` read is guaranteed to see that write.
        But if the regular :attr:`~.consistency_level` of that write is
        :attr:`~.ConsistencyLevel.ANY`, then only a read with a
        :attr:`~.consistency_level` of :attr:`~.ConsistencyLevel.SERIAL` is
        guaranteed to see it (even a read with consistency
        :attr:`~.ConsistencyLevel.ALL` is not guaranteed to be enough).

        The serial consistency can only be one of :attr:`~.ConsistencyLevel.SERIAL`
        or :attr:`~.ConsistencyLevel.LOCAL_SERIAL`. While ``SERIAL`` guarantees full
        linearizability (with other ``SERIAL`` updates), ``LOCAL_SERIAL`` only
        guarantees it in the local data center.

        The serial consistency level is ignored for any query that is not a
        conditional update. Serial reads should use the regular
        :attr:`consistency_level`.

        Serial consistency levels may only be used against Cassandra 2.0+
        and the :attr:`~.Cluster.protocol_version` must be set to 2 or higher.

        See :doc:`/lwt` for a discussion on how to work with results returned from
        conditional statements.

        .. versionadded:: 2.0.0
        """)


class SimpleStatement(Statement):
    """
    A simple, un-prepared query.
    """

    def __init__(self, query_string, retry_policy=None, consistency_level=None, routing_key=None,
                 serial_consistency_level=None, fetch_size=FETCH_SIZE_UNSET, keyspace=None,
                 custom_payload=None, is_idempotent=False):
        """
        `query_string` should be a literal CQL statement with the exception
        of parameter placeholders that will be filled through the
        `parameters` argument of :meth:`.Session.execute()`.

        See :class:`Statement` attributes for a description of the other parameters.
        """
        Statement.__init__(self, retry_policy, consistency_level, routing_key,
                           serial_consistency_level, fetch_size, keyspace, custom_payload, is_idempotent)
        self._query_string = query_string

    @property
    def query_string(self):
        return self._query_string

    def __str__(self):
        consistency = ConsistencyLevel.value_to_name.get(self.consistency_level, 'Not Set')
        return (u'<SimpleStatement query="%s", consistency=%s>' %
                (self.query_string, consistency))
    __repr__ = __str__


class PreparedStatement(object):
    """
    A statement that has been prepared against at least one Cassandra node.
    Instances of this class should not be created directly, but through
    :meth:`.Session.prepare()`.

    A :class:`.PreparedStatement` should be prepared only once. Re-preparing a statement
    may affect performance (as the operation requires a network roundtrip).

    |prepared_stmt_head|: Do not use ``*`` in prepared statements if you might
    change the schema of the table being queried. The driver and server each
    maintain a map between metadata for a schema and statements that were
    prepared against that schema. When a user changes a schema, e.g. by adding
    or removing a column, the server invalidates its mappings involving that
    schema. However, there is currently no way to propagate that invalidation
    to drivers. Thus, after a schema change, the driver will incorrectly
    interpret the results of ``SELECT *`` queries prepared before the schema
    change. This is currently being addressed in `CASSANDRA-10786
    <https://issues.apache.org/jira/browse/CASSANDRA-10786>`_.

    .. |prepared_stmt_head| raw:: html

       <b>A note about <code>*</code> in prepared statements</b>
    """

    column_metadata = None  #TODO: make this bind_metadata in next major
    retry_policy = None
    consistency_level = None
    custom_payload = None
    fetch_size = FETCH_SIZE_UNSET
    keyspace = None  # change to prepared_keyspace in major release
    protocol_version = None
    query_id = None
    query_string = None
    result_metadata = None
    result_metadata_id = None
    column_encryption_policy = None
    routing_key_indexes = None
    _routing_key_index_set = None
    serial_consistency_level = None  # TODO never used?

    def __init__(self, column_metadata, query_id, routing_key_indexes, query,
                 keyspace, protocol_version, result_metadata, result_metadata_id,
                 column_encryption_policy=None):
        self.column_metadata = column_metadata
        self.query_id = query_id
        self.routing_key_indexes = routing_key_indexes
        self.query_string = query
        self.keyspace = keyspace
        self.protocol_version = protocol_version
        self.result_metadata = result_metadata
        self.result_metadata_id = result_metadata_id
        self.column_encryption_policy = column_encryption_policy
        self.is_idempotent = False

    @classmethod
    def from_message(cls, query_id, column_metadata, pk_indexes, cluster_metadata,
                     query, prepared_keyspace, protocol_version, result_metadata,
                     result_metadata_id, column_encryption_policy=None):
        if not column_metadata:
            return PreparedStatement(column_metadata, query_id, None,
                                     query, prepared_keyspace, protocol_version, result_metadata,
                                     result_metadata_id, column_encryption_policy)

        if pk_indexes:
            routing_key_indexes = pk_indexes
        else:
            routing_key_indexes = None

            first_col = column_metadata[0]
            ks_meta = cluster_metadata.keyspaces.get(first_col.keyspace_name)
            if ks_meta:
                table_meta = ks_meta.tables.get(first_col.table_name)
                if table_meta:
                    partition_key_columns = table_meta.partition_key

                    # make a map of {column_name: index} for each column in the statement
                    statement_indexes = dict((c.name, i) for i, c in enumerate(column_metadata))

                    # a list of which indexes in the statement correspond to partition key items
                    try:
                        routing_key_indexes = [statement_indexes[c.name]
                                               for c in partition_key_columns]
                    except KeyError:  # we're missing a partition key component in the prepared
                        pass          # statement; just leave routing_key_indexes as None

        return PreparedStatement(column_metadata, query_id, routing_key_indexes,
                                 query, prepared_keyspace, protocol_version, result_metadata,
                                 result_metadata_id, column_encryption_policy)

    def bind(self, values):
        """
        Creates and returns a :class:`BoundStatement` instance using `values`.

        See :meth:`BoundStatement.bind` for rules on input ``values``.
        """
        return BoundStatement(self).bind(values)

    def is_routing_key_index(self, i):
        if self._routing_key_index_set is None:
            self._routing_key_index_set = set(self.routing_key_indexes) if self.routing_key_indexes else set()
        return i in self._routing_key_index_set

    def __str__(self):
        consistency = ConsistencyLevel.value_to_name.get(self.consistency_level, 'Not Set')
        return (u'<PreparedStatement query="%s", consistency=%s>' %
                (self.query_string, consistency))
    __repr__ = __str__


class BoundStatement(Statement):
    """
    A prepared statement that has been bound to a particular set of values.
    These may be created directly or through :meth:`.PreparedStatement.bind()`.
    """

    prepared_statement = None
    """
    The :class:`PreparedStatement` instance that this was created from.
    """

    values = None
    """
    The sequence of values that were bound to the prepared statement.
    """

    def __init__(self, prepared_statement, retry_policy=None, consistency_level=None, routing_key=None,
                 serial_consistency_level=None, fetch_size=FETCH_SIZE_UNSET, keyspace=None,
                 custom_payload=None):
        """
        `prepared_statement` should be an instance of :class:`PreparedStatement`.

        See :class:`Statement` attributes for a description of the other parameters.
        """
        self.prepared_statement = prepared_statement

        self.retry_policy = prepared_statement.retry_policy
        self.consistency_level = prepared_statement.consistency_level
        self.serial_consistency_level = prepared_statement.serial_consistency_level
        self.fetch_size = prepared_statement.fetch_size
        self.custom_payload = prepared_statement.custom_payload
        self.is_idempotent = prepared_statement.is_idempotent
        self.values = []

        meta = prepared_statement.column_metadata
        if meta:
            self.keyspace = meta[0].keyspace_name

        Statement.__init__(self, retry_policy, consistency_level, routing_key,
                           serial_consistency_level, fetch_size, keyspace, custom_payload,
                           prepared_statement.is_idempotent)

    def bind(self, values):
        """
        Binds a sequence of values for the prepared statement parameters
        and returns this instance.  Note that `values` *must* be:

        * a sequence, even if you are only binding one value, or
        * a dict that relates 1-to-1 between dict keys and columns

        .. versionchanged:: 2.6.0

            :data:`~.UNSET_VALUE` was introduced. These can be bound as positional parameters
            in a sequence, or by name in a dict. Additionally, when using protocol v4+:

            * short sequences will be extended to match bind parameters with UNSET_VALUE
            * names may be omitted from a dict with UNSET_VALUE implied.

        .. versionchanged:: 3.0.0

            method will not throw if extra keys are present in bound dict (PYTHON-178)
        """
        if values is None:
            values = ()
        proto_version = self.prepared_statement.protocol_version
        col_meta = self.prepared_statement.column_metadata
        ce_policy = self.prepared_statement.column_encryption_policy

        # special case for binding dicts
        if isinstance(values, dict):
            values_dict = values
            values = []

            # sort values accordingly
            for col in col_meta:
                try:
                    values.append(values_dict[col.name])
                except KeyError:
                    if proto_version >= 4:
                        values.append(UNSET_VALUE)
                    else:
                        raise KeyError(
                            'Column name `%s` not found in bound dict.' %
                            (col.name))

        value_len = len(values)
        col_meta_len = len(col_meta)

        if value_len > col_meta_len:
            raise ValueError(
                "Too many arguments provided to bind() (got %d, expected %d)" %
                (len(values), len(col_meta)))

        # this is fail-fast for clarity pre-v4. When v4 can be assumed,
        # the error will be better reported when UNSET_VALUE is implicitly added.
        if proto_version < 4 and self.prepared_statement.routing_key_indexes and \
           value_len < len(self.prepared_statement.routing_key_indexes):
            raise ValueError(
                "Too few arguments provided to bind() (got %d, required %d for routing key)" %
                (value_len, len(self.prepared_statement.routing_key_indexes)))

        self.raw_values = values
        self.values = []
        for value, col_spec in zip(values, col_meta):
            if value is None:
                self.values.append(None)
            elif value is UNSET_VALUE:
                if proto_version >= 4:
                    self._append_unset_value()
                else:
                    raise ValueError("Attempt to bind UNSET_VALUE while using unsuitable protocol version (%d < 4)" % proto_version)
            else:
                try:
                    col_desc = ColDesc(col_spec.keyspace_name, col_spec.table_name, col_spec.name)
                    uses_ce = ce_policy and ce_policy.contains_column(col_desc)
                    col_type = ce_policy.column_type(col_desc) if uses_ce else col_spec.type
                    col_bytes = col_type.serialize(value, proto_version)
                    if uses_ce:
                        col_bytes = ce_policy.encrypt(col_desc, col_bytes)
                    self.values.append(col_bytes)
                except (TypeError, struct.error) as exc:
                    actual_type = type(value)
                    message = ('Received an argument of invalid type for column "%s". '
                               'Expected: %s, Got: %s; (%s)' % (col_spec.name, col_spec.type, actual_type, exc))
                    raise TypeError(message)

        if proto_version >= 4:
            diff = col_meta_len - len(self.values)
            if diff:
                for _ in range(diff):
                    self._append_unset_value()

        return self

    def _append_unset_value(self):
        next_index = len(self.values)
        if self.prepared_statement.is_routing_key_index(next_index):
            col_meta = self.prepared_statement.column_metadata[next_index]
            raise ValueError("Cannot bind UNSET_VALUE as a part of the routing key '%s'" % col_meta.name)
        self.values.append(UNSET_VALUE)

    @property
    def routing_key(self):
        if not self.prepared_statement.routing_key_indexes:
            return None

        if self._routing_key is not None:
            return self._routing_key

        routing_indexes = self.prepared_statement.routing_key_indexes
        if len(routing_indexes) == 1:
            self._routing_key = self.values[routing_indexes[0]]
        else:
            self._routing_key = b"".join(self._key_parts_packed(self.values[i] for i in routing_indexes))

        return self._routing_key

    def __str__(self):
        consistency = ConsistencyLevel.value_to_name.get(self.consistency_level, 'Not Set')
        return (u'<BoundStatement query="%s", values=%s, consistency=%s>' %
                (self.prepared_statement.query_string, self.raw_values, consistency))
    __repr__ = __str__


class BatchType(object):
    """
    A BatchType is used with :class:`.BatchStatement` instances to control
    the atomicity of the batch operation.

    .. versionadded:: 2.0.0
    """

    LOGGED = None
    """
    Atomic batch operation.
    """

    UNLOGGED = None
    """
    Non-atomic batch operation.
    """

    COUNTER = None
    """
    Batches of counter operations.
    """

    def __init__(self, name, value):
        self.name = name
        self.value = value

    def __str__(self):
        return self.name

    def __repr__(self):
        return "BatchType.%s" % (self.name, )


BatchType.LOGGED = BatchType("LOGGED", 0)
BatchType.UNLOGGED = BatchType("UNLOGGED", 1)
BatchType.COUNTER = BatchType("COUNTER", 2)


class BatchStatement(Statement):
    """
    A protocol-level batch of operations which are applied atomically
    by default.

    .. versionadded:: 2.0.0
    """

    batch_type = None
    """
    The :class:`.BatchType` for the batch operation.  Defaults to
    :attr:`.BatchType.LOGGED`.
    """

    serial_consistency_level = None
    """
    The same as :attr:`.Statement.serial_consistency_level`, but is only
    supported when using protocol version 3 or higher.
    """

    _statements_and_parameters = None
    _session = None

    def __init__(self, batch_type=BatchType.LOGGED, retry_policy=None,
                 consistency_level=None, serial_consistency_level=None,
                 session=None, custom_payload=None):
        """
        `batch_type` specifies The :class:`.BatchType` for the batch operation.
        Defaults to :attr:`.BatchType.LOGGED`.

        `retry_policy` should be a :class:`~.RetryPolicy` instance for
        controlling retries on the operation.

        `consistency_level` should be a :class:`~.ConsistencyLevel` value
        to be used for all operations in the batch.

        `custom_payload` is a :ref:`custom_payload` passed to the server.
        Note: as Statement objects are added to the batch, this map is
        updated with any values found in their custom payloads. These are
        only allowed when using protocol version 4 or higher.

        Example usage:

        .. code-block:: python

            insert_user = session.prepare("INSERT INTO users (name, age) VALUES (?, ?)")
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

            for (name, age) in users_to_insert:
                batch.add(insert_user, (name, age))

            session.execute(batch)

        You can also mix different types of operations within a batch:

        .. code-block:: python

            batch = BatchStatement()
            batch.add(SimpleStatement("INSERT INTO users (name, age) VALUES (%s, %s)"), (name, age))
            batch.add(SimpleStatement("DELETE FROM pending_users WHERE name=%s"), (name,))
            session.execute(batch)

        .. versionadded:: 2.0.0

        .. versionchanged:: 2.1.0
            Added `serial_consistency_level` as a parameter

        .. versionchanged:: 2.6.0
            Added `custom_payload` as a parameter
        """
        self.batch_type = batch_type
        self._statements_and_parameters = []
        self._session = session
        Statement.__init__(self, retry_policy=retry_policy, consistency_level=consistency_level,
                           serial_consistency_level=serial_consistency_level, custom_payload=custom_payload)

    def clear(self):
        """
        This is a convenience method to clear a batch statement for reuse.

        *Note:* it should not be used concurrently with uncompleted execution futures executing the same
        ``BatchStatement``.
        """
        del self._statements_and_parameters[:]
        self.keyspace = None
        self.routing_key = None
        if self.custom_payload:
            self.custom_payload.clear()

    def add(self, statement, parameters=None):
        """
        Adds a :class:`.Statement` and optional sequence of parameters
        to be used with the statement to the batch.

        Like with other statements, parameters must be a sequence, even
        if there is only one item.
        """
        if isinstance(statement, str):
            if parameters:
                encoder = Encoder() if self._session is None else self._session.encoder
                statement = bind_params(statement, parameters, encoder)
            self._add_statement_and_params(False, statement, ())
        elif isinstance(statement, PreparedStatement):
            query_id = statement.query_id
            bound_statement = statement.bind(() if parameters is None else parameters)
            self._update_state(bound_statement)
            self._add_statement_and_params(True, query_id, bound_statement.values)
        elif isinstance(statement, BoundStatement):
            if parameters:
                raise ValueError(
                    "Parameters cannot be passed with a BoundStatement "
                    "to BatchStatement.add()")
            self._update_state(statement)
            self._add_statement_and_params(True, statement.prepared_statement.query_id, statement.values)
        else:
            # it must be a SimpleStatement
            query_string = statement.query_string
            if parameters:
                encoder = Encoder() if self._session is None else self._session.encoder
                query_string = bind_params(query_string, parameters, encoder)
            self._update_state(statement)
            self._add_statement_and_params(False, query_string, ())
        return self

    def add_all(self, statements, parameters):
        """
        Adds a sequence of :class:`.Statement` objects and a matching sequence
        of parameters to the batch. Statement and parameter sequences must be of equal length or
        one will be truncated. :const:`None` can be used in the parameters position where are needed.
        """
        for statement, value in zip(statements, parameters):
            self.add(statement, value)

    def _add_statement_and_params(self, is_prepared, statement, parameters):
        if len(self._statements_and_parameters) >= 0xFFFF:
            raise ValueError("Batch statement cannot contain more than %d statements." % 0xFFFF)
        self._statements_and_parameters.append((is_prepared, statement, parameters))

    def _maybe_set_routing_attributes(self, statement):
        if self.routing_key is None:
            if statement.keyspace and statement.routing_key:
                self.routing_key = statement.routing_key
                self.keyspace = statement.keyspace

    def _update_custom_payload(self, statement):
        if statement.custom_payload:
            if self.custom_payload is None:
                self.custom_payload = {}
            self.custom_payload.update(statement.custom_payload)

    def _update_state(self, statement):
        self._maybe_set_routing_attributes(statement)
        self._update_custom_payload(statement)

    def __len__(self):
        return len(self._statements_and_parameters)

    def __str__(self):
        consistency = ConsistencyLevel.value_to_name.get(self.consistency_level, 'Not Set')
        return (u'<BatchStatement type=%s, statements=%d, consistency=%s>' %
                (self.batch_type, len(self), consistency))
    __repr__ = __str__


ValueSequence = cassandra.encoder.ValueSequence
"""
A wrapper class that is used to specify that a sequence of values should
be treated as a CQL list of values instead of a single column collection when used
as part of the `parameters` argument for :meth:`.Session.execute()`.

This is typically needed when supplying a list of keys to select.
For example::

    >>> my_user_ids = ('alice', 'bob', 'charles')
    >>> query = "SELECT * FROM users WHERE user_id IN %s"
    >>> session.execute(query, parameters=[ValueSequence(my_user_ids)])

"""


def bind_params(query, params, encoder):
    if isinstance(params, dict):
        return query % dict((k, encoder.cql_encode_all_types(v)) for k, v in params.items())
    else:
        return query % tuple(encoder.cql_encode_all_types(v) for v in params)


class TraceUnavailable(Exception):
    """
    Raised when complete trace details cannot be fetched from Cassandra.
    """
    pass


class QueryTrace(object):
    """
    A trace of the duration and events that occurred when executing
    an operation.
    """

    trace_id = None
    """
    :class:`uuid.UUID` unique identifier for this tracing session.  Matches
    the ``session_id`` column in ``system_traces.sessions`` and
    ``system_traces.events``.
    """

    request_type = None
    """
    A string that very generally describes the traced operation.
    """

    duration = None
    """
    A :class:`datetime.timedelta` measure of the duration of the query.
    """

    client = None
    """
    The IP address of the client that issued this request

    This is only available when using Cassandra 2.2+
    """

    coordinator = None
    """
    The IP address of the host that acted as coordinator for this request.
    """

    parameters = None
    """
    A :class:`dict` of parameters for the traced operation, such as the
    specific query string.
    """

    started_at = None
    """
    A UTC :class:`datetime.datetime` object describing when the operation
    was started.
    """

    events = None
    """
    A chronologically sorted list of :class:`.TraceEvent` instances
    representing the steps the traced operation went through.  This
    corresponds to the rows in ``system_traces.events`` for this tracing
    session.
    """

    _session = None

    _SELECT_SESSIONS_FORMAT = "SELECT * FROM system_traces.sessions WHERE session_id = %s"
    _SELECT_EVENTS_FORMAT = "SELECT * FROM system_traces.events WHERE session_id = %s"
    _BASE_RETRY_SLEEP = 0.003

    def __init__(self, trace_id, session):
        self.trace_id = trace_id
        self._session = session

    def populate(self, max_wait=2.0, wait_for_complete=True, query_cl=None):
        """
        Retrieves the actual tracing details from Cassandra and populates the
        attributes of this instance.  Because tracing details are stored
        asynchronously by Cassandra, this may need to retry the session
        detail fetch.  If the trace is still not available after `max_wait`
        seconds, :exc:`.TraceUnavailable` will be raised; if `max_wait` is
        :const:`None`, this will retry forever.

        `wait_for_complete=False` bypasses the wait for duration to be populated.
        This can be used to query events from partial sessions.

        `query_cl` specifies a consistency level to use for polling the trace tables,
        if different from the session default.
        """
        attempt = 0
        start = time.time()
        while True:
            time_spent = time.time() - start
            if max_wait is not None and time_spent >= max_wait:
                raise TraceUnavailable(
                    "Trace information was not available within %f seconds. Consider raising Session.max_trace_wait." % (max_wait,))

            log.debug("Attempting to fetch trace info for trace ID: %s", self.trace_id)
            session_results = self._execute(
                SimpleStatement(self._SELECT_SESSIONS_FORMAT, consistency_level=query_cl), (self.trace_id,), time_spent, max_wait)

            # PYTHON-730: There is race condition that the duration mutation is written before started_at the for fast queries
            session_row = session_results.one() if session_results else None
            is_complete = session_row is not None and session_row.duration is not None and session_row.started_at is not None
            if not session_results or (wait_for_complete and not is_complete):
                time.sleep(self._BASE_RETRY_SLEEP * (2 ** attempt))
                attempt += 1
                continue
            if is_complete:
                log.debug("Fetched trace info for trace ID: %s", self.trace_id)
            else:
                log.debug("Fetching partial trace info for trace ID: %s", self.trace_id)

            self.request_type = session_row.request
            self.duration = timedelta(microseconds=session_row.duration) if is_complete else None
            self.started_at = session_row.started_at
            self.coordinator = session_row.coordinator
            self.parameters = session_row.parameters
            # since C* 2.2
            self.client = getattr(session_row, 'client', None)

            log.debug("Attempting to fetch trace events for trace ID: %s", self.trace_id)
            time_spent = time.time() - start
            event_results = self._execute(
                SimpleStatement(self._SELECT_EVENTS_FORMAT, consistency_level=query_cl), (self.trace_id,), time_spent, max_wait)
            log.debug("Fetched trace events for trace ID: %s", self.trace_id)
            self.events = tuple(TraceEvent(r.activity, r.event_id, r.source, r.source_elapsed, r.thread)
                                for r in event_results)
            break

    def _execute(self, query, parameters, time_spent, max_wait):
        timeout = (max_wait - time_spent) if max_wait is not None else None
        future = self._session._create_response_future(query, parameters, trace=False, custom_payload=None, timeout=timeout)
        # in case the user switched the row factory, set it to namedtuple for this query
        future.row_factory = named_tuple_factory
        future.send_request()

        try:
            return future.result()
        except OperationTimedOut:
            raise TraceUnavailable("Trace information was not available within %f seconds" % (max_wait,))

    def __str__(self):
        return "%s [%s] coordinator: %s, started at: %s, duration: %s, parameters: %s" \
               % (self.request_type, self.trace_id, self.coordinator, self.started_at,
                  self.duration, self.parameters)


class TraceEvent(object):
    """
    Representation of a single event within a query trace.
    """

    description = None
    """
    A brief description of the event.
    """

    datetime = None
    """
    A UTC :class:`datetime.datetime` marking when the event occurred.
    """

    source = None
    """
    The IP address of the node this event occurred on.
    """

    source_elapsed = None
    """
    A :class:`datetime.timedelta` measuring the amount of time until
    this event occurred starting from when :attr:`.source` first
    received the query.
    """

    thread_name = None
    """
    The name of the thread that this event occurred on.
    """

    def __init__(self, description, timeuuid, source, source_elapsed, thread_name):
        self.description = description
        self.datetime = datetime.fromtimestamp(unix_time_from_uuid1(timeuuid), tz=timezone.utc)
        self.source = source
        if source_elapsed is not None:
            self.source_elapsed = timedelta(microseconds=source_elapsed)
        else:
            self.source_elapsed = None
        self.thread_name = thread_name

    def __str__(self):
        return "%s on %s[%s] at %s" % (self.description, self.source, self.thread_name, self.datetime)


# TODO remove next major since we can target using the `host` attribute of session.execute
class HostTargetingStatement(object):
    """
    Wraps any query statement and attaches a target host, making
    it usable in a targeted LBP without modifying the user's statement.
    """
    def __init__(self, inner_statement, target_host):
            self.__class__ = type(inner_statement.__class__.__name__,
                                  (self.__class__, inner_statement.__class__),
                                  {})
            self.__dict__ = inner_statement.__dict__
            self.target_host = target_host
