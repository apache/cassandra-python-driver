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

import logging


class NullHandler(logging.Handler):

    def emit(self, record):
        pass

logging.getLogger('cassandra').addHandler(NullHandler())

__version_info__ = (3, 29, 3)
__version__ = '.'.join(map(str, __version_info__))


class ConsistencyLevel(object):
    """
    Specifies how many replicas must respond for an operation to be considered
    a success.  By default, ``ONE`` is used for all operations.
    """

    ANY = 0
    """
    Only requires that one replica receives the write *or* the coordinator
    stores a hint to replay later. Valid only for writes.
    """

    ONE = 1
    """
    Only one replica needs to respond to consider the operation a success
    """

    TWO = 2
    """
    Two replicas must respond to consider the operation a success
    """

    THREE = 3
    """
    Three replicas must respond to consider the operation a success
    """

    QUORUM = 4
    """
    ``ceil(RF/2) + 1`` replicas must respond to consider the operation a success
    """

    ALL = 5
    """
    All replicas must respond to consider the operation a success
    """

    LOCAL_QUORUM = 6
    """
    Requires a quorum of replicas in the local datacenter
    """

    EACH_QUORUM = 7
    """
    Requires a quorum of replicas in each datacenter
    """

    SERIAL = 8
    """
    For conditional inserts/updates that utilize Cassandra's lightweight
    transactions, this requires consensus among all replicas for the
    modified data.
    """

    LOCAL_SERIAL = 9
    """
    Like :attr:`~ConsistencyLevel.SERIAL`, but only requires consensus
    among replicas in the local datacenter.
    """

    LOCAL_ONE = 10
    """
    Sends a request only to replicas in the local datacenter and waits for
    one response.
    """

    @staticmethod
    def is_serial(cl):
        return cl == ConsistencyLevel.SERIAL or cl == ConsistencyLevel.LOCAL_SERIAL


ConsistencyLevel.value_to_name = {
    ConsistencyLevel.ANY: 'ANY',
    ConsistencyLevel.ONE: 'ONE',
    ConsistencyLevel.TWO: 'TWO',
    ConsistencyLevel.THREE: 'THREE',
    ConsistencyLevel.QUORUM: 'QUORUM',
    ConsistencyLevel.ALL: 'ALL',
    ConsistencyLevel.LOCAL_QUORUM: 'LOCAL_QUORUM',
    ConsistencyLevel.EACH_QUORUM: 'EACH_QUORUM',
    ConsistencyLevel.SERIAL: 'SERIAL',
    ConsistencyLevel.LOCAL_SERIAL: 'LOCAL_SERIAL',
    ConsistencyLevel.LOCAL_ONE: 'LOCAL_ONE'
}

ConsistencyLevel.name_to_value = {
    'ANY': ConsistencyLevel.ANY,
    'ONE': ConsistencyLevel.ONE,
    'TWO': ConsistencyLevel.TWO,
    'THREE': ConsistencyLevel.THREE,
    'QUORUM': ConsistencyLevel.QUORUM,
    'ALL': ConsistencyLevel.ALL,
    'LOCAL_QUORUM': ConsistencyLevel.LOCAL_QUORUM,
    'EACH_QUORUM': ConsistencyLevel.EACH_QUORUM,
    'SERIAL': ConsistencyLevel.SERIAL,
    'LOCAL_SERIAL': ConsistencyLevel.LOCAL_SERIAL,
    'LOCAL_ONE': ConsistencyLevel.LOCAL_ONE
}


def consistency_value_to_name(value):
    return ConsistencyLevel.value_to_name[value] if value is not None else "Not Set"


class ProtocolVersion(object):
    """
    Defines native protocol versions supported by this driver.
    """
    V1 = 1
    """
    v1, supported in Cassandra 1.2-->2.2
    """

    V2 = 2
    """
    v2, supported in Cassandra 2.0-->2.2;
    added support for lightweight transactions, batch operations, and automatic query paging.
    """

    V3 = 3
    """
    v3, supported in Cassandra 2.1-->3.x+;
    added support for protocol-level client-side timestamps (see :attr:`.Session.use_client_timestamp`),
    serial consistency levels for :class:`~.BatchStatement`, and an improved connection pool.
    """

    V4 = 4
    """
    v4, supported in Cassandra 2.2-->3.x+;
    added a number of new types, server warnings, new failure messages, and custom payloads. Details in the
    `project docs <https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec>`_
    """

    V5 = 5
    """
    v5, in beta from 3.x+. Finalised in 4.0-beta5
    """

    V6 = 6
    """
    v6, in beta from 4.0-beta5
    """

    DSE_V1 = 0x41
    """
    DSE private protocol v1, supported in DSE 5.1+
    """

    DSE_V2 = 0x42
    """
    DSE private protocol v2, supported in DSE 6.0+
    """

    SUPPORTED_VERSIONS = (DSE_V2, DSE_V1, V6, V5, V4, V3, V2, V1)
    """
    A tuple of all supported protocol versions
    """

    BETA_VERSIONS = (V6,)
    """
    A tuple of all beta protocol versions
    """

    MIN_SUPPORTED = min(SUPPORTED_VERSIONS)
    """
    Minimum protocol version supported by this driver.
    """

    MAX_SUPPORTED = max(SUPPORTED_VERSIONS)
    """
    Maximum protocol version supported by this driver.
    """

    @classmethod
    def get_lower_supported(cls, previous_version):
        """
        Return the lower supported protocol version. Beta versions are omitted.
        """
        try:
            version = next(v for v in sorted(ProtocolVersion.SUPPORTED_VERSIONS, reverse=True) if
                           v not in ProtocolVersion.BETA_VERSIONS and v < previous_version)
        except StopIteration:
            version = 0

        return version

    @classmethod
    def uses_int_query_flags(cls, version):
        return version >= cls.V5

    @classmethod
    def uses_prepare_flags(cls, version):
        return version >= cls.V5 and version != cls.DSE_V1

    @classmethod
    def uses_prepared_metadata(cls, version):
        return version >= cls.V5 and version != cls.DSE_V1

    @classmethod
    def uses_error_code_map(cls, version):
        return version >= cls.V5

    @classmethod
    def uses_keyspace_flag(cls, version):
        return version >= cls.V5 and version != cls.DSE_V1

    @classmethod
    def has_continuous_paging_support(cls, version):
        return version >= cls.DSE_V1

    @classmethod
    def has_continuous_paging_next_pages(cls, version):
        return version >= cls.DSE_V2

    @classmethod
    def has_checksumming_support(cls, version):
        return cls.V5 <= version < cls.DSE_V1


class WriteType(object):
    """
    For usage with :class:`.RetryPolicy`, this describes a type
    of write operation.
    """

    SIMPLE = 0
    """
    A write to a single partition key. Such writes are guaranteed to be atomic
    and isolated.
    """

    BATCH = 1
    """
    A write to multiple partition keys that used the distributed batch log to
    ensure atomicity.
    """

    UNLOGGED_BATCH = 2
    """
    A write to multiple partition keys that did not use the distributed batch
    log. Atomicity for such writes is not guaranteed.
    """

    COUNTER = 3
    """
    A counter write (for one or multiple partition keys). Such writes should
    not be replayed in order to avoid over counting.
    """

    BATCH_LOG = 4
    """
    The initial write to the distributed batch log that Cassandra performs
    internally before a BATCH write.
    """

    CAS = 5
    """
    A lightweight-transaction write, such as "DELETE ... IF EXISTS".
    """

    VIEW = 6
    """
    This WriteType is only seen in results for requests that were unable to
    complete MV operations.
    """

    CDC = 7
    """
    This WriteType is only seen in results for requests that were unable to
    complete CDC operations.
    """


WriteType.name_to_value = {
    'SIMPLE': WriteType.SIMPLE,
    'BATCH': WriteType.BATCH,
    'UNLOGGED_BATCH': WriteType.UNLOGGED_BATCH,
    'COUNTER': WriteType.COUNTER,
    'BATCH_LOG': WriteType.BATCH_LOG,
    'CAS': WriteType.CAS,
    'VIEW': WriteType.VIEW,
    'CDC': WriteType.CDC
}


WriteType.value_to_name = {v: k for k, v in WriteType.name_to_value.items()}


class SchemaChangeType(object):
    DROPPED = 'DROPPED'
    CREATED = 'CREATED'
    UPDATED = 'UPDATED'


class SchemaTargetType(object):
    KEYSPACE = 'KEYSPACE'
    TABLE = 'TABLE'
    TYPE = 'TYPE'
    FUNCTION = 'FUNCTION'
    AGGREGATE = 'AGGREGATE'


class SignatureDescriptor(object):

    def __init__(self, name, argument_types):
        self.name = name
        self.argument_types = argument_types

    @property
    def signature(self):
        """
        function signature string in the form 'name([type0[,type1[...]]])'

        can be used to uniquely identify overloaded function names within a keyspace
        """
        return self.format_signature(self.name, self.argument_types)

    @staticmethod
    def format_signature(name, argument_types):
        return "%s(%s)" % (name, ','.join(t for t in argument_types))

    def __repr__(self):
        return "%s(%s, %s)" % (self.__class__.__name__, self.name, self.argument_types)


class UserFunctionDescriptor(SignatureDescriptor):
    """
    Describes a User function by name and argument signature
    """

    name = None
    """
    name of the function
    """

    argument_types = None
    """
    Ordered list of CQL argument type names comprising the type signature
    """


class UserAggregateDescriptor(SignatureDescriptor):
    """
    Describes a User aggregate function by name and argument signature
    """

    name = None
    """
    name of the aggregate
    """

    argument_types = None
    """
    Ordered list of CQL argument type names comprising the type signature
    """


class DriverException(Exception):
    """
    Base for all exceptions explicitly raised by the driver.
    """
    pass


class RequestExecutionException(DriverException):
    """
    Base for request execution exceptions returned from the server.
    """
    pass


class Unavailable(RequestExecutionException):
    """
    There were not enough live replicas to satisfy the requested consistency
    level, so the coordinator node immediately failed the request without
    forwarding it to any replicas.
    """

    consistency = None
    """ The requested :class:`ConsistencyLevel` """

    required_replicas = None
    """ The number of replicas that needed to be live to complete the operation """

    alive_replicas = None
    """ The number of replicas that were actually alive """

    def __init__(self, summary_message, consistency=None, required_replicas=None, alive_replicas=None):
        self.consistency = consistency
        self.required_replicas = required_replicas
        self.alive_replicas = alive_replicas
        Exception.__init__(self, summary_message + ' info=' +
                           repr({'consistency': consistency_value_to_name(consistency),
                                 'required_replicas': required_replicas,
                                 'alive_replicas': alive_replicas}))


class Timeout(RequestExecutionException):
    """
    Replicas failed to respond to the coordinator node before timing out.
    """

    consistency = None
    """ The requested :class:`ConsistencyLevel` """

    required_responses = None
    """ The number of required replica responses """

    received_responses = None
    """
    The number of replicas that responded before the coordinator timed out
    the operation
    """

    def __init__(self, summary_message, consistency=None, required_responses=None,
                 received_responses=None, **kwargs):
        self.consistency = consistency
        self.required_responses = required_responses
        self.received_responses = received_responses

        if "write_type" in kwargs:
            kwargs["write_type"] = WriteType.value_to_name[kwargs["write_type"]]

        info = {'consistency': consistency_value_to_name(consistency),
                'required_responses': required_responses,
                'received_responses': received_responses}
        info.update(kwargs)

        Exception.__init__(self, summary_message + ' info=' + repr(info))


class ReadTimeout(Timeout):
    """
    A subclass of :exc:`Timeout` for read operations.

    This indicates that the replicas failed to respond to the coordinator
    node before the configured timeout. This timeout is configured in
    ``cassandra.yaml`` with the ``read_request_timeout_in_ms``
    and ``range_request_timeout_in_ms`` options.
    """

    data_retrieved = None
    """
    A boolean indicating whether the requested data was retrieved
    by the coordinator from any replicas before it timed out the
    operation
    """

    def __init__(self, message, data_retrieved=None, **kwargs):
        Timeout.__init__(self, message, **kwargs)
        self.data_retrieved = data_retrieved


class WriteTimeout(Timeout):
    """
    A subclass of :exc:`Timeout` for write operations.

    This indicates that the replicas failed to respond to the coordinator
    node before the configured timeout. This timeout is configured in
    ``cassandra.yaml`` with the ``write_request_timeout_in_ms``
    option.
    """

    write_type = None
    """
    The type of write operation, enum on :class:`~cassandra.policies.WriteType`
    """

    def __init__(self, message, write_type=None, **kwargs):
        kwargs["write_type"] = write_type
        Timeout.__init__(self, message, **kwargs)
        self.write_type = write_type


class CDCWriteFailure(RequestExecutionException):
    """
    Hit limit on data in CDC folder, writes are rejected
    """
    def __init__(self, message):
        Exception.__init__(self, message)


class CoordinationFailure(RequestExecutionException):
    """
    Replicas sent a failure to the coordinator.
    """

    consistency = None
    """ The requested :class:`ConsistencyLevel` """

    required_responses = None
    """ The number of required replica responses """

    received_responses = None
    """
    The number of replicas that responded before the coordinator timed out
    the operation
    """

    failures = None
    """
    The number of replicas that sent a failure message
    """

    error_code_map = None
    """
    A map of inet addresses to error codes representing replicas that sent
    a failure message.  Only set when `protocol_version` is 5 or higher.
    """

    def __init__(self, summary_message, consistency=None, required_responses=None,
                 received_responses=None, failures=None, error_code_map=None):
        self.consistency = consistency
        self.required_responses = required_responses
        self.received_responses = received_responses
        self.failures = failures
        self.error_code_map = error_code_map

        info_dict = {
            'consistency': consistency_value_to_name(consistency),
            'required_responses': required_responses,
            'received_responses': received_responses,
            'failures': failures
        }

        if error_code_map is not None:
            # make error codes look like "0x002a"
            formatted_map = dict((addr, '0x%04x' % err_code)
                                 for (addr, err_code) in error_code_map.items())
            info_dict['error_code_map'] = formatted_map

        Exception.__init__(self, summary_message + ' info=' + repr(info_dict))


class ReadFailure(CoordinationFailure):
    """
    A subclass of :exc:`CoordinationFailure` for read operations.

    This indicates that the replicas sent a failure message to the coordinator.
    """

    data_retrieved = None
    """
    A boolean indicating whether the requested data was retrieved
    by the coordinator from any replicas before it timed out the
    operation
    """

    def __init__(self, message, data_retrieved=None, **kwargs):
        CoordinationFailure.__init__(self, message, **kwargs)
        self.data_retrieved = data_retrieved


class WriteFailure(CoordinationFailure):
    """
    A subclass of :exc:`CoordinationFailure` for write operations.

    This indicates that the replicas sent a failure message to the coordinator.
    """

    write_type = None
    """
    The type of write operation, enum on :class:`~cassandra.policies.WriteType`
    """

    def __init__(self, message, write_type=None, **kwargs):
        CoordinationFailure.__init__(self, message, **kwargs)
        self.write_type = write_type


class FunctionFailure(RequestExecutionException):
    """
    User Defined Function failed during execution
    """

    keyspace = None
    """
    Keyspace of the function
    """

    function = None
    """
    Name of the function
    """

    arg_types = None
    """
    List of argument type names of the function
    """

    def __init__(self, summary_message, keyspace, function, arg_types):
        self.keyspace = keyspace
        self.function = function
        self.arg_types = arg_types
        Exception.__init__(self, summary_message)


class RequestValidationException(DriverException):
    """
    Server request validation failed
    """
    pass


class ConfigurationException(RequestValidationException):
    """
    Server indicated request errro due to current configuration
    """
    pass


class AlreadyExists(ConfigurationException):
    """
    An attempt was made to create a keyspace or table that already exists.
    """

    keyspace = None
    """
    The name of the keyspace that already exists, or, if an attempt was
    made to create a new table, the keyspace that the table is in.
    """

    table = None
    """
    The name of the table that already exists, or, if an attempt was
    make to create a keyspace, :const:`None`.
    """

    def __init__(self, keyspace=None, table=None):
        if table:
            message = "Table '%s.%s' already exists" % (keyspace, table)
        else:
            message = "Keyspace '%s' already exists" % (keyspace,)

        Exception.__init__(self, message)
        self.keyspace = keyspace
        self.table = table


class InvalidRequest(RequestValidationException):
    """
    A query was made that was invalid for some reason, such as trying to set
    the keyspace for a connection to a nonexistent keyspace.
    """
    pass


class Unauthorized(RequestValidationException):
    """
    The current user is not authorized to perform the requested operation.
    """
    pass


class AuthenticationFailed(DriverException):
    """
    Failed to authenticate.
    """
    pass


class OperationTimedOut(DriverException):
    """
    The operation took longer than the specified (client-side) timeout
    to complete.  This is not an error generated by Cassandra, only
    the driver.
    """

    errors = None
    """
    A dict of errors keyed by the :class:`~.Host` against which they occurred.
    """

    last_host = None
    """
    The last :class:`~.Host` this operation was attempted against.
    """

    def __init__(self, errors=None, last_host=None):
        self.errors = errors
        self.last_host = last_host
        message = "errors=%s, last_host=%s" % (self.errors, self.last_host)
        Exception.__init__(self, message)


class UnsupportedOperation(DriverException):
    """
    An attempt was made to use a feature that is not supported by the
    selected protocol version.  See :attr:`Cluster.protocol_version`
    for more details.
    """
    pass


class UnresolvableContactPoints(DriverException):
    """
    The driver was unable to resolve any provided hostnames.

    Note that this is *not* raised when a :class:`.Cluster` is created with no
    contact points, only when lookup fails for all hosts
    """
    pass

class DependencyException(Exception):
    """
    Specific exception class for handling issues with driver dependencies
    """

    excs = []
    """
    A sequence of child exceptions
    """

    def __init__(self, msg, excs=[]):
        complete_msg = msg
        if excs:
            complete_msg += ("\nThe following exceptions were observed: \n - " + '\n - '.join(str(e) for e in excs))
        Exception.__init__(self, complete_msg)
