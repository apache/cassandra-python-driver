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
from collections import namedtuple
import logging
import socket
from uuid import UUID

import io

from cassandra import ProtocolVersion
from cassandra import type_codes, DriverException
from cassandra import (Unavailable, WriteTimeout, ReadTimeout,
                       WriteFailure, ReadFailure, FunctionFailure,
                       AlreadyExists, InvalidRequest, Unauthorized,
                       UnsupportedOperation, UserFunctionDescriptor,
                       UserAggregateDescriptor, SchemaTargetType)
from cassandra.cqltypes import (AsciiType, BytesType, BooleanType,
                                CounterColumnType, DateType, DecimalType,
                                DoubleType, FloatType, Int32Type,
                                InetAddressType, IntegerType, ListType,
                                LongType, MapType, SetType, TimeUUIDType,
                                UTF8Type, VarcharType, UUIDType, UserType,
                                TupleType, lookup_casstype, SimpleDateType,
                                TimeType, ByteType, ShortType, DurationType)
from cassandra.marshal import (int32_pack, int32_unpack, uint16_pack, uint16_unpack,
                               uint8_pack, int8_unpack, uint64_pack, header_pack,
                               v3_header_pack, uint32_pack, uint32_le_unpack, uint32_le_pack)
from cassandra.policies import ColDesc
from cassandra import WriteType
from cassandra.cython_deps import HAVE_CYTHON, HAVE_NUMPY
from cassandra import util

log = logging.getLogger(__name__)


class NotSupportedError(Exception):
    pass


class InternalError(Exception):
    pass

ColumnMetadata = namedtuple("ColumnMetadata", ['keyspace_name', 'table_name', 'name', 'type'])

HEADER_DIRECTION_TO_CLIENT = 0x80
HEADER_DIRECTION_MASK = 0x80

COMPRESSED_FLAG = 0x01
TRACING_FLAG = 0x02
CUSTOM_PAYLOAD_FLAG = 0x04
WARNING_FLAG = 0x08
USE_BETA_FLAG = 0x10
USE_BETA_MASK = ~USE_BETA_FLAG

_message_types_by_opcode = {}

_UNSET_VALUE = object()


def register_class(cls):
    _message_types_by_opcode[cls.opcode] = cls


def get_registered_classes():
    return _message_types_by_opcode.copy()


class _RegisterMessageType(type):
    def __init__(cls, name, bases, dct):
        if not name.startswith('_'):
            register_class(cls)


class _MessageType(object, metaclass=_RegisterMessageType):

    tracing = False
    custom_payload = None
    warnings = None

    def update_custom_payload(self, other):
        if other:
            if not self.custom_payload:
                self.custom_payload = {}
            self.custom_payload.update(other)
            if len(self.custom_payload) > 65535:
                raise ValueError("Custom payload map exceeds max count allowed by protocol (65535)")

    def __repr__(self):
        return '<%s(%s)>' % (self.__class__.__name__, ', '.join('%s=%r' % i for i in _get_params(self)))


def _get_params(message_obj):
    base_attrs = dir(_MessageType)
    return (
        (n, a) for n, a in message_obj.__dict__.items()
        if n not in base_attrs and not n.startswith('_') and not callable(a)
    )


error_classes = {}


class ErrorMessage(_MessageType, Exception):
    opcode = 0x00
    name = 'ERROR'
    summary = 'Unknown'

    def __init__(self, code, message, info):
        self.code = code
        self.message = message
        self.info = info

    @classmethod
    def recv_body(cls, f, protocol_version, *args):
        code = read_int(f)
        msg = read_string(f)
        subcls = error_classes.get(code, cls)
        extra_info = subcls.recv_error_info(f, protocol_version)
        return subcls(code=code, message=msg, info=extra_info)

    def summary_msg(self):
        msg = 'Error from server: code=%04x [%s] message="%s"' \
              % (self.code, self.summary, self.message)
        return msg

    def __str__(self):
        return '<%s>' % self.summary_msg()
    __repr__ = __str__

    @staticmethod
    def recv_error_info(f, protocol_version):
        pass

    def to_exception(self):
        return self


class ErrorMessageSubclass(_RegisterMessageType):
    def __init__(cls, name, bases, dct):
        if cls.error_code is not None:  # Server has an error code of 0.
            error_classes[cls.error_code] = cls


class ErrorMessageSub(ErrorMessage, metaclass=ErrorMessageSubclass):
    error_code = None


class RequestExecutionException(ErrorMessageSub):
    pass


class RequestValidationException(ErrorMessageSub):
    pass


class ServerError(ErrorMessageSub):
    summary = 'Server error'
    error_code = 0x0000


class ProtocolException(ErrorMessageSub):
    summary = 'Protocol error'
    error_code = 0x000A

    @property
    def is_beta_protocol_error(self):
        return 'USE_BETA flag is unset' in str(self)


class BadCredentials(ErrorMessageSub):
    summary = 'Bad credentials'
    error_code = 0x0100


class UnavailableErrorMessage(RequestExecutionException):
    summary = 'Unavailable exception'
    error_code = 0x1000

    @staticmethod
    def recv_error_info(f, protocol_version):
        return {
            'consistency': read_consistency_level(f),
            'required_replicas': read_int(f),
            'alive_replicas': read_int(f),
        }

    def to_exception(self):
        return Unavailable(self.summary_msg(), **self.info)


class OverloadedErrorMessage(RequestExecutionException):
    summary = 'Coordinator node overloaded'
    error_code = 0x1001


class IsBootstrappingErrorMessage(RequestExecutionException):
    summary = 'Coordinator node is bootstrapping'
    error_code = 0x1002


class TruncateError(RequestExecutionException):
    summary = 'Error during truncate'
    error_code = 0x1003


class WriteTimeoutErrorMessage(RequestExecutionException):
    summary = "Coordinator node timed out waiting for replica nodes' responses"
    error_code = 0x1100

    @staticmethod
    def recv_error_info(f, protocol_version):
        return {
            'consistency': read_consistency_level(f),
            'received_responses': read_int(f),
            'required_responses': read_int(f),
            'write_type': WriteType.name_to_value[read_string(f)],
        }

    def to_exception(self):
        return WriteTimeout(self.summary_msg(), **self.info)


class ReadTimeoutErrorMessage(RequestExecutionException):
    summary = "Coordinator node timed out waiting for replica nodes' responses"
    error_code = 0x1200

    @staticmethod
    def recv_error_info(f, protocol_version):
        return {
            'consistency': read_consistency_level(f),
            'received_responses': read_int(f),
            'required_responses': read_int(f),
            'data_retrieved': bool(read_byte(f)),
        }

    def to_exception(self):
        return ReadTimeout(self.summary_msg(), **self.info)


class ReadFailureMessage(RequestExecutionException):
    summary = "Replica(s) failed to execute read"
    error_code = 0x1300

    @staticmethod
    def recv_error_info(f, protocol_version):
        consistency = read_consistency_level(f)
        received_responses = read_int(f)
        required_responses = read_int(f)

        if ProtocolVersion.uses_error_code_map(protocol_version):
            error_code_map = read_error_code_map(f)
            failures = len(error_code_map)
        else:
            error_code_map = None
            failures = read_int(f)

        data_retrieved = bool(read_byte(f))

        return {
            'consistency': consistency,
            'received_responses': received_responses,
            'required_responses': required_responses,
            'failures': failures,
            'error_code_map': error_code_map,
            'data_retrieved': data_retrieved
        }

    def to_exception(self):
        return ReadFailure(self.summary_msg(), **self.info)


class FunctionFailureMessage(RequestExecutionException):
    summary = "User Defined Function failure"
    error_code = 0x1400

    @staticmethod
    def recv_error_info(f, protocol_version):
        return {
            'keyspace': read_string(f),
            'function': read_string(f),
            'arg_types': [read_string(f) for _ in range(read_short(f))],
        }

    def to_exception(self):
        return FunctionFailure(self.summary_msg(), **self.info)


class WriteFailureMessage(RequestExecutionException):
    summary = "Replica(s) failed to execute write"
    error_code = 0x1500

    @staticmethod
    def recv_error_info(f, protocol_version):
        consistency = read_consistency_level(f)
        received_responses = read_int(f)
        required_responses = read_int(f)

        if ProtocolVersion.uses_error_code_map(protocol_version):
            error_code_map = read_error_code_map(f)
            failures = len(error_code_map)
        else:
            error_code_map = None
            failures = read_int(f)

        write_type = WriteType.name_to_value[read_string(f)]

        return {
            'consistency': consistency,
            'received_responses': received_responses,
            'required_responses': required_responses,
            'failures': failures,
            'error_code_map': error_code_map,
            'write_type': write_type
        }

    def to_exception(self):
        return WriteFailure(self.summary_msg(), **self.info)


class CDCWriteException(RequestExecutionException):
    summary = 'Failed to execute write due to CDC space exhaustion.'
    error_code = 0x1600


class SyntaxException(RequestValidationException):
    summary = 'Syntax error in CQL query'
    error_code = 0x2000


class UnauthorizedErrorMessage(RequestValidationException):
    summary = 'Unauthorized'
    error_code = 0x2100

    def to_exception(self):
        return Unauthorized(self.summary_msg())


class InvalidRequestException(RequestValidationException):
    summary = 'Invalid query'
    error_code = 0x2200

    def to_exception(self):
        return InvalidRequest(self.summary_msg())


class ConfigurationException(RequestValidationException):
    summary = 'Query invalid because of configuration issue'
    error_code = 0x2300


class PreparedQueryNotFound(RequestValidationException):
    summary = 'Matching prepared statement not found on this node'
    error_code = 0x2500

    @staticmethod
    def recv_error_info(f, protocol_version):
        # return the query ID
        return read_binary_string(f)


class AlreadyExistsException(ConfigurationException):
    summary = 'Item already exists'
    error_code = 0x2400

    @staticmethod
    def recv_error_info(f, protocol_version):
        return {
            'keyspace': read_string(f),
            'table': read_string(f),
        }

    def to_exception(self):
        return AlreadyExists(**self.info)


class ClientWriteError(RequestExecutionException):
    summary = 'Client write failure.'
    error_code = 0x8000


class StartupMessage(_MessageType):
    opcode = 0x01
    name = 'STARTUP'

    KNOWN_OPTION_KEYS = set((
        'CQL_VERSION',
        'COMPRESSION',
        'NO_COMPACT'
    ))

    def __init__(self, cqlversion, options):
        self.cqlversion = cqlversion
        self.options = options

    def send_body(self, f, protocol_version):
        optmap = self.options.copy()
        optmap['CQL_VERSION'] = self.cqlversion
        write_stringmap(f, optmap)


class ReadyMessage(_MessageType):
    opcode = 0x02
    name = 'READY'

    @classmethod
    def recv_body(cls, *args):
        return cls()


class AuthenticateMessage(_MessageType):
    opcode = 0x03
    name = 'AUTHENTICATE'

    def __init__(self, authenticator):
        self.authenticator = authenticator

    @classmethod
    def recv_body(cls, f, *args):
        authname = read_string(f)
        return cls(authenticator=authname)


class CredentialsMessage(_MessageType):
    opcode = 0x04
    name = 'CREDENTIALS'

    def __init__(self, creds):
        self.creds = creds

    def send_body(self, f, protocol_version):
        if protocol_version > 1:
            raise UnsupportedOperation(
                "Credentials-based authentication is not supported with "
                "protocol version 2 or higher.  Use the SASL authentication "
                "mechanism instead.")
        write_short(f, len(self.creds))
        for credkey, credval in self.creds.items():
            write_string(f, credkey)
            write_string(f, credval)


class AuthChallengeMessage(_MessageType):
    opcode = 0x0E
    name = 'AUTH_CHALLENGE'

    def __init__(self, challenge):
        self.challenge = challenge

    @classmethod
    def recv_body(cls, f, *args):
        return cls(read_binary_longstring(f))


class AuthResponseMessage(_MessageType):
    opcode = 0x0F
    name = 'AUTH_RESPONSE'

    def __init__(self, response):
        self.response = response

    def send_body(self, f, protocol_version):
        write_longstring(f, self.response)


class AuthSuccessMessage(_MessageType):
    opcode = 0x10
    name = 'AUTH_SUCCESS'

    def __init__(self, token):
        self.token = token

    @classmethod
    def recv_body(cls, f, *args):
        return cls(read_longstring(f))


class OptionsMessage(_MessageType):
    opcode = 0x05
    name = 'OPTIONS'

    def send_body(self, f, protocol_version):
        pass


class SupportedMessage(_MessageType):
    opcode = 0x06
    name = 'SUPPORTED'

    def __init__(self, cql_versions, options):
        self.cql_versions = cql_versions
        self.options = options

    @classmethod
    def recv_body(cls, f, *args):
        options = read_stringmultimap(f)
        cql_versions = options.pop('CQL_VERSION')
        return cls(cql_versions=cql_versions, options=options)


# used for QueryMessage and ExecuteMessage
_VALUES_FLAG = 0x01
_SKIP_METADATA_FLAG = 0x02
_PAGE_SIZE_FLAG = 0x04
_WITH_PAGING_STATE_FLAG = 0x08
_WITH_SERIAL_CONSISTENCY_FLAG = 0x10
_PROTOCOL_TIMESTAMP_FLAG = 0x20
_NAMES_FOR_VALUES_FLAG = 0x40  # not used here
_WITH_KEYSPACE_FLAG = 0x80
_PREPARED_WITH_KEYSPACE_FLAG = 0x01
_PAGE_SIZE_BYTES_FLAG = 0x40000000
_PAGING_OPTIONS_FLAG = 0x80000000


class _QueryMessage(_MessageType):

    def __init__(self, query_params, consistency_level,
                 serial_consistency_level=None, fetch_size=None,
                 paging_state=None, timestamp=None, skip_meta=False,
                 continuous_paging_options=None, keyspace=None):
        self.query_params = query_params
        self.consistency_level = consistency_level
        self.serial_consistency_level = serial_consistency_level
        self.fetch_size = fetch_size
        self.paging_state = paging_state
        self.timestamp = timestamp
        self.skip_meta = skip_meta
        self.continuous_paging_options = continuous_paging_options
        self.keyspace = keyspace

    def _write_query_params(self, f, protocol_version):
        write_consistency_level(f, self.consistency_level)
        flags = 0x00
        if self.query_params is not None:
            flags |= _VALUES_FLAG  # also v2+, but we're only setting params internally right now

        if self.serial_consistency_level:
            if protocol_version >= 2:
                flags |= _WITH_SERIAL_CONSISTENCY_FLAG
            else:
                raise UnsupportedOperation(
                    "Serial consistency levels require the use of protocol version "
                    "2 or higher. Consider setting Cluster.protocol_version to 2 "
                    "to support serial consistency levels.")

        if self.fetch_size:
            if protocol_version >= 2:
                flags |= _PAGE_SIZE_FLAG
            else:
                raise UnsupportedOperation(
                    "Automatic query paging may only be used with protocol version "
                    "2 or higher. Consider setting Cluster.protocol_version to 2.")

        if self.paging_state:
            if protocol_version >= 2:
                flags |= _WITH_PAGING_STATE_FLAG
            else:
                raise UnsupportedOperation(
                    "Automatic query paging may only be used with protocol version "
                    "2 or higher. Consider setting Cluster.protocol_version to 2.")

        if self.timestamp is not None:
            flags |= _PROTOCOL_TIMESTAMP_FLAG

        if self.continuous_paging_options:
            if ProtocolVersion.has_continuous_paging_support(protocol_version):
                flags |= _PAGING_OPTIONS_FLAG
            else:
                raise UnsupportedOperation(
                    "Continuous paging may only be used with protocol version "
                    "ProtocolVersion.DSE_V1 or higher. Consider setting Cluster.protocol_version to ProtocolVersion.DSE_V1.")

        if self.keyspace is not None:
            if ProtocolVersion.uses_keyspace_flag(protocol_version):
                flags |= _WITH_KEYSPACE_FLAG
            else:
                raise UnsupportedOperation(
                    "Keyspaces may only be set on queries with protocol version "
                    "5 or DSE_V2 or higher. Consider setting Cluster.protocol_version.")

        if ProtocolVersion.uses_int_query_flags(protocol_version):
            write_uint(f, flags)
        else:
            write_byte(f, flags)

        if self.query_params is not None:
            write_short(f, len(self.query_params))
            for param in self.query_params:
                write_value(f, param)
        if self.fetch_size:
            write_int(f, self.fetch_size)
        if self.paging_state:
            write_longstring(f, self.paging_state)
        if self.serial_consistency_level:
            write_consistency_level(f, self.serial_consistency_level)
        if self.timestamp is not None:
            write_long(f, self.timestamp)
        if self.keyspace is not None:
            write_string(f, self.keyspace)
        if self.continuous_paging_options:
            self._write_paging_options(f, self.continuous_paging_options, protocol_version)

    def _write_paging_options(self, f, paging_options, protocol_version):
        write_int(f, paging_options.max_pages)
        write_int(f, paging_options.max_pages_per_second)
        if ProtocolVersion.has_continuous_paging_next_pages(protocol_version):
            write_int(f, paging_options.max_queue_size)


class QueryMessage(_QueryMessage):
    opcode = 0x07
    name = 'QUERY'

    def __init__(self, query, consistency_level, serial_consistency_level=None,
                 fetch_size=None, paging_state=None, timestamp=None, continuous_paging_options=None, keyspace=None):
        self.query = query
        super(QueryMessage, self).__init__(None, consistency_level, serial_consistency_level, fetch_size,
                                           paging_state, timestamp, False, continuous_paging_options, keyspace)

    def send_body(self, f, protocol_version):
        write_longstring(f, self.query)
        self._write_query_params(f, protocol_version)


class ExecuteMessage(_QueryMessage):
    opcode = 0x0A
    name = 'EXECUTE'

    def __init__(self, query_id, query_params, consistency_level,
                 serial_consistency_level=None, fetch_size=None,
                 paging_state=None, timestamp=None, skip_meta=False,
                 continuous_paging_options=None, result_metadata_id=None):
        self.query_id = query_id
        self.result_metadata_id = result_metadata_id
        super(ExecuteMessage, self).__init__(query_params, consistency_level, serial_consistency_level, fetch_size,
                                             paging_state, timestamp, skip_meta, continuous_paging_options)

    def _write_query_params(self, f, protocol_version):
        if protocol_version == 1:
            if self.serial_consistency_level:
                raise UnsupportedOperation(
                    "Serial consistency levels require the use of protocol version "
                    "2 or higher. Consider setting Cluster.protocol_version to 2 "
                    "to support serial consistency levels.")
            if self.fetch_size or self.paging_state:
                raise UnsupportedOperation(
                    "Automatic query paging may only be used with protocol version "
                    "2 or higher. Consider setting Cluster.protocol_version to 2.")
            write_short(f, len(self.query_params))
            for param in self.query_params:
                write_value(f, param)
            write_consistency_level(f, self.consistency_level)
        else:
            super(ExecuteMessage, self)._write_query_params(f, protocol_version)

    def send_body(self, f, protocol_version):
        write_string(f, self.query_id)
        if ProtocolVersion.uses_prepared_metadata(protocol_version):
            write_string(f, self.result_metadata_id)
        self._write_query_params(f, protocol_version)


CUSTOM_TYPE = object()

RESULT_KIND_VOID = 0x0001
RESULT_KIND_ROWS = 0x0002
RESULT_KIND_SET_KEYSPACE = 0x0003
RESULT_KIND_PREPARED = 0x0004
RESULT_KIND_SCHEMA_CHANGE = 0x0005


class ResultMessage(_MessageType):
    opcode = 0x08
    name = 'RESULT'

    kind = None
    results = None
    paging_state = None

    # Names match type name in module scope. Most are imported from cassandra.cqltypes (except CUSTOM_TYPE)
    type_codes = _cqltypes_by_code = dict((v, globals()[k]) for k, v in type_codes.__dict__.items() if not k.startswith('_'))

    _FLAGS_GLOBAL_TABLES_SPEC = 0x0001
    _HAS_MORE_PAGES_FLAG = 0x0002
    _NO_METADATA_FLAG = 0x0004
    _CONTINUOUS_PAGING_FLAG = 0x40000000
    _CONTINUOUS_PAGING_LAST_FLAG = 0x80000000
    _METADATA_ID_FLAG = 0x0008

    kind = None

    # These are all the things a result message might contain. They are populated according to 'kind'
    column_names = None
    column_types = None
    parsed_rows = None
    paging_state = None
    continuous_paging_seq = None
    continuous_paging_last = None
    new_keyspace = None
    column_metadata = None
    query_id = None
    bind_metadata = None
    pk_indexes = None
    schema_change_event = None

    def __init__(self, kind):
        self.kind = kind

    def recv(self, f, protocol_version, user_type_map, result_metadata, column_encryption_policy):
        if self.kind == RESULT_KIND_VOID:
            return
        elif self.kind == RESULT_KIND_ROWS:
            self.recv_results_rows(f, protocol_version, user_type_map, result_metadata, column_encryption_policy)
        elif self.kind == RESULT_KIND_SET_KEYSPACE:
            self.new_keyspace = read_string(f)
        elif self.kind == RESULT_KIND_PREPARED:
            self.recv_results_prepared(f, protocol_version, user_type_map)
        elif self.kind == RESULT_KIND_SCHEMA_CHANGE:
            self.recv_results_schema_change(f, protocol_version)
        else:
            raise DriverException("Unknown RESULT kind: %d" % self.kind)

    @classmethod
    def recv_body(cls, f, protocol_version, user_type_map, result_metadata, column_encryption_policy):
        kind = read_int(f)
        msg = cls(kind)
        msg.recv(f, protocol_version, user_type_map, result_metadata, column_encryption_policy)
        return msg

    def recv_results_rows(self, f, protocol_version, user_type_map, result_metadata, column_encryption_policy):
        self.recv_results_metadata(f, user_type_map)
        column_metadata = self.column_metadata or result_metadata
        rowcount = read_int(f)
        rows = [self.recv_row(f, len(column_metadata)) for _ in range(rowcount)]
        self.column_names = [c[2] for c in column_metadata]
        self.column_types = [c[3] for c in column_metadata]
        col_descs = [ColDesc(md[0], md[1], md[2]) for md in column_metadata]

        def decode_val(val, col_md, col_desc):
            uses_ce = column_encryption_policy and column_encryption_policy.contains_column(col_desc)
            col_type = column_encryption_policy.column_type(col_desc) if uses_ce else col_md[3]
            raw_bytes = column_encryption_policy.decrypt(col_desc, val) if uses_ce else val
            return col_type.from_binary(raw_bytes, protocol_version)

        def decode_row(row):
            return tuple(decode_val(val, col_md, col_desc) for val, col_md, col_desc in zip(row, column_metadata, col_descs))

        try:
            self.parsed_rows = [decode_row(row) for row in rows]
        except Exception:
            for row in rows:
                for val, col_md, col_desc in zip(row, column_metadata, col_descs):
                    try:
                        decode_val(val, col_md, col_desc)
                    except Exception as e:
                        raise DriverException('Failed decoding result column "%s" of type %s: %s' % (col_md[2],
                                                                                                     col_md[3].cql_parameterized_type(),
                                                                                                     str(e)))

    def recv_results_prepared(self, f, protocol_version, user_type_map):
        self.query_id = read_binary_string(f)
        if ProtocolVersion.uses_prepared_metadata(protocol_version):
            self.result_metadata_id = read_binary_string(f)
        else:
            self.result_metadata_id = None
        self.recv_prepared_metadata(f, protocol_version, user_type_map)

    def recv_results_metadata(self, f, user_type_map):
        flags = read_int(f)
        colcount = read_int(f)

        if flags & self._HAS_MORE_PAGES_FLAG:
            self.paging_state = read_binary_longstring(f)

        no_meta = bool(flags & self._NO_METADATA_FLAG)
        if no_meta:
            return

        if flags & self._CONTINUOUS_PAGING_FLAG:
            self.continuous_paging_seq = read_int(f)
            self.continuous_paging_last = flags & self._CONTINUOUS_PAGING_LAST_FLAG

        if flags & self._METADATA_ID_FLAG:
            self.result_metadata_id = read_binary_string(f)

        glob_tblspec = bool(flags & self._FLAGS_GLOBAL_TABLES_SPEC)
        if glob_tblspec:
            ksname = read_string(f)
            cfname = read_string(f)
        column_metadata = []
        for _ in range(colcount):
            if glob_tblspec:
                colksname = ksname
                colcfname = cfname
            else:
                colksname = read_string(f)
                colcfname = read_string(f)
            colname = read_string(f)
            coltype = self.read_type(f, user_type_map)
            column_metadata.append((colksname, colcfname, colname, coltype))

        self.column_metadata = column_metadata

    def recv_prepared_metadata(self, f, protocol_version, user_type_map):
        flags = read_int(f)
        colcount = read_int(f)
        pk_indexes = None
        if protocol_version >= 4:
            num_pk_indexes = read_int(f)
            pk_indexes = [read_short(f) for _ in range(num_pk_indexes)]

        glob_tblspec = bool(flags & self._FLAGS_GLOBAL_TABLES_SPEC)
        if glob_tblspec:
            ksname = read_string(f)
            cfname = read_string(f)
        bind_metadata = []
        for _ in range(colcount):
            if glob_tblspec:
                colksname = ksname
                colcfname = cfname
            else:
                colksname = read_string(f)
                colcfname = read_string(f)
            colname = read_string(f)
            coltype = self.read_type(f, user_type_map)
            bind_metadata.append(ColumnMetadata(colksname, colcfname, colname, coltype))

        if protocol_version >= 2:
            self.recv_results_metadata(f, user_type_map)

        self.bind_metadata = bind_metadata
        self.pk_indexes = pk_indexes

    def recv_results_schema_change(self, f, protocol_version):
        self.schema_change_event = EventMessage.recv_schema_change(f, protocol_version)

    @classmethod
    def read_type(cls, f, user_type_map):
        optid = read_short(f)
        try:
            typeclass = cls.type_codes[optid]
        except KeyError:
            raise NotSupportedError("Unknown data type code 0x%04x. Have to skip"
                                    " entire result set." % (optid,))
        if typeclass in (ListType, SetType):
            subtype = cls.read_type(f, user_type_map)
            typeclass = typeclass.apply_parameters((subtype,))
        elif typeclass == MapType:
            keysubtype = cls.read_type(f, user_type_map)
            valsubtype = cls.read_type(f, user_type_map)
            typeclass = typeclass.apply_parameters((keysubtype, valsubtype))
        elif typeclass == TupleType:
            num_items = read_short(f)
            types = tuple(cls.read_type(f, user_type_map) for _ in range(num_items))
            typeclass = typeclass.apply_parameters(types)
        elif typeclass == UserType:
            ks = read_string(f)
            udt_name = read_string(f)
            num_fields = read_short(f)
            names, types = zip(*((read_string(f), cls.read_type(f, user_type_map))
                                 for _ in range(num_fields)))
            specialized_type = typeclass.make_udt_class(ks, udt_name, names, types)
            specialized_type.mapped_class = user_type_map.get(ks, {}).get(udt_name)
            typeclass = specialized_type
        elif typeclass == CUSTOM_TYPE:
            classname = read_string(f)
            typeclass = lookup_casstype(classname)

        return typeclass

    @staticmethod
    def recv_row(f, colcount):
        return [read_value(f) for _ in range(colcount)]


class PrepareMessage(_MessageType):
    opcode = 0x09
    name = 'PREPARE'

    def __init__(self, query, keyspace=None):
        self.query = query
        self.keyspace = keyspace

    def send_body(self, f, protocol_version):
        write_longstring(f, self.query)

        flags = 0x00

        if self.keyspace is not None:
            if ProtocolVersion.uses_keyspace_flag(protocol_version):
                flags |= _PREPARED_WITH_KEYSPACE_FLAG
            else:
                raise UnsupportedOperation(
                    "Keyspaces may only be set on queries with protocol version "
                    "5 or DSE_V2 or higher. Consider setting Cluster.protocol_version.")

        if ProtocolVersion.uses_prepare_flags(protocol_version):
            write_uint(f, flags)
        else:
            # checks above should prevent this, but just to be safe...
            if flags:
                raise UnsupportedOperation(
                    "Attempted to set flags with value {flags:0=#8x} on"
                    "protocol version {pv}, which doesn't support flags"
                    "in prepared statements."
                    "Consider setting Cluster.protocol_version to 5 or DSE_V2."
                    "".format(flags=flags, pv=protocol_version))

        if ProtocolVersion.uses_keyspace_flag(protocol_version):
            if self.keyspace:
                write_string(f, self.keyspace)


class BatchMessage(_MessageType):
    opcode = 0x0D
    name = 'BATCH'

    def __init__(self, batch_type, queries, consistency_level,
                 serial_consistency_level=None, timestamp=None,
                 keyspace=None):
        self.batch_type = batch_type
        self.queries = queries
        self.consistency_level = consistency_level
        self.serial_consistency_level = serial_consistency_level
        self.timestamp = timestamp
        self.keyspace = keyspace

    def send_body(self, f, protocol_version):
        write_byte(f, self.batch_type.value)
        write_short(f, len(self.queries))
        for prepared, string_or_query_id, params in self.queries:
            if not prepared:
                write_byte(f, 0)
                write_longstring(f, string_or_query_id)
            else:
                write_byte(f, 1)
                write_short(f, len(string_or_query_id))
                f.write(string_or_query_id)
            write_short(f, len(params))
            for param in params:
                write_value(f, param)

        write_consistency_level(f, self.consistency_level)
        if protocol_version >= 3:
            flags = 0
            if self.serial_consistency_level:
                flags |= _WITH_SERIAL_CONSISTENCY_FLAG
            if self.timestamp is not None:
                flags |= _PROTOCOL_TIMESTAMP_FLAG
            if self.keyspace:
                if ProtocolVersion.uses_keyspace_flag(protocol_version):
                    flags |= _WITH_KEYSPACE_FLAG
                else:
                    raise UnsupportedOperation(
                        "Keyspaces may only be set on queries with protocol version "
                        "5 or higher. Consider setting Cluster.protocol_version to 5.")

            if ProtocolVersion.uses_int_query_flags(protocol_version):
                write_int(f, flags)
            else:
                write_byte(f, flags)

            if self.serial_consistency_level:
                write_consistency_level(f, self.serial_consistency_level)
            if self.timestamp is not None:
                write_long(f, self.timestamp)

            if ProtocolVersion.uses_keyspace_flag(protocol_version):
                if self.keyspace is not None:
                    write_string(f, self.keyspace)


known_event_types = frozenset((
    'TOPOLOGY_CHANGE',
    'STATUS_CHANGE',
    'SCHEMA_CHANGE'
))


class RegisterMessage(_MessageType):
    opcode = 0x0B
    name = 'REGISTER'

    def __init__(self, event_list):
        self.event_list = event_list

    def send_body(self, f, protocol_version):
        write_stringlist(f, self.event_list)


class EventMessage(_MessageType):
    opcode = 0x0C
    name = 'EVENT'

    def __init__(self, event_type, event_args):
        self.event_type = event_type
        self.event_args = event_args

    @classmethod
    def recv_body(cls, f, protocol_version, *args):
        event_type = read_string(f).upper()
        if event_type in known_event_types:
            read_method = getattr(cls, 'recv_' + event_type.lower())
            return cls(event_type=event_type, event_args=read_method(f, protocol_version))
        raise NotSupportedError('Unknown event type %r' % event_type)

    @classmethod
    def recv_topology_change(cls, f, protocol_version):
        # "NEW_NODE" or "REMOVED_NODE"
        change_type = read_string(f)
        address = read_inet(f)
        return dict(change_type=change_type, address=address)

    @classmethod
    def recv_status_change(cls, f, protocol_version):
        # "UP" or "DOWN"
        change_type = read_string(f)
        address = read_inet(f)
        return dict(change_type=change_type, address=address)

    @classmethod
    def recv_schema_change(cls, f, protocol_version):
        # "CREATED", "DROPPED", or "UPDATED"
        change_type = read_string(f)
        if protocol_version >= 3:
            target = read_string(f)
            keyspace = read_string(f)
            event = {'target_type': target, 'change_type': change_type, 'keyspace': keyspace}
            if target != SchemaTargetType.KEYSPACE:
                target_name = read_string(f)
                if target == SchemaTargetType.FUNCTION:
                    event['function'] = UserFunctionDescriptor(target_name, [read_string(f) for _ in range(read_short(f))])
                elif target == SchemaTargetType.AGGREGATE:
                    event['aggregate'] = UserAggregateDescriptor(target_name, [read_string(f) for _ in range(read_short(f))])
                else:
                    event[target.lower()] = target_name
        else:
            keyspace = read_string(f)
            table = read_string(f)
            if table:
                event = {'target_type': SchemaTargetType.TABLE, 'change_type': change_type, 'keyspace': keyspace, 'table': table}
            else:
                event = {'target_type': SchemaTargetType.KEYSPACE, 'change_type': change_type, 'keyspace': keyspace}
        return event


class ReviseRequestMessage(_MessageType):

    class RevisionType(object):
        PAGING_CANCEL = 1
        PAGING_BACKPRESSURE = 2

    opcode = 0xFF
    name = 'REVISE_REQUEST'

    def __init__(self, op_type, op_id, next_pages=0):
        self.op_type = op_type
        self.op_id = op_id
        self.next_pages = next_pages

    def send_body(self, f, protocol_version):
        write_int(f, self.op_type)
        write_int(f, self.op_id)
        if self.op_type == ReviseRequestMessage.RevisionType.PAGING_BACKPRESSURE:
            if self.next_pages <= 0:
                raise UnsupportedOperation("Continuous paging backpressure requires next_pages > 0")
            elif not ProtocolVersion.has_continuous_paging_next_pages(protocol_version):
                raise UnsupportedOperation(
                    "Continuous paging backpressure may only be used with protocol version "
                    "ProtocolVersion.DSE_V2 or higher. Consider setting Cluster.protocol_version to ProtocolVersion.DSE_V2.")
            else:
                write_int(f, self.next_pages)


class _ProtocolHandler(object):
    """
    _ProtocolHander handles encoding and decoding messages.

    This class can be specialized to compose Handlers which implement alternative
    result decoding or type deserialization. Class definitions are passed to :class:`cassandra.cluster.Cluster`
    on initialization.

    Contracted class methods are :meth:`_ProtocolHandler.encode_message` and :meth:`_ProtocolHandler.decode_message`.
    """

    message_types_by_opcode = _message_types_by_opcode.copy()
    """
    Default mapping of opcode to Message implementation. The default ``decode_message`` implementation uses
    this to instantiate a message and populate using ``recv_body``. This mapping can be updated to inject specialized
    result decoding implementations.
    """

    column_encryption_policy = None
    """Instance of :class:`cassandra.policies.ColumnEncryptionPolicy` in use by this handler"""

    @classmethod
    def encode_message(cls, msg, stream_id, protocol_version, compressor, allow_beta_protocol_version):
        """
        Encodes a message using the specified frame parameters, and compressor

        :param msg: the message, typically of cassandra.protocol._MessageType, generated by the driver
        :param stream_id: protocol stream id for the frame header
        :param protocol_version: version for the frame header, and used encoding contents
        :param compressor: optional compression function to be used on the body
        """
        flags = 0
        body = io.BytesIO()
        if msg.custom_payload:
            if protocol_version < 4:
                raise UnsupportedOperation("Custom key/value payloads can only be used with protocol version 4 or higher")
            flags |= CUSTOM_PAYLOAD_FLAG
            write_bytesmap(body, msg.custom_payload)
        msg.send_body(body, protocol_version)
        body = body.getvalue()

        # With checksumming, the compression is done at the segment frame encoding
        if (not ProtocolVersion.has_checksumming_support(protocol_version)
                and compressor and len(body) > 0):
            body = compressor(body)
            flags |= COMPRESSED_FLAG

        if msg.tracing:
            flags |= TRACING_FLAG

        if allow_beta_protocol_version:
            flags |= USE_BETA_FLAG

        buff = io.BytesIO()
        cls._write_header(buff, protocol_version, flags, stream_id, msg.opcode, len(body))
        buff.write(body)

        return buff.getvalue()

    @staticmethod
    def _write_header(f, version, flags, stream_id, opcode, length):
        """
        Write a CQL protocol frame header.
        """
        pack = v3_header_pack if version >= 3 else header_pack
        f.write(pack(version, flags, stream_id, opcode))
        write_int(f, length)

    @classmethod
    def decode_message(cls, protocol_version, user_type_map, stream_id, flags, opcode, body,
                       decompressor, result_metadata):
        """
        Decodes a native protocol message body

        :param protocol_version: version to use decoding contents
        :param user_type_map: map[keyspace name] = map[type name] = custom type to instantiate when deserializing this type
        :param stream_id: native protocol stream id from the frame header
        :param flags: native protocol flags bitmap from the header
        :param opcode: native protocol opcode from the header
        :param body: frame body
        :param decompressor: optional decompression function to inflate the body
        :return: a message decoded from the body and frame attributes
        """
        if (not ProtocolVersion.has_checksumming_support(protocol_version) and
                flags & COMPRESSED_FLAG):
            if decompressor is None:
                raise RuntimeError("No de-compressor available for compressed frame!")
            body = decompressor(body)
            flags ^= COMPRESSED_FLAG

        body = io.BytesIO(body)
        if flags & TRACING_FLAG:
            trace_id = UUID(bytes=body.read(16))
            flags ^= TRACING_FLAG
        else:
            trace_id = None

        if flags & WARNING_FLAG:
            warnings = read_stringlist(body)
            flags ^= WARNING_FLAG
        else:
            warnings = None

        if flags & CUSTOM_PAYLOAD_FLAG:
            custom_payload = read_bytesmap(body)
            flags ^= CUSTOM_PAYLOAD_FLAG
        else:
            custom_payload = None

        flags &= USE_BETA_MASK  # will only be set if we asserted it in connection estabishment

        if flags:
            log.warning("Unknown protocol flags set: %02x. May cause problems.", flags)

        msg_class = cls.message_types_by_opcode[opcode]
        msg = msg_class.recv_body(body, protocol_version, user_type_map, result_metadata, cls.column_encryption_policy)
        msg.stream_id = stream_id
        msg.trace_id = trace_id
        msg.custom_payload = custom_payload
        msg.warnings = warnings

        if msg.warnings:
            for w in msg.warnings:
                log.warning("Server warning: %s", w)

        return msg


def cython_protocol_handler(colparser):
    """
    Given a column parser to deserialize ResultMessages, return a suitable
    Cython-based protocol handler.

    There are three Cython-based protocol handlers:

        - obj_parser.ListParser
            decodes result messages into a list of tuples

        - obj_parser.LazyParser
            decodes result messages lazily by returning an iterator

        - numpy_parser.NumPyParser
            decodes result messages into NumPy arrays

    The default is to use obj_parser.ListParser
    """
    from cassandra.row_parser import make_recv_results_rows

    class FastResultMessage(ResultMessage):
        """
        Cython version of Result Message that has a faster implementation of
        recv_results_row.
        """
        # type_codes = ResultMessage.type_codes.copy()
        code_to_type = dict((v, k) for k, v in ResultMessage.type_codes.items())
        recv_results_rows = make_recv_results_rows(colparser)

    class CythonProtocolHandler(_ProtocolHandler):
        """
        Use FastResultMessage to decode query result message messages.
        """

        my_opcodes = _ProtocolHandler.message_types_by_opcode.copy()
        my_opcodes[FastResultMessage.opcode] = FastResultMessage
        message_types_by_opcode = my_opcodes

        col_parser = colparser

    return CythonProtocolHandler


if HAVE_CYTHON:
    from cassandra.obj_parser import ListParser, LazyParser
    ProtocolHandler = cython_protocol_handler(ListParser())
    LazyProtocolHandler = cython_protocol_handler(LazyParser())
else:
    # Use Python-based ProtocolHandler
    ProtocolHandler = _ProtocolHandler
    LazyProtocolHandler = None


if HAVE_CYTHON and HAVE_NUMPY:
    from cassandra.numpy_parser import NumpyParser
    NumpyProtocolHandler = cython_protocol_handler(NumpyParser())
else:
    NumpyProtocolHandler = None


def read_byte(f):
    return int8_unpack(f.read(1))


def write_byte(f, b):
    f.write(uint8_pack(b))


def read_int(f):
    return int32_unpack(f.read(4))


def read_uint_le(f, size=4):
    """
    Read a sequence of little endian bytes and return an unsigned integer.
    """

    if size == 4:
        value = uint32_le_unpack(f.read(4))
    else:
        value = 0
        for i in range(size):
            value |= (read_byte(f) & 0xFF) << 8 * i

    return value


def write_uint_le(f, i, size=4):
    """
    Write an unsigned integer on a sequence of little endian bytes.
    """
    if size == 4:
        f.write(uint32_le_pack(i))
    else:
        for j in range(size):
            shift = j * 8
            write_byte(f, i >> shift & 0xFF)


def write_int(f, i):
    f.write(int32_pack(i))


def write_uint(f, i):
    f.write(uint32_pack(i))


def write_long(f, i):
    f.write(uint64_pack(i))


def read_short(f):
    return uint16_unpack(f.read(2))


def write_short(f, s):
    f.write(uint16_pack(s))


def read_consistency_level(f):
    return read_short(f)


def write_consistency_level(f, cl):
    write_short(f, cl)


def read_string(f):
    size = read_short(f)
    contents = f.read(size)
    return contents.decode('utf8')


def read_binary_string(f):
    size = read_short(f)
    contents = f.read(size)
    return contents


def write_string(f, s):
    if isinstance(s, str):
        s = s.encode('utf8')
    write_short(f, len(s))
    f.write(s)


def read_binary_longstring(f):
    size = read_int(f)
    contents = f.read(size)
    return contents


def read_longstring(f):
    return read_binary_longstring(f).decode('utf8')


def write_longstring(f, s):
    if isinstance(s, str):
        s = s.encode('utf8')
    write_int(f, len(s))
    f.write(s)


def read_stringlist(f):
    numstrs = read_short(f)
    return [read_string(f) for _ in range(numstrs)]


def write_stringlist(f, stringlist):
    write_short(f, len(stringlist))
    for s in stringlist:
        write_string(f, s)


def read_stringmap(f):
    numpairs = read_short(f)
    strmap = {}
    for _ in range(numpairs):
        k = read_string(f)
        strmap[k] = read_string(f)
    return strmap


def write_stringmap(f, strmap):
    write_short(f, len(strmap))
    for k, v in strmap.items():
        write_string(f, k)
        write_string(f, v)


def read_bytesmap(f):
    numpairs = read_short(f)
    bytesmap = {}
    for _ in range(numpairs):
        k = read_string(f)
        bytesmap[k] = read_value(f)
    return bytesmap


def write_bytesmap(f, bytesmap):
    write_short(f, len(bytesmap))
    for k, v in bytesmap.items():
        write_string(f, k)
        write_value(f, v)


def read_stringmultimap(f):
    numkeys = read_short(f)
    strmmap = {}
    for _ in range(numkeys):
        k = read_string(f)
        strmmap[k] = read_stringlist(f)
    return strmmap


def write_stringmultimap(f, strmmap):
    write_short(f, len(strmmap))
    for k, v in strmmap.items():
        write_string(f, k)
        write_stringlist(f, v)


def read_error_code_map(f):
    numpairs = read_int(f)
    error_code_map = {}
    for _ in range(numpairs):
        endpoint = read_inet_addr_only(f)
        error_code_map[endpoint] = read_short(f)
    return error_code_map


def read_value(f):
    size = read_int(f)
    if size < 0:
        return None
    return f.read(size)


def write_value(f, v):
    if v is None:
        write_int(f, -1)
    elif v is _UNSET_VALUE:
        write_int(f, -2)
    else:
        write_int(f, len(v))
        f.write(v)


def read_inet_addr_only(f):
    size = read_byte(f)
    addrbytes = f.read(size)
    if size == 4:
        addrfam = socket.AF_INET
    elif size == 16:
        addrfam = socket.AF_INET6
    else:
        raise InternalError("bad inet address: %r" % (addrbytes,))
    return util.inet_ntop(addrfam, addrbytes)


def read_inet(f):
    addr = read_inet_addr_only(f)
    port = read_int(f)
    return (addr, port)


def write_inet(f, addrtuple):
    addr, port = addrtuple
    if ':' in addr:
        addrfam = socket.AF_INET6
    else:
        addrfam = socket.AF_INET
    addrbytes = util.inet_pton(addrfam, addr)
    write_byte(f, len(addrbytes))
    f.write(addrbytes)
    write_int(f, port)
