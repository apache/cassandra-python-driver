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


from libc.stdint cimport int32_t, uint16_t

include 'cython_marshal.pyx'
from cassandra.buffer cimport Buffer, to_bytes, slice_buffer
from cassandra.cython_utils cimport datetime_from_timestamp

from cython.view cimport array as cython_array
from cassandra.tuple cimport tuple_new, tuple_set

import socket
from decimal import Decimal
from uuid import UUID

from cassandra import cqltypes
from cassandra import util

cdef class Deserializer:
    """Cython-based deserializer class for a cqltype"""

    def __init__(self, cqltype):
        self.cqltype = cqltype
        self.empty_binary_ok = cqltype.empty_binary_ok

    cdef deserialize(self, Buffer *buf, int protocol_version):
        raise NotImplementedError


cdef class DesBytesType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        if buf.size == 0:
            return b""
        return to_bytes(buf)

# this is to facilitate cqlsh integration, which requires bytearrays for BytesType
# It is switched in by simply overwriting DesBytesType:
# deserializers.DesBytesType = deserializers.DesBytesTypeByteArray
cdef class DesBytesTypeByteArray(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        if buf.size == 0:
            return bytearray()
        return bytearray(buf.ptr[:buf.size])

# TODO: Use libmpdec: http://www.bytereef.org/mpdecimal/index.html
cdef class DesDecimalType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef Buffer varint_buf
        slice_buffer(buf, &varint_buf, 4, buf.size - 4)

        cdef int32_t scale = unpack_num[int32_t](buf)
        unscaled = varint_unpack(&varint_buf)

        return Decimal('%de%d' % (unscaled, -scale))


cdef class DesUUIDType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return UUID(bytes=to_bytes(buf))


cdef class DesBooleanType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        if unpack_num[int8_t](buf):
            return True
        return False


cdef class DesByteType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return unpack_num[int8_t](buf)


cdef class DesAsciiType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        if buf.size == 0:
            return ""
        return to_bytes(buf).decode('ascii')


cdef class DesFloatType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return unpack_num[float](buf)


cdef class DesDoubleType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return unpack_num[double](buf)


cdef class DesLongType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return unpack_num[int64_t](buf)


cdef class DesInt32Type(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return unpack_num[int32_t](buf)


cdef class DesIntegerType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return varint_unpack(buf)


cdef class DesInetAddressType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef bytes byts = to_bytes(buf)

        # TODO: optimize inet_ntop, inet_ntoa
        if buf.size == 16:
            return util.inet_ntop(socket.AF_INET6, byts)
        else:
            # util.inet_pton could also handle, but this is faster
            # since we've already determined the AF
            return socket.inet_ntoa(byts)


cdef class DesCounterColumnType(DesLongType):
    pass


cdef class DesDateType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef double timestamp = unpack_num[int64_t](buf) / 1000.0
        return datetime_from_timestamp(timestamp)


cdef class TimestampType(DesDateType):
    pass


cdef class TimeUUIDType(DesDateType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return UUID(bytes=to_bytes(buf))


# Values of the 'date'` type are encoded as 32-bit unsigned integers
# representing a number of days with epoch (January 1st, 1970) at the center of the
# range (2^31).
EPOCH_OFFSET_DAYS = 2 ** 31

cdef class DesSimpleDateType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        days = unpack_num[uint32_t](buf) - EPOCH_OFFSET_DAYS
        return util.Date(days)


cdef class DesShortType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return unpack_num[int16_t](buf)


cdef class DesTimeType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return util.Time(unpack_num[int64_t](buf))


cdef class DesUTF8Type(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        if buf.size == 0:
            return ""
        cdef val = to_bytes(buf)
        return val.decode('utf8')


cdef class DesVarcharType(DesUTF8Type):
    pass


cdef class _DesParameterizedType(Deserializer):

    cdef object subtypes
    cdef Deserializer[::1] deserializers
    cdef Py_ssize_t subtypes_len

    def __init__(self, cqltype):
        super().__init__(cqltype)
        self.subtypes = cqltype.subtypes
        self.deserializers = make_deserializers(cqltype.subtypes)
        self.subtypes_len = len(self.subtypes)


cdef class _DesSingleParamType(_DesParameterizedType):
    cdef Deserializer deserializer

    def __init__(self, cqltype):
        assert cqltype.subtypes and len(cqltype.subtypes) == 1, cqltype.subtypes
        super().__init__(cqltype)
        self.deserializer = self.deserializers[0]


#--------------------------------------------------------------------------
# List and set deserialization

cdef class DesListType(_DesSingleParamType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef uint16_t v2_and_below = 2
        cdef int32_t v3_and_above = 3

        if protocol_version >= 3:
            result = _deserialize_list_or_set[int32_t](
                v3_and_above, buf, protocol_version, self.deserializer)
        else:
            result = _deserialize_list_or_set[uint16_t](
                v2_and_below, buf, protocol_version, self.deserializer)

        return result

cdef class DesSetType(DesListType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return util.sortedset(DesListType.deserialize(self, buf, protocol_version))


ctypedef fused itemlen_t:
    uint16_t # protocol <= v2
    int32_t  # protocol >= v3

cdef list _deserialize_list_or_set(itemlen_t dummy_version,
                                   Buffer *buf, int protocol_version,
                                   Deserializer deserializer):
    """
    Deserialize a list or set.

    The 'dummy' parameter is needed to make fused types work, so that
    we can specialize on the protocol version.
    """
    cdef Buffer itemlen_buf
    cdef Buffer elem_buf

    cdef itemlen_t numelements
    cdef int offset
    cdef list result = []

    _unpack_len[itemlen_t](buf, 0, &numelements)
    offset = sizeof(itemlen_t)
    protocol_version = max(3, protocol_version)
    for _ in range(numelements):
        subelem[itemlen_t](buf, &elem_buf, &offset, dummy_version)
        result.append(from_binary(deserializer, &elem_buf, protocol_version))

    return result


cdef inline int subelem(
        Buffer *buf, Buffer *elem_buf, int* offset, itemlen_t dummy) except -1:
    """
    Read the next element from the buffer: first read the size (in bytes) of the
    element, then fill elem_buf with a newly sliced buffer of this size (and the
    right offset).
    """
    cdef itemlen_t elemlen

    _unpack_len[itemlen_t](buf, offset[0], &elemlen)
    offset[0] += sizeof(itemlen_t)
    slice_buffer(buf, elem_buf, offset[0], elemlen)
    offset[0] += elemlen
    return 0


cdef int _unpack_len(Buffer *buf, int offset, itemlen_t *output) except -1:
    cdef Buffer itemlen_buf
    slice_buffer(buf, &itemlen_buf, offset, sizeof(itemlen_t))

    if itemlen_t is uint16_t:
        output[0] = unpack_num[uint16_t](&itemlen_buf)
    else:
        output[0] = unpack_num[int32_t](&itemlen_buf)

    return 0

#--------------------------------------------------------------------------
# Map deserialization

cdef class DesMapType(_DesParameterizedType):

    cdef Deserializer key_deserializer, val_deserializer

    def __init__(self, cqltype):
        super().__init__(cqltype)
        self.key_deserializer = self.deserializers[0]
        self.val_deserializer = self.deserializers[1]

    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef uint16_t v2_and_below = 0
        cdef int32_t v3_and_above = 0
        key_type, val_type = self.cqltype.subtypes

        if protocol_version >= 3:
            result = _deserialize_map[int32_t](
                v3_and_above, buf, protocol_version,
                self.key_deserializer, self.val_deserializer,
                key_type, val_type)
        else:
            result = _deserialize_map[uint16_t](
                v2_and_below, buf, protocol_version,
                self.key_deserializer, self.val_deserializer,
                key_type, val_type)

        return result


cdef _deserialize_map(itemlen_t dummy_version,
                      Buffer *buf, int protocol_version,
                      Deserializer key_deserializer, Deserializer val_deserializer,
                      key_type, val_type):
    cdef Buffer key_buf, val_buf
    cdef Buffer itemlen_buf

    cdef itemlen_t numelements
    cdef int offset
    cdef list result = []

    _unpack_len[itemlen_t](buf, 0, &numelements)
    offset = sizeof(itemlen_t)
    themap = util.OrderedMapSerializedKey(key_type, protocol_version)
    protocol_version = max(3, protocol_version)
    for _ in range(numelements):
        subelem[itemlen_t](buf, &key_buf, &offset, dummy_version)
        subelem[itemlen_t](buf, &val_buf, &offset, numelements)
        key = from_binary(key_deserializer, &key_buf, protocol_version)
        val = from_binary(val_deserializer, &val_buf, protocol_version)
        themap._insert_unchecked(key, to_bytes(&key_buf), val)

    return themap

#--------------------------------------------------------------------------

cdef class DesTupleType(_DesParameterizedType):

    # TODO: Use TupleRowParser to parse these tuples

    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef Py_ssize_t i, p
        cdef int32_t itemlen
        cdef tuple res = tuple_new(self.subtypes_len)
        cdef Buffer item_buf
        cdef Buffer itemlen_buf
        cdef Deserializer deserializer

        # collections inside UDTs are always encoded with at least the
        # version 3 format
        protocol_version = max(3, protocol_version)

        p = 0
        values = []
        for i in range(self.subtypes_len):
            item = None
            if p < buf.size:
                slice_buffer(buf, &itemlen_buf, p, 4)
                itemlen = unpack_num[int32_t](&itemlen_buf)
                p += 4
                if itemlen >= 0:
                    slice_buffer(buf, &item_buf, p, itemlen)
                    p += itemlen

                    deserializer = self.deserializers[i]
                    item = from_binary(deserializer, &item_buf, protocol_version)

            tuple_set(res, i, item)

        return res


cdef class DesUserType(DesTupleType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        typ = self.cqltype
        values = DesTupleType.deserialize(self, buf, protocol_version)
        if typ.mapped_class:
            return typ.mapped_class(**dict(zip(typ.fieldnames, values)))
        elif typ.tuple_type:
            return typ.tuple_type(*values)
        else:
            return tuple(values)


cdef class DesCompositeType(_DesParameterizedType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef Py_ssize_t i, idx, start
        cdef Buffer elem_buf
        cdef int16_t element_length
        cdef Deserializer deserializer
        cdef tuple res = tuple_new(self.subtypes_len)

        idx = 0
        for i in range(self.subtypes_len):
            if not buf.size:
                # CompositeType can have missing elements at the end

                # Fill the tuple with None values and slice it
                #
                # (I'm not sure a tuple needs to be fully initialized before
                #  it can be destroyed, so play it safe)
                for j in range(i, self.subtypes_len):
                    tuple_set(res, j, None)
                res = res[:i]
                break

            element_length = unpack_num[uint16_t](buf)
            slice_buffer(buf, &elem_buf, 2, element_length)

            deserializer = self.deserializers[i]
            item = from_binary(deserializer, &elem_buf, protocol_version)
            tuple_set(res, i, item)

            # skip element length, element, and the EOC (one byte)
            start = 2 + element_length + 1
            slice_buffer(buf, buf, start, buf.size - start)

        return res


DesDynamicCompositeType = DesCompositeType


cdef class DesReversedType(_DesSingleParamType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return from_binary(self.deserializer, buf, protocol_version)


cdef class DesFrozenType(_DesSingleParamType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return from_binary(self.deserializer, buf, protocol_version)

#--------------------------------------------------------------------------

cdef _ret_empty(Deserializer deserializer, Py_ssize_t buf_size):
    """
    Decide whether to return None or EMPTY when a buffer size is
    zero or negative. This is used by from_binary in deserializers.pxd.
    """
    if buf_size < 0:
        return None
    elif deserializer.cqltype.support_empty_values:
        return cqltypes.EMPTY
    else:
        return None

#--------------------------------------------------------------------------
# Generic deserialization

cdef class GenericDeserializer(Deserializer):
    """
    Wrap a generic datatype for deserialization
    """

    cdef deserialize(self, Buffer *buf, int protocol_version):
        return self.cqltype.deserialize(to_bytes(buf), protocol_version)

    def __repr__(self):
        return "GenericDeserializer(%s)" % (self.cqltype,)

#--------------------------------------------------------------------------
# Helper utilities

def make_deserializers(cqltypes):
    """Create an array of Deserializers for each given cqltype in cqltypes"""
    cdef Deserializer[::1] deserializers
    return obj_array([find_deserializer(ct) for ct in cqltypes])


cdef dict classes = globals()

cpdef Deserializer find_deserializer(cqltype):
    """Find a deserializer for a cqltype"""
    name = 'Des' + cqltype.__name__

    if name in globals():
        cls = classes[name]
    elif issubclass(cqltype, cqltypes.ListType):
        cls = DesListType
    elif issubclass(cqltype, cqltypes.SetType):
        cls = DesSetType
    elif issubclass(cqltype, cqltypes.MapType):
        cls = DesMapType
    elif issubclass(cqltype, cqltypes.UserType):
        # UserType is a subclass of TupleType, so should precede it
        cls = DesUserType
    elif issubclass(cqltype, cqltypes.TupleType):
        cls = DesTupleType
    elif issubclass(cqltype, cqltypes.DynamicCompositeType):
        # DynamicCompositeType is a subclass of CompositeType, so should precede it
        cls = DesDynamicCompositeType
    elif issubclass(cqltype, cqltypes.CompositeType):
        cls = DesCompositeType
    elif issubclass(cqltype, cqltypes.ReversedType):
        cls = DesReversedType
    elif issubclass(cqltype, cqltypes.FrozenType):
        cls = DesFrozenType
    else:
        cls = GenericDeserializer

    return cls(cqltype)


def obj_array(list objs):
    """Create a (Cython) array of objects given a list of objects"""
    cdef object[:] arr
    cdef Py_ssize_t i
    arr = cython_array(shape=(len(objs),), itemsize=sizeof(void *), format="O")
    # arr[:] = objs # This does not work (segmentation faults)
    for i, obj in enumerate(objs):
        arr[i] = obj
    return arr
