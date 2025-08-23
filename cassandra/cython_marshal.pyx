# -- cython: profile=True
#
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

from libc.stdint cimport (int8_t, int16_t, int32_t, int64_t,
                          uint8_t, uint16_t, uint32_t, uint64_t)
from libc.string cimport memcpy
from cassandra.buffer cimport Buffer, buf_read, to_bytes

cdef bint is_little_endian
from cassandra.util import is_little_endian

ctypedef fused num_t:
    int64_t
    int32_t
    int16_t
    int8_t
    uint64_t
    uint32_t
    uint16_t
    uint8_t
    double
    float

cdef inline num_t unpack_num(Buffer *buf, num_t *dummy=NULL): # dummy pointer because cython wants the fused type as an arg
    """
    Copy to aligned destination, conditionally swapping to native byte order
    """
    cdef Py_ssize_t start, end, i
    cdef char *src = buf_read(buf, sizeof(num_t))
    cdef num_t ret = 0
    cdef char *out = <char*> &ret

    if is_little_endian:
        for i in range(sizeof(num_t)):
            out[sizeof(num_t) - i - 1] = src[i]
    else:
        memcpy(out, src, sizeof(num_t))

    return ret

cdef varint_unpack(Buffer *term):
    """Unpack a variable-sized integer"""
    return varint_unpack_py3(to_bytes(term))

# TODO: Optimize these two functions
cdef varint_unpack_py3(bytes term):
    val = int(''.join(["%02x" % i for i in term]), 16)
    if (term[0] & 128) != 0:
        shift = len(term) * 8  # * Note below
        val -= 1 << shift
    return val

# * Note *
# '1 << (len(term) * 8)' Cython tries to do native
# integer shifts, which overflows. We need this to
# emulate Python shifting, which will expand the long
# to accommodate
