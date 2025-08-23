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

from cassandra.buffer cimport Buffer

cdef class Deserializer:
    # The cqltypes._CassandraType corresponding to this deserializer
    cdef object cqltype

    # String may be empty, whereas other values may not be.
    # Other values may be NULL, in which case the integer length
    # of the binary data is negative. However, non-string types
    # may also return a zero length for legacy reasons
    # (see http://code.metager.de/source/xref/apache/cassandra/doc/native_protocol_v3.spec
    # paragraph 6)
    cdef bint empty_binary_ok

    cdef deserialize(self, Buffer *buf, int protocol_version)
    # cdef deserialize(self, CString byts, protocol_version)


cdef inline object from_binary(Deserializer deserializer,
                               Buffer *buf,
                               int protocol_version):
    if buf.size < 0:
        return None
    elif buf.size == 0 and not deserializer.empty_binary_ok:
        return _ret_empty(deserializer, buf.size)
    else:
        return deserializer.deserialize(buf, protocol_version)

cdef _ret_empty(Deserializer deserializer, Py_ssize_t buf_size)
