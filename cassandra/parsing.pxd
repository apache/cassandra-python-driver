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

from cassandra.bytesio cimport BytesIOReader
from cassandra.deserializers cimport Deserializer

cdef class ParseDesc:
    cdef public object colnames
    cdef public object coltypes
    cdef public object column_encryption_policy
    cdef public list coldescs
    cdef Deserializer[::1] deserializers
    cdef public int protocol_version
    cdef Py_ssize_t rowsize

cdef class ColumnParser:
    cpdef parse_rows(self, BytesIOReader reader, ParseDesc desc)

cdef class RowParser:
    cpdef unpack_row(self, BytesIOReader reader, ParseDesc desc)

