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

include 'cython_marshal.pyx'
from cassandra.buffer cimport Buffer, from_ptr_and_size

from libc.stdint cimport int32_t
from cassandra.bytesio cimport BytesIOReader


cdef inline int get_buf(BytesIOReader reader, Buffer *buf_out) except -1:
    """
    Get a pointer into the buffer provided by BytesIOReader for the
    next data item in the stream of values.

    BEWARE:
        If the next item has a zero negative size, the pointer will be set to NULL.
        A negative size happens when the value is NULL in the database, whereas a
        zero size may happen either for legacy reasons, or for data types such as
        strings (which may be empty).
    """
    cdef Py_ssize_t raw_val_size = read_int(reader)
    cdef char *ptr
    if raw_val_size <= 0:
        ptr = NULL
    else:
        ptr = reader.read(raw_val_size)

    from_ptr_and_size(ptr, raw_val_size, buf_out)
    return 0

cdef inline int32_t read_int(BytesIOReader reader) except ?0xDEAD:
    cdef Buffer buf
    buf.ptr = reader.read(4)
    buf.size = 4
    return unpack_num[int32_t](&buf)
