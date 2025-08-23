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
Simple buffer data structure that provides a view on existing memory
(e.g. from a bytes object). This memory must stay alive while the
buffer is in use.
"""

from cpython.bytes cimport PyBytes_AS_STRING
    # char* PyBytes_AS_STRING(object string)
    # Macro form of PyBytes_AsString() but without error
    # checking. Only string objects are supported; no Unicode objects
    # should be passed.


cdef struct Buffer:
    char *ptr
    Py_ssize_t size


cdef inline bytes to_bytes(Buffer *buf):
    return buf.ptr[:buf.size]

cdef inline char *buf_ptr(Buffer *buf):
    return buf.ptr

cdef inline char *buf_read(Buffer *buf, Py_ssize_t size) except NULL:
    if size > buf.size:
        raise IndexError("Requested more than length of buffer")
    return buf.ptr

cdef inline int slice_buffer(Buffer *buf, Buffer *out,
                             Py_ssize_t start, Py_ssize_t size) except -1:
    if size < 0:
        raise ValueError("Length must be positive")

    if start + size > buf.size:
        raise IndexError("Buffer slice out of bounds")

    out.ptr = buf.ptr + start
    out.size = size
    return 0

cdef inline void from_ptr_and_size(char *ptr, Py_ssize_t size, Buffer *out):
    out.ptr = ptr
    out.size = size
