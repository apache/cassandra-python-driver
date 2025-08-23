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

include "ioutils.pyx"

from cassandra import DriverException
from cassandra.bytesio cimport BytesIOReader
from cassandra.deserializers cimport Deserializer, from_binary
from cassandra.deserializers import find_deserializer
from cassandra.parsing cimport ParseDesc, ColumnParser, RowParser
from cassandra.tuple cimport tuple_new, tuple_set

from cpython.bytes cimport PyBytes_AsStringAndSize


cdef class ListParser(ColumnParser):
    """Decode a ResultMessage into a list of tuples (or other objects)"""

    cpdef parse_rows(self, BytesIOReader reader, ParseDesc desc):
        cdef Py_ssize_t i, rowcount
        rowcount = read_int(reader)
        cdef RowParser rowparser = TupleRowParser()
        return [rowparser.unpack_row(reader, desc) for i in range(rowcount)]


cdef class LazyParser(ColumnParser):
    """Decode a ResultMessage lazily using a generator"""

    cpdef parse_rows(self, BytesIOReader reader, ParseDesc desc):
        # Use a little helper function as closures (generators) are not
        # supported in cpdef methods
        return parse_rows_lazy(reader, desc)


def parse_rows_lazy(BytesIOReader reader, ParseDesc desc):
    cdef Py_ssize_t i, rowcount
    rowcount = read_int(reader)
    cdef RowParser rowparser = TupleRowParser()
    return (rowparser.unpack_row(reader, desc) for i in range(rowcount))


cdef class TupleRowParser(RowParser):
    """
    Parse a single returned row into a tuple of objects:

        (obj1, ..., objN)
    """

    cpdef unpack_row(self, BytesIOReader reader, ParseDesc desc):
        assert desc.rowsize >= 0

        cdef Buffer buf
        cdef Buffer newbuf
        cdef Py_ssize_t i, rowsize = desc.rowsize
        cdef Deserializer deserializer
        cdef tuple res = tuple_new(desc.rowsize)

        ce_policy = desc.column_encryption_policy
        for i in range(rowsize):
            # Read the next few bytes
            get_buf(reader, &buf)

            # Deserialize bytes to python object
            deserializer = desc.deserializers[i]
            coldesc = desc.coldescs[i]
            uses_ce = ce_policy and ce_policy.contains_column(coldesc)
            try:
                if uses_ce:
                    col_type = ce_policy.column_type(coldesc)
                    decrypted_bytes = ce_policy.decrypt(coldesc, to_bytes(&buf))
                    PyBytes_AsStringAndSize(decrypted_bytes, &newbuf.ptr, &newbuf.size)
                    deserializer = find_deserializer(ce_policy.column_type(coldesc))
                    val = from_binary(deserializer, &newbuf, desc.protocol_version)
                else:
                    val = from_binary(deserializer, &buf, desc.protocol_version)
            except Exception as e:
                raise DriverException('Failed decoding result column "%s" of type %s: %s' % (desc.colnames[i],
                                                                                             desc.coltypes[i].cql_parameterized_type(),
                                                                                             str(e)))
            # Insert new object into tuple
            tuple_set(res, i, val)

        return res
