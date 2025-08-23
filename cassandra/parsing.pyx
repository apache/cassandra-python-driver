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
Module containing the definitions and declarations (parsing.pxd) for parsers.
"""

cdef class ParseDesc:
    """Description of what structure to parse"""

    def __init__(self, colnames, coltypes, column_encryption_policy, coldescs, deserializers, protocol_version):
        self.colnames = colnames
        self.coltypes = coltypes
        self.column_encryption_policy = column_encryption_policy
        self.coldescs = coldescs
        self.deserializers = deserializers
        self.protocol_version = protocol_version
        self.rowsize = len(colnames)


cdef class ColumnParser:
    """Decode a ResultMessage into a set of columns"""

    cpdef parse_rows(self, BytesIOReader reader, ParseDesc desc):
        raise NotImplementedError


cdef class RowParser:
    """Parser for a single row"""

    cpdef unpack_row(self, BytesIOReader reader, ParseDesc desc):
        """
        Unpack a single row of data in a ResultMessage.
        """
        raise NotImplementedError
