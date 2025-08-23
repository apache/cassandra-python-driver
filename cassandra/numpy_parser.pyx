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
This module provides an optional protocol parser that returns
NumPy arrays.

=============================================================================
This module should not be imported by any of the main python-driver modules,
as numpy is an optional dependency.
=============================================================================
"""

include "ioutils.pyx"

cimport cython
from libc.stdint cimport uint64_t, uint8_t
from cpython.ref cimport Py_INCREF, PyObject

from cassandra.bytesio cimport BytesIOReader
from cassandra.deserializers cimport Deserializer, from_binary
from cassandra.parsing cimport ParseDesc, ColumnParser, RowParser
from cassandra import cqltypes
from cassandra.util import is_little_endian

import numpy as np

cdef extern from "numpyFlags.h":
    # Include 'numpyFlags.h' into the generated C code to disable the
    # deprecated NumPy API
    pass

cdef extern from "Python.h":
    # An integer type large enough to hold a pointer
    ctypedef uint64_t Py_uintptr_t


# Simple array descriptor, useful to parse rows into a NumPy array
ctypedef struct ArrDesc:
    Py_uintptr_t buf_ptr
    int stride # should be large enough as we allocate contiguous arrays
    int is_object
    Py_uintptr_t mask_ptr

arrDescDtype = np.dtype(
    [ ('buf_ptr', np.uintp)
    , ('stride', np.dtype('i'))
    , ('is_object', np.dtype('i'))
    , ('mask_ptr', np.uintp)
    ], align=True)

_cqltype_to_numpy = {
    cqltypes.LongType:          np.dtype('>i8'),
    cqltypes.CounterColumnType: np.dtype('>i8'),
    cqltypes.Int32Type:         np.dtype('>i4'),
    cqltypes.ShortType:         np.dtype('>i2'),
    cqltypes.FloatType:         np.dtype('>f4'),
    cqltypes.DoubleType:        np.dtype('>f8'),
}

obj_dtype = np.dtype('O')

cdef uint8_t mask_true = 0x01

cdef class NumpyParser(ColumnParser):
    """Decode a ResultMessage into a bunch of NumPy arrays"""

    cpdef parse_rows(self, BytesIOReader reader, ParseDesc desc):
        cdef Py_ssize_t rowcount
        cdef ArrDesc[::1] array_descs
        cdef ArrDesc *arrs

        rowcount = read_int(reader)
        array_descs, arrays = make_arrays(desc, rowcount)
        arrs = &array_descs[0]

        _parse_rows(reader, desc, arrs, rowcount)

        arrays = [make_native_byteorder(arr) for arr in arrays]
        result = dict(zip(desc.colnames, arrays))
        return result


cdef _parse_rows(BytesIOReader reader, ParseDesc desc,
                 ArrDesc *arrs, Py_ssize_t rowcount):
    cdef Py_ssize_t i

    for i in range(rowcount):
        unpack_row(reader, desc, arrs)


### Helper functions to create NumPy arrays and array descriptors

def make_arrays(ParseDesc desc, array_size):
    """
    Allocate arrays for each result column.

    returns a tuple of (array_descs, arrays), where
        'array_descs' describe the arrays for NativeRowParser and
        'arrays' is a dict mapping column names to arrays
            (e.g. this can be fed into pandas.DataFrame)
    """
    array_descs = np.empty((desc.rowsize,), arrDescDtype)
    arrays = []

    for i, coltype in enumerate(desc.coltypes):
        arr = make_array(coltype, array_size)
        array_descs[i]['buf_ptr'] = arr.ctypes.data
        array_descs[i]['stride'] = arr.strides[0]
        array_descs[i]['is_object'] = arr.dtype is obj_dtype
        try:
            array_descs[i]['mask_ptr'] = arr.mask.ctypes.data
        except AttributeError:
            array_descs[i]['mask_ptr'] = 0
        arrays.append(arr)

    return array_descs, arrays


def make_array(coltype, array_size):
    """
    Allocate a new NumPy array of the given column type and size.
    """
    try:
        a = np.ma.empty((array_size,), dtype=_cqltype_to_numpy[coltype])
        a.mask = np.zeros((array_size,), dtype=bool)
    except KeyError:
        a = np.empty((array_size,), dtype=obj_dtype)
    return a


#### Parse rows into NumPy arrays

@cython.boundscheck(False)
@cython.wraparound(False)
cdef inline int unpack_row(
        BytesIOReader reader, ParseDesc desc, ArrDesc *arrays) except -1:
    cdef Buffer buf
    cdef Py_ssize_t i, rowsize = desc.rowsize
    cdef ArrDesc arr
    cdef Deserializer deserializer
    for i in range(rowsize):
        get_buf(reader, &buf)
        arr = arrays[i]

        if arr.is_object:
            deserializer = desc.deserializers[i]
            val = from_binary(deserializer, &buf, desc.protocol_version)
            Py_INCREF(val)
            (<PyObject **> arr.buf_ptr)[0] = <PyObject *> val
        elif buf.size >= 0:
            memcpy(<char *> arr.buf_ptr, buf.ptr, buf.size)
        else:
            memcpy(<char *>arr.mask_ptr, &mask_true, 1)

        # Update the pointer into the array for the next time
        arrays[i].buf_ptr += arr.stride
        arrays[i].mask_ptr += 1

    return 0


def make_native_byteorder(arr):
    """
    Make sure all values have a native endian in the NumPy arrays.
    """
    if is_little_endian and not arr.dtype.kind == 'O':
        # We have arrays in big-endian order. First swap the bytes
        # into little endian order, and then update the numpy dtype
        # accordingly (e.g. from '>i8' to '<i8')
        #
        # Ignore any object arrays of dtype('O')
        return arr.byteswap().newbyteorder()
    return arr
