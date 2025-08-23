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

import calendar
import datetime
import time

include '../../../cassandra/ioutils.pyx'

import io

from cassandra.cqltypes import DateType
from cassandra.protocol import write_value
from cassandra.deserializers import find_deserializer
from cassandra.bytesio cimport BytesIOReader
from cassandra.buffer cimport Buffer
from cassandra.deserializers cimport from_binary, Deserializer


def test_datetype(assert_equal):

    cdef Deserializer des = find_deserializer(DateType)

    def deserialize(timestamp):
        """Serialize a datetime and deserialize it using the cython deserializer"""

        cdef BytesIOReader reader
        cdef Buffer buf

        dt = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)

        bytes = io.BytesIO()
        write_value(bytes, DateType.serialize(dt, 0))
        bytes.seek(0)
        reader = BytesIOReader(bytes.read())
        get_buf(reader, &buf)
        deserialized_dt = from_binary(des, &buf, 0)

        return deserialized_dt

    # deserialize
    # epoc
    expected = 0
    assert_equal(deserialize(expected), datetime.datetime.fromtimestamp(expected, tz=datetime.timezone.utc).replace(tzinfo=None))

    # beyond 32b
    expected = 2 ** 33
    assert_equal(deserialize(expected), datetime.datetime(2242, 3, 16, 12, 56, 32))

    # less than epoc (PYTHON-119)
    expected = -770172256
    assert_equal(deserialize(expected), datetime.datetime(1945, 8, 5, 23, 15, 44))

    # work around rounding difference among Python versions (PYTHON-230)
    # This wont pass with the cython extension until we fix the microseconds alignment with CPython
    #expected = 1424817268.274
    #assert_equal(deserialize(expected), datetime.datetime(2015, 2, 24, 22, 34, 28, 274000))

    # Large date overflow (PYTHON-452)
    expected = 2177403010.123
    assert_equal(deserialize(expected), datetime.datetime(2038, 12, 31, 10, 10, 10, 123000))


def test_date_side_by_side(assert_equal):
    # Test pure python and cython date deserialization side-by-side
    # This is meant to detect inconsistent rounding or conversion (PYTHON-480 for example)
    # The test covers the full range of time deserializable in Python. It bounds through
    # the range in factors of two to cover floating point scale. At each bound it sweeps
    # all combinations of fractional seconds to verify rounding

    cdef BytesIOReader reader
    cdef Buffer buf
    cdef Deserializer cython_deserializer = find_deserializer(DateType)
    import time

    def verify_time(ms):
        blob = DateType.serialize(ms, 0)
        bior = BytesIOReader(blob)
        buf.ptr = bior.read()
        buf.size = bior.size
        cython_deserialized = from_binary(cython_deserializer, &buf, 0)
        python_deserialized = DateType.deserialize(blob, 0)
        assert_equal(cython_deserialized, python_deserialized)

    # min -> 0
    x = int(calendar.timegm(datetime.datetime(1, 1, 1).utctimetuple()) * 1000)
    while x < -1:  # for some reason -1 // 2 == -1 so we can't wait for zero
        for y in range(1000):
            verify_time(x + y)
        x //= 2

    # max -> 0
    x = int(calendar.timegm(datetime.datetime(9999, 12, 31, 23, 59, 59, 999999).utctimetuple()) * 1000)
    while x:
        for ms in range(1000):
            verify_time(x - ms)
        x //= 2
