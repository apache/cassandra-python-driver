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

from tests.unit.cython.utils import cyimport, cythontest
bytesio_testhelper = cyimport('tests.unit.cython.bytesio_testhelper')

import unittest


class BytesIOTest(unittest.TestCase):
    """Test Cython BytesIO proxy"""

    @cythontest
    def test_reading(self):
        bytesio_testhelper.test_read1(self.assertEqual, self.assertRaises)
        bytesio_testhelper.test_read2(self.assertEqual, self.assertRaises)
        bytesio_testhelper.test_read3(self.assertEqual, self.assertRaises)

    @cythontest
    def test_reading_error(self):
        bytesio_testhelper.test_read_eof(self.assertEqual, self.assertRaises)
