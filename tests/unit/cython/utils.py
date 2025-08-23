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

from cassandra.cython_deps import HAVE_CYTHON, HAVE_NUMPY
try:
    from tests import VERIFY_CYTHON
except ImportError:
    VERIFY_CYTHON = False

import unittest

def cyimport(import_path):
    """
    Import a Cython module if available, otherwise return None
    (and skip any relevant tests).
    """
    if HAVE_CYTHON:
        import pyximport
        py_importer, pyx_importer = pyximport.install()
        mod = __import__(import_path, fromlist=[True])
        pyximport.uninstall(py_importer, pyx_importer)
        return mod


# @cythontest
# def test_something(self): ...
cythontest = unittest.skipUnless((HAVE_CYTHON or VERIFY_CYTHON) or VERIFY_CYTHON, 'Cython is not available')
notcython = unittest.skipIf(HAVE_CYTHON, 'Cython not supported')
numpytest = unittest.skipUnless((HAVE_CYTHON and HAVE_NUMPY) or VERIFY_CYTHON, 'NumPy is not available')
