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
import unittest

import sys

from cassandra.cqlengine.connection import get_session
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns

from uuid import uuid4

class TestQueryUpdateModel(Model):

    partition = columns.UUID(primary_key=True, default=uuid4)
    cluster = columns.Integer(primary_key=True)
    count = columns.Integer(required=False)
    text = columns.Text(required=False, index=True)
    text_set = columns.Set(columns.Text, required=False)
    text_list = columns.List(columns.Text, required=False)
    text_map = columns.Map(columns.Text, columns.Text, required=False)

class BaseCassEngTestCase(unittest.TestCase):

    session = None

    def setUp(self):
        self.session = get_session()

    def assertHasAttr(self, obj, attr):
        self.assertTrue(hasattr(obj, attr),
                "{0} doesn't have attribute: {1}".format(obj, attr))

    def assertNotHasAttr(self, obj, attr):
        self.assertFalse(hasattr(obj, attr),
                "{0} shouldn't have the attribute: {1}".format(obj, attr))

    if sys.version_info > (3, 0):
        def assertItemsEqual(self, first, second, msg=None):
            return self.assertCountEqual(first, second, msg)
