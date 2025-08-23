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

from uuid import uuid4

from cassandra.cqlengine import columns
from cassandra.cqlengine.management import sync_table, drop_table
from cassandra.cqlengine.models import Model

from tests.integration.cqlengine.base import BaseCassEngTestCase
from tests.integration import PROTOCOL_VERSION

# TODO: is this really a protocol limitation, or is it just C* version?
# good enough proxy for now
STATIC_SUPPORTED = PROTOCOL_VERSION >= 2

class TestStaticModel(Model):
    partition = columns.UUID(primary_key=True, default=uuid4)
    cluster = columns.UUID(primary_key=True, default=uuid4)
    static = columns.Text(static=True)
    text = columns.Text()


class TestStaticColumn(BaseCassEngTestCase):

    def setUp(cls):
        if not STATIC_SUPPORTED:
            raise unittest.SkipTest("only runs against the cql3 protocol v2.0")
        super(TestStaticColumn, cls).setUp()

    @classmethod
    def setUpClass(cls):
        drop_table(TestStaticModel)
        if STATIC_SUPPORTED:  # setup and teardown run regardless of skip
            sync_table(TestStaticModel)

    @classmethod
    def tearDownClass(cls):
        drop_table(TestStaticModel)

    def test_mixed_updates(self):
        """ Tests that updates on both static and non-static columns work as intended """
        instance = TestStaticModel.create()
        instance.static = "it's shared"
        instance.text = "some text"
        instance.save()

        u = TestStaticModel.get(partition=instance.partition)
        u.static = "it's still shared"
        u.text = "another text"
        u.update()
        actual = TestStaticModel.get(partition=u.partition)

        assert actual.static == "it's still shared"

    def test_static_only_updates(self):
        """ Tests that updates on static only column work as intended """
        instance = TestStaticModel.create()
        instance.static = "it's shared"
        instance.text = "some text"
        instance.save()

        u = TestStaticModel.get(partition=instance.partition)
        u.static = "it's still shared"
        u.update()
        actual = TestStaticModel.get(partition=u.partition)
        assert actual.static == "it's still shared"

    def test_static_with_null_cluster_key(self):
        """ Tests that save/update/delete works for static column works when clustering key is null"""
        instance = TestStaticModel.create(cluster=None, static = "it's shared")
        instance.save()

        u = TestStaticModel.get(partition=instance.partition)
        u.static = "it's still shared"
        u.update()
        actual = TestStaticModel.get(partition=u.partition)
        assert actual.static == "it's still shared"
