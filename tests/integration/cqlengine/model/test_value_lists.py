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

import random
from tests.integration.cqlengine.base import BaseCassEngTestCase

from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.management import drop_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns


class TestModel(Model):

    id = columns.Integer(primary_key=True)
    clustering_key = columns.Integer(primary_key=True, clustering_order='desc')

class TestClusteringComplexModel(Model):

    id = columns.Integer(primary_key=True)
    clustering_key = columns.Integer(primary_key=True, clustering_order='desc')
    some_value = columns.Integer()

class TestClusteringOrder(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestClusteringOrder, cls).setUpClass()
        sync_table(TestModel)

    @classmethod
    def tearDownClass(cls):
        super(TestClusteringOrder, cls).tearDownClass()
        drop_table(TestModel)

    def test_clustering_order(self):
        """
        Tests that models can be saved and retrieved
        """
        items = list(range(20))
        random.shuffle(items)
        for i in items:
            TestModel.create(id=1, clustering_key=i)

        values = list(TestModel.objects.values_list('clustering_key', flat=True))
        # [19L, 18L, 17L, 16L, 15L, 14L, 13L, 12L, 11L, 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L, 1L, 0L]
        self.assertEqual(values, sorted(items, reverse=True))

    def test_clustering_order_more_complex(self):
        """
        Tests that models can be saved and retrieved
        """
        sync_table(TestClusteringComplexModel)

        items = list(range(20))
        random.shuffle(items)
        for i in items:
            TestClusteringComplexModel.create(id=1, clustering_key=i, some_value=2)

        values = list(TestClusteringComplexModel.objects.values_list('some_value', flat=True))

        self.assertEqual([2] * 20, values)
        drop_table(TestClusteringComplexModel)

