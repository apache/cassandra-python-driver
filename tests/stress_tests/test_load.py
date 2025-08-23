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

import gc

from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table

from tests.integration.cqlengine.base import BaseCassEngTestCase


class LoadTests(BaseCassEngTestCase):

    def test_lots_of_queries(self):
        import resource
        import objgraph

        class LoadTest(Model):
            k = columns.Integer(primary_key=True)
            v = columns.Integer()

        sync_table(LoadTest)
        gc.collect()
        objgraph.show_most_common_types()

        print("Starting...")

        for i in range(1000000):
            if i % 25000 == 0:
                # print memory statistic
                print("Memory usage: %s" % (resource.getrusage(resource.RUSAGE_SELF).ru_maxrss))

            LoadTest.create(k=i, v=i)

        objgraph.show_most_common_types()

        raise Exception("you shouldn't be here")
