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

from datetime import datetime, timedelta
from uuid import uuid4
from cassandra.cqlengine.functions import get_total_seconds

from tests.integration.cqlengine.base import BaseCassEngTestCase

from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.management import drop_table
from cassandra.cqlengine.models import Model
from cassandra.cqlengine import columns
from tests.integration.cqlengine import execute_count


class DateTimeQueryTestModel(Model):

    user = columns.Integer(primary_key=True)
    day = columns.DateTime(primary_key=True)
    data = columns.Text()


class TestDateTimeQueries(BaseCassEngTestCase):

    @classmethod
    def setUpClass(cls):
        super(TestDateTimeQueries, cls).setUpClass()
        sync_table(DateTimeQueryTestModel)

        cls.base_date = datetime.now() - timedelta(days=10)
        for x in range(7):
            for y in range(10):
                DateTimeQueryTestModel.create(
                    user=x,
                    day=(cls.base_date+timedelta(days=y)),
                    data=str(uuid4())
                )

    @classmethod
    def tearDownClass(cls):
        super(TestDateTimeQueries, cls).tearDownClass()
        drop_table(DateTimeQueryTestModel)

    @execute_count(1)
    def test_range_query(self):
        """ Tests that loading from a range of dates works properly """
        start = datetime(*self.base_date.timetuple()[:3])
        end = start + timedelta(days=3)

        results = DateTimeQueryTestModel.filter(user=0, day__gte=start, day__lt=end)
        assert len(results) == 3

    @execute_count(3)
    def test_datetime_precision(self):
        """ Tests that millisecond resolution is preserved when saving datetime objects """
        now = datetime.now()
        pk = 1000
        obj = DateTimeQueryTestModel.create(user=pk, day=now, data='energy cheese')
        load = DateTimeQueryTestModel.get(user=pk)

        self.assertAlmostEqual(get_total_seconds(now - load.day), 0, 2)
        obj.delete()

