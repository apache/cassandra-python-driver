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

import os
from cassandra.cluster import Cluster
from tests.integration import use_singledc, PROTOCOL_VERSION


def setup_module():
    use_singledc()


class StressInsertsTests(unittest.TestCase):
    """
    Test case for PYTHON-124: Repeated inserts may exhaust all connections
    causing NoConnectionsAvailable, in_flight never decreased
    """

    def setUp(self):
        """
        USE test1rf;
        DROP TABLE IF EXISTS race;
        CREATE TABLE race (x int PRIMARY KEY);
        """
        self.cluster = Cluster(protocol_version=PROTOCOL_VERSION)
        self.session = self.cluster.connect('test1rf')

        ddl1 = '''
            DROP TABLE IF EXISTS race'''
        self.session.execute(ddl1)

        ddl2 = '''
            CREATE TABLE race (x int PRIMARY KEY)'''
        self.session.execute(ddl2)

    def tearDown(self):
        """
        DROP TABLE test1rf.race
        Shutdown cluster
        """
        self.session.execute("DROP TABLE race")
        self.cluster.shutdown()

    def test_in_flight_is_one(self):
        """
        Verify that in_flight value stays equal to one while doing multiple inserts.
        The number of inserts can be set through INSERTS_ITERATIONS environmental variable.
        Default value is 1000000.
        """
        prepared = self.session.prepare("INSERT INTO race (x) VALUES (?)")
        iterations = int(os.getenv("INSERT_ITERATIONS", 1000000))
        i = 0
        leaking_connections = False
        while i < iterations and not leaking_connections:
            bound = prepared.bind((i,))
            self.session.execute(bound)
            for pool in self.session._pools.values():
                if leaking_connections:
                    break
                for conn in pool.get_connections():
                    if conn.in_flight > 1:
                        print(self.session.get_pool_state())
                        leaking_connections = True
                        break
            i = i + 1

        self.assertFalse(leaking_connections, 'Detected leaking connection after %s iterations' % i)
