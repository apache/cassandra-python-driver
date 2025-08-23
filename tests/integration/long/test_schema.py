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

import logging

from cassandra import ConsistencyLevel, AlreadyExists
from cassandra.query import SimpleStatement

from tests.integration import use_singledc, execute_until_pass, TestCluster

import time

import unittest

log = logging.getLogger(__name__)


def setup_module():
    use_singledc()


class SchemaTests(unittest.TestCase):

    @classmethod
    def setup_class(cls):
        cls.cluster = TestCluster()
        cls.session = cls.cluster.connect(wait_for_all_pools=True)

    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()

    def test_recreates(self):
        """
        Basic test for repeated schema creation and use, using many different keyspaces
        """

        session = self.session

        for i in range(2):
            for keyspace_number in range(5):
                keyspace = "ks_{0}".format(keyspace_number)

                if keyspace in self.cluster.metadata.keyspaces.keys():
                    drop = "DROP KEYSPACE {0}".format(keyspace)
                    log.debug(drop)
                    execute_until_pass(session, drop)

                create = "CREATE KEYSPACE {0} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}}".format(keyspace)
                log.debug(create)
                execute_until_pass(session, create)

                create = "CREATE TABLE {0}.cf (k int PRIMARY KEY, i int)".format(keyspace)
                log.debug(create)
                execute_until_pass(session, create)

                use = "USE {0}".format(keyspace)
                log.debug(use)
                execute_until_pass(session, use)

                insert = "INSERT INTO cf (k, i) VALUES (0, 0)"
                log.debug(insert)
                ss = SimpleStatement(insert, consistency_level=ConsistencyLevel.QUORUM)
                execute_until_pass(session, ss)

    def test_for_schema_disagreements_different_keyspaces(self):
        """
        Tests for any schema disagreements using many different keyspaces
        """

        session = self.session

        for i in range(30):
            execute_until_pass(session, "CREATE KEYSPACE test_{0} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}".format(i))
            execute_until_pass(session, "CREATE TABLE test_{0}.cf (key int PRIMARY KEY, value int)".format(i))

            for j in range(100):
                execute_until_pass(session, "INSERT INTO test_{0}.cf (key, value) VALUES ({1}, {1})".format(i, j))

            execute_until_pass(session, "DROP KEYSPACE test_{0}".format(i))

    def test_for_schema_disagreements_same_keyspace(self):
        """
        Tests for any schema disagreements using the same keyspace multiple times
        """

        cluster = TestCluster()
        session = cluster.connect(wait_for_all_pools=True)

        for i in range(30):
            try:
                execute_until_pass(session, "CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
            except AlreadyExists:
                execute_until_pass(session, "DROP KEYSPACE test")
                execute_until_pass(session, "CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")

            execute_until_pass(session, "CREATE TABLE test.cf (key int PRIMARY KEY, value int)")

            for j in range(100):
                execute_until_pass(session, "INSERT INTO test.cf (key, value) VALUES ({0}, {0})".format(j))

            execute_until_pass(session, "DROP KEYSPACE test")
        cluster.shutdown()

    def test_for_schema_disagreement_attribute(self):
        """
        Tests to ensure that schema disagreement is properly surfaced on the response future.

        Creates and destroys keyspaces/tables with various schema agreement timeouts set.
        First part runs cql create/drop cmds with schema agreement set in such away were it will be impossible for agreement to occur during timeout.
        It then validates that the correct value is set on the result.
        Second part ensures that when schema agreement occurs, that the result set reflects that appropriately

        @since 3.1.0
        @jira_ticket PYTHON-458
        @expected_result is_schema_agreed is set appropriately on response thefuture

        @test_category schema
        """
        # This should yield a schema disagreement
        cluster = TestCluster(max_schema_agreement_wait=0.001)
        session = cluster.connect(wait_for_all_pools=True)

        rs = session.execute("CREATE KEYSPACE test_schema_disagreement WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
        self.check_and_wait_for_agreement(session, rs, False)
        rs = session.execute(SimpleStatement("CREATE TABLE test_schema_disagreement.cf (key int PRIMARY KEY, value int)",
                                             consistency_level=ConsistencyLevel.ALL))
        self.check_and_wait_for_agreement(session, rs, False)
        rs = session.execute("DROP KEYSPACE test_schema_disagreement")
        self.check_and_wait_for_agreement(session, rs, False)
        cluster.shutdown()
        
        # These should have schema agreement
        cluster = TestCluster(max_schema_agreement_wait=100)
        session = cluster.connect()
        rs = session.execute("CREATE KEYSPACE test_schema_disagreement WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
        self.check_and_wait_for_agreement(session, rs, True)
        rs = session.execute(SimpleStatement("CREATE TABLE test_schema_disagreement.cf (key int PRIMARY KEY, value int)",
                                             consistency_level=ConsistencyLevel.ALL))
        self.check_and_wait_for_agreement(session, rs, True)
        rs = session.execute("DROP KEYSPACE test_schema_disagreement")
        self.check_and_wait_for_agreement(session, rs, True)
        cluster.shutdown()

    def check_and_wait_for_agreement(self, session, rs, exepected):
        # Wait for RESULT_KIND_SCHEMA_CHANGE message to arrive
        time.sleep(1)
        self.assertEqual(rs.response_future.is_schema_agreed, exepected)
        if not rs.response_future.is_schema_agreed:
            session.cluster.control_connection.wait_for_schema_agreement(wait_time=1000)
