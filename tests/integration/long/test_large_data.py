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

try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # noqa

from struct import pack
import logging, sys, traceback, time

from cassandra import ConsistencyLevel, OperationTimedOut, WriteTimeout
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.query import dict_factory
from cassandra.query import SimpleStatement
from tests.integration import use_singledc, PROTOCOL_VERSION, TestCluster
from tests.integration.long.utils import create_schema

import unittest

log = logging.getLogger(__name__)


def setup_module():
    use_singledc()


# Converts an integer to an string of letters
def create_column_name(i):
    letters = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

    column_name = ''
    while True:
        column_name += letters[i % 10]
        i = i // 10
        if not i:
            break

    if column_name == 'if':
        column_name = 'special_case'
    return column_name


class LargeDataTests(unittest.TestCase):

    def setUp(self):
        self.keyspace = 'large_data'

    def make_session_and_keyspace(self):
        cluster = TestCluster(execution_profiles={
            EXEC_PROFILE_DEFAULT: ExecutionProfile(request_timeout=20, row_factory=dict_factory)
        })
        session = cluster.connect()
        create_schema(cluster, session, self.keyspace)
        return session

    def batch_futures(self, session, statement_generator):
        concurrency = 10
        futures = Queue(maxsize=concurrency)
        number_of_timeouts = 0
        for i, statement in enumerate(statement_generator):
            if i > 0 and i % (concurrency - 1) == 0:
                # clear the existing queue
                while True:
                    try:
                        futures.get_nowait().result()
                    except (OperationTimedOut, WriteTimeout):
                        ex_type, ex, tb = sys.exc_info()
                        number_of_timeouts += 1
                        log.warning("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                        del tb
                        time.sleep(1)
                    except Empty:
                        break

            future = session.execute_async(statement)
            futures.put_nowait(future)

        while True:
            try:
                futures.get_nowait().result()
            except (OperationTimedOut, WriteTimeout):
                ex_type, ex, tb = sys.exc_info()
                number_of_timeouts += 1
                log.warning("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                del tb
                time.sleep(1)
            except Empty:
                break
        return number_of_timeouts

    def test_wide_rows(self):
        table = 'wide_rows'
        session = self.make_session_and_keyspace()
        session.execute('CREATE TABLE %s (k INT, i INT, PRIMARY KEY(k, i))' % table)

        prepared = session.prepare('INSERT INTO %s (k, i) VALUES (0, ?)' % (table, ))

        # Write via async futures
        self.batch_futures(session, (prepared.bind((i, )) for i in range(100000)))

        # Read
        results = session.execute('SELECT i FROM %s WHERE k=0' % (table, ))

        # Verify
        for i, row in enumerate(results):
            self.assertAlmostEqual(row['i'], i, delta=3)

        session.cluster.shutdown()

    def test_wide_batch_rows(self):
        """
        Test for inserting wide rows with batching

        test_wide_batch_rows tests inserting a wide row of data using batching. It will then attempt to query
        that data and ensure that all of it has been inserted appropriately.

        @expected_result all items should be inserted, and verified.

        @test_category queries:batch
        """

        # Table Creation
        table = 'wide_batch_rows'
        session = self.make_session_and_keyspace()
        session.execute('CREATE TABLE %s (k INT, i INT, PRIMARY KEY(k, i))' % table)

        # Run batch insert
        statement = 'BEGIN BATCH '
        to_insert = 2000
        for i in range(to_insert):
            statement += 'INSERT INTO %s (k, i) VALUES (%s, %s) ' % (table, 0, i)
        statement += 'APPLY BATCH'
        statement = SimpleStatement(statement, consistency_level=ConsistencyLevel.QUORUM)

        # Execute insert with larger timeout, since it's a wide row
        try:
            session.execute(statement,timeout=30.0)

        except OperationTimedOut:
            #If we timeout on insertion that's bad but it could be just slow underlying c*
            #Attempt to validate anyway, we will fail if we don't get the right data back.
            ex_type, ex, tb = sys.exc_info()
            log.warning("Batch wide row insertion timed out, this may require additional investigation")
            log.warning("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
            del tb

        # Verify
        results = session.execute('SELECT i FROM %s WHERE k=%s' % (table, 0))
        lastvalue = 0
        for j, row in enumerate(results):
            lastValue=row['i']
            self.assertEqual(lastValue, j)

        #check the last value make sure it's what we expect
        index_value = to_insert-1
        self.assertEqual(lastValue,index_value,"Verification failed only found {0} inserted we were expecting {1}".format(j,index_value))

        session.cluster.shutdown()

    def test_wide_byte_rows(self):
        """
        Test for inserting wide row of bytes

        test_wide_batch_rows tests inserting a wide row of data bytes. It will then attempt to query
        that data and ensure that all of it has been inserted appropriately.

        @expected_result all items should be inserted, and verified.

        @test_category queries
        """

        # Table creation
        table = 'wide_byte_rows'
        session = self.make_session_and_keyspace()
        session.execute('CREATE TABLE %s (k INT, i INT, v BLOB, PRIMARY KEY(k, i))' % table)

        # Prepare statement and run insertions
        to_insert = 100000
        prepared = session.prepare('INSERT INTO %s (k, i, v) VALUES (0, ?, 0xCAFE)' % (table, ))
        timeouts = self.batch_futures(session, (prepared.bind((i, )) for i in range(to_insert)))

        # Read
        results = session.execute('SELECT i, v FROM %s WHERE k=0' % (table, ))

        # number of expected results
        expected_results = to_insert-timeouts-1

        # Verify
        bb = pack('>H', 0xCAFE)
        for i, row in enumerate(results):
            self.assertEqual(row['v'], bb)

        self.assertGreaterEqual(i, expected_results, "Verification failed only found {0} inserted we were expecting {1}".format(i,expected_results))

        session.cluster.shutdown()

    def test_large_text(self):
        """
        Test for inserting a large text field

        test_large_text tests inserting a large text field into a row.

        @expected_result the large text value should be inserted. When the row is queried it should match the original
        value that was inserted

        @test_category queries
        """
        table = 'large_text'
        session = self.make_session_and_keyspace()
        session.execute('CREATE TABLE %s (k int PRIMARY KEY, txt text)' % table)

        # Create ultra-long text
        text = 'a' * 1000000

        # Write
        session.execute(SimpleStatement("INSERT INTO %s (k, txt) VALUES (%s, '%s')"
                                        % (table, 0, text),
                                        consistency_level=ConsistencyLevel.QUORUM))

        # Read
        result = session.execute('SELECT * FROM %s WHERE k=%s' % (table, 0))

        # Verify
        found_result = False
        for i, row in enumerate(result):
            self.assertEqual(row['txt'], text)
            found_result = True
        self.assertTrue(found_result, "No results were found")

        session.cluster.shutdown()

    def test_wide_table(self):
        table = 'wide_table'
        table_width = 330
        session = self.make_session_and_keyspace()
        table_declaration = 'CREATE TABLE %s (key INT PRIMARY KEY, '
        table_declaration += ' INT, '.join(create_column_name(i) for i in range(table_width))
        table_declaration += ' INT)'
        session.execute(table_declaration % table)

        # Write
        insert_statement = 'INSERT INTO %s (key, '
        insert_statement += ', '.join(create_column_name(i) for i in range(table_width))
        insert_statement += ') VALUES (%s, '
        insert_statement += ', '.join(str(i) for i in range(table_width))
        insert_statement += ')'
        insert_statement = insert_statement % (table, 0)

        session.execute(SimpleStatement(insert_statement, consistency_level=ConsistencyLevel.QUORUM))

        # Read
        result = session.execute('SELECT * FROM %s WHERE key=%s' % (table, 0))

        # Verify
        for row in result:
            for i in range(table_width):
                self.assertEqual(row[create_column_name(i)], i)

        session.cluster.shutdown()
