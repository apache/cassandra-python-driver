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

from cassandra import OperationTimedOut, WriteTimeout
from cassandra.cluster import Cluster, ExecutionProfile, ResponseFuture, EXEC_PROFILE_DEFAULT, NoHostAvailable
from cassandra.query import SimpleStatement
from cassandra.policies import ConstantSpeculativeExecutionPolicy, RoundRobinPolicy, RetryPolicy, WriteType
from cassandra.protocol import OverloadedErrorMessage, IsBootstrappingErrorMessage, TruncateError, ServerError

from tests.integration import greaterthancass21, requiressimulacron, SIMULACRON_JAR, \
    CASSANDRA_VERSION
from tests.integration.simulacron import PROTOCOL_VERSION
from tests.integration.simulacron.utils import start_and_prime_singledc, prime_query, \
    stop_simulacron, NO_THEN, clear_queries

from itertools import count
from packaging.version import Version


class BadRoundRobinPolicy(RoundRobinPolicy):
    def make_query_plan(self, working_keyspace=None, query=None):
        pos = self._position
        self._position += 1

        hosts = []
        for _ in range(10):
            hosts.extend(self._live_hosts)

        return hosts


# This doesn't work well with Windows clock granularity
@requiressimulacron
class SpecExecTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if SIMULACRON_JAR is None or CASSANDRA_VERSION < Version("2.1"):
            return

        start_and_prime_singledc()
        cls.cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False)
        cls.session = cls.cluster.connect(wait_for_all_pools=True)

        spec_ep_brr = ExecutionProfile(load_balancing_policy=BadRoundRobinPolicy(),
                                       speculative_execution_policy=ConstantSpeculativeExecutionPolicy(1, 6),
                                       request_timeout=12)
        spec_ep_rr = ExecutionProfile(speculative_execution_policy=ConstantSpeculativeExecutionPolicy(.5, 10),
                                      request_timeout=12)
        spec_ep_rr_lim = ExecutionProfile(load_balancing_policy=BadRoundRobinPolicy(),
                                          speculative_execution_policy=ConstantSpeculativeExecutionPolicy(0.5, 1),
                                          request_timeout=12)
        spec_ep_brr_lim = ExecutionProfile(load_balancing_policy=BadRoundRobinPolicy(),
                                           speculative_execution_policy=ConstantSpeculativeExecutionPolicy(4, 10))

        cls.cluster.add_execution_profile("spec_ep_brr", spec_ep_brr)
        cls.cluster.add_execution_profile("spec_ep_rr", spec_ep_rr)
        cls.cluster.add_execution_profile("spec_ep_rr_lim", spec_ep_rr_lim)
        cls.cluster.add_execution_profile("spec_ep_brr_lim", spec_ep_brr_lim)

    @classmethod
    def tearDownClass(cls):
        if SIMULACRON_JAR is None or CASSANDRA_VERSION < Version("2.1"):
            return

        cls.cluster.shutdown()
        stop_simulacron()

    def tearDown(self):
        clear_queries()

    @greaterthancass21
    def test_speculative_execution(self):
        """
        Test to ensure that speculative execution honors LBP, and that they retry appropriately.

        This test will use various LBP, and ConstantSpeculativeExecutionPolicy settings and ensure the proper number of hosts are queried
        @since 3.7.0
        @jira_ticket PYTHON-218
        @expected_result speculative retries should honor max retries, idempotent state of queries, and underlying lbp.

        @test_category metadata
        """
        query_to_prime = "INSERT INTO test3rf.test (k, v) VALUES (0, 1);"
        prime_query(query_to_prime, then={"delay_in_ms": 10000})

        statement = SimpleStatement(query_to_prime, is_idempotent=True)
        statement_non_idem = SimpleStatement(query_to_prime, is_idempotent=False)

        # This LBP should repeat hosts up to around 30
        result = self.session.execute(statement, execution_profile='spec_ep_brr')
        self.assertEqual(7, len(result.response_future.attempted_hosts))

        # This LBP should keep host list to 3
        result = self.session.execute(statement, execution_profile='spec_ep_rr')
        self.assertEqual(3, len(result.response_future.attempted_hosts))
        # Spec_execution policy should limit retries to 1
        result = self.session.execute(statement, execution_profile='spec_ep_rr_lim')

        self.assertEqual(2, len(result.response_future.attempted_hosts))

        # Spec_execution policy should not be used if  the query is not idempotent
        result = self.session.execute(statement_non_idem, execution_profile='spec_ep_brr')
        self.assertEqual(1, len(result.response_future.attempted_hosts))

        # Default policy with non_idem query
        result = self.session.execute(statement_non_idem, timeout=12)
        self.assertEqual(1, len(result.response_future.attempted_hosts))

        # Should be able to run an idempotent query against default execution policy with no speculative_execution_policy
        result = self.session.execute(statement, timeout=12)
        self.assertEqual(1, len(result.response_future.attempted_hosts))

        # Test timeout with spec_ex
        with self.assertRaises(OperationTimedOut):
            self.session.execute(statement, execution_profile='spec_ep_rr', timeout=.5)

        prepared_query_to_prime = "SELECT * FROM test3rf.test where k = ?"
        when = {"params": {"k": "0"}, "param_types": {"k": "ascii"}}
        prime_query(prepared_query_to_prime, when=when, then={"delay_in_ms": 4000})

        # PYTHON-736 Test speculation policy works with a prepared statement
        prepared_statement = self.session.prepare(prepared_query_to_prime)
        # non-idempotent
        result = self.session.execute(prepared_statement, ("0",), execution_profile='spec_ep_brr')
        self.assertEqual(1, len(result.response_future.attempted_hosts))
        # idempotent
        prepared_statement.is_idempotent = True
        result = self.session.execute(prepared_statement, ("0",), execution_profile='spec_ep_brr')
        self.assertLess(1, len(result.response_future.attempted_hosts))

    def test_speculative_and_timeout(self):
        """
        Test to ensure the timeout is honored when using speculative execution
        @since 3.10
        @jira_ticket PYTHON-750
        @expected_result speculative retries be schedule every fixed period, during the maximum
        period of the timeout.

        @test_category metadata
        """
        query_to_prime = "INSERT INTO testkeyspace.testtable (k, v) VALUES (0, 1);"
        prime_query(query_to_prime, then=NO_THEN)

        statement = SimpleStatement(query_to_prime, is_idempotent=True)

        # An OperationTimedOut is placed here in response_future,
        # that's why we can't call session.execute,which would raise it, but
        # we have to directly wait for the event
        response_future = self.session.execute_async(statement, execution_profile='spec_ep_brr_lim',
                                                     timeout=14)
        response_future._event.wait(16)
        self.assertIsInstance(response_future._final_exception, OperationTimedOut)

        # This is because 14 / 4 + 1 = 4
        self.assertEqual(len(response_future.attempted_hosts), 4)

    def test_delay_can_be_0(self):
        """
        Test to validate that the delay can be zero for the ConstantSpeculativeExecutionPolicy
        @since 3.13
        @jira_ticket PYTHON-836
        @expected_result all the queries are executed immediately
        @test_category policy
        """
        query_to_prime = "INSERT INTO madeup_keyspace.madeup_table(k, v) VALUES (1, 2)"
        prime_query(query_to_prime, then={"delay_in_ms": 5000})
        number_of_requests = 4
        spec = ExecutionProfile(load_balancing_policy=RoundRobinPolicy(),
                                speculative_execution_policy=ConstantSpeculativeExecutionPolicy(0, number_of_requests))

        cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False)
        cluster.add_execution_profile("spec", spec)
        session = cluster.connect(wait_for_all_pools=True)
        self.addCleanup(cluster.shutdown)

        counter = count()

        def patch_and_count(f):
            def patched(*args, **kwargs):
                next(counter)
                f(*args, **kwargs)
            return patched

        self.addCleanup(setattr, ResponseFuture, "send_request", ResponseFuture.send_request)
        ResponseFuture.send_request = patch_and_count(ResponseFuture.send_request)
        stmt = SimpleStatement(query_to_prime)
        stmt.is_idempotent = True
        results = session.execute(stmt, execution_profile="spec")
        self.assertEqual(len(results.response_future.attempted_hosts), 3)

        # send_request is called number_of_requests times for the speculative request
        # plus one for the call from the main thread.
        self.assertEqual(next(counter), number_of_requests + 1)


class CustomRetryPolicy(RetryPolicy):
    def on_write_timeout(self, query, consistency, write_type,
                         required_responses, received_responses, retry_num):
        if retry_num != 0:
            return self.RETHROW, None
        elif write_type == WriteType.SIMPLE:
            return self.RETHROW, None
        elif write_type == WriteType.CDC:
            return self.IGNORE, None


class CounterRetryPolicy(RetryPolicy):
    def __init__(self):
        self.write_timeout = count()
        self.read_timeout = count()
        self.unavailable = count()
        self.request_error = count()

    def on_read_timeout(self, query, consistency, required_responses,
                        received_responses, data_retrieved, retry_num):
        next(self.read_timeout)
        return self.IGNORE, None

    def on_write_timeout(self, query, consistency, write_type,
                         required_responses, received_responses, retry_num):
        next(self.write_timeout)
        return self.IGNORE, None

    def on_unavailable(self, query, consistency, required_replicas, alive_replicas, retry_num):
        next(self.unavailable)
        return self.IGNORE, None

    def on_request_error(self, query, consistency, error, retry_num):
        next(self.request_error)
        return self.RETHROW, None

    def reset_counters(self):
        self.write_timeout = count()
        self.read_timeout = count()
        self.unavailable = count()
        self.request_error = count()


@requiressimulacron
class RetryPolicyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if SIMULACRON_JAR is None or CASSANDRA_VERSION < Version("2.1"):
            return
        start_and_prime_singledc()

    @classmethod
    def tearDownClass(cls):
        if SIMULACRON_JAR is None or CASSANDRA_VERSION < Version("2.1"):
            return
        stop_simulacron()

    def tearDown(self):
        clear_queries()

    def set_cluster(self, retry_policy):
        self.cluster = Cluster(
            protocol_version=PROTOCOL_VERSION,
            compression=False,
            execution_profiles={
                EXEC_PROFILE_DEFAULT: ExecutionProfile(retry_policy=retry_policy)
            },
        )
        self.session = self.cluster.connect(wait_for_all_pools=True)
        self.addCleanup(self.cluster.shutdown)

    def test_retry_policy_ignores_and_rethrows(self):
        """
        Test to verify :class:`~cassandra.protocol.WriteTimeoutErrorMessage` is decoded correctly and that
        :attr:`.~cassandra.policies.RetryPolicy.RETHROW` and
        :attr:`.~cassandra.policies.RetryPolicy.IGNORE` are respected
        to localhost

        @since 3.12
        @jira_ticket PYTHON-812
        @expected_result the retry policy functions as expected

        @test_category connection
        """
        self.set_cluster(CustomRetryPolicy())
        query_to_prime_simple = "SELECT * from simulacron_keyspace.simple"
        query_to_prime_cdc = "SELECT * from simulacron_keyspace.cdc"
        then = {
            "result": "write_timeout",
            "delay_in_ms": 0,
            "consistency_level": "LOCAL_QUORUM",
            "received": 1,
            "block_for": 2,
            "write_type": "SIMPLE",
            "ignore_on_prepare": True
          }
        prime_query(query_to_prime_simple, rows=None, column_types=None, then=then)
        then["write_type"] = "CDC"
        prime_query(query_to_prime_cdc, rows=None, column_types=None, then=then)

        with self.assertRaises(WriteTimeout):
            self.session.execute(query_to_prime_simple)

        #CDC should be ignored
        self.session.execute(query_to_prime_cdc)

    def test_retry_policy_with_prepared(self):
        """
        Test to verify that the retry policy is called as expected
        for bound and prepared statements when set at the cluster level

        @since 3.13
        @jira_ticket PYTHON-861
        @expected_result the appropriate retry policy is called

        @test_category connection
        """
        counter_policy = CounterRetryPolicy()
        self.set_cluster(counter_policy)
        query_to_prime = "SELECT * from simulacron_keyspace.simulacron_table"
        then = {
            "result": "write_timeout",
            "delay_in_ms": 0,
            "consistency_level": "LOCAL_QUORUM",
            "received": 1,
            "block_for": 2,
            "write_type": "SIMPLE",
            "ignore_on_prepare": True
          }
        prime_query(query_to_prime, then=then, rows=None, column_types=None)
        self.session.execute(query_to_prime)
        self.assertEqual(next(counter_policy.write_timeout), 1)
        counter_policy.reset_counters()

        query_to_prime_prepared = "SELECT * from simulacron_keyspace.simulacron_table WHERE key = :key"
        when = {"params": {"key": "0"}, "param_types": {"key": "ascii"}}

        prime_query(query_to_prime_prepared, when=when, then=then, rows=None, column_types=None)

        prepared_stmt = self.session.prepare(query_to_prime_prepared)

        bound_stm = prepared_stmt.bind({"key": "0"})
        self.session.execute(bound_stm)
        self.assertEqual(next(counter_policy.write_timeout), 1)

        counter_policy.reset_counters()
        self.session.execute(prepared_stmt, ("0",))
        self.assertEqual(next(counter_policy.write_timeout), 1)

    def test_setting_retry_policy_to_statement(self):
        """
        Test to verify that the retry policy is called as expected
        for bound and prepared statements when set to the prepared statement

        @since 3.13
        @jira_ticket PYTHON-861
        @expected_result the appropriate retry policy is called

        @test_category connection
        """
        retry_policy = RetryPolicy()
        self.set_cluster(retry_policy)
        then = {
            "result": "write_timeout",
            "delay_in_ms": 0,
            "consistency_level": "LOCAL_QUORUM",
            "received": 1,
            "block_for": 2,
            "write_type": "SIMPLE",
            "ignore_on_prepare": True
        }
        query_to_prime_prepared = "SELECT * from simulacron_keyspace.simulacron_table WHERE key = :key"
        when = {"params": {"key": "0"}, "param_types": {"key": "ascii"}}
        prime_query(query_to_prime_prepared, when=when, then=then, rows=None, column_types=None)

        counter_policy = CounterRetryPolicy()
        prepared_stmt = self.session.prepare(query_to_prime_prepared)
        prepared_stmt.retry_policy = counter_policy
        self.session.execute(prepared_stmt, ("0",))
        self.assertEqual(next(counter_policy.write_timeout), 1)

        counter_policy.reset_counters()
        bound_stmt = prepared_stmt.bind({"key": "0"})
        bound_stmt.retry_policy = counter_policy
        self.session.execute(bound_stmt)
        self.assertEqual(next(counter_policy.write_timeout), 1)

    def test_retry_policy_on_request_error(self):
        """
        Test to verify that on_request_error is called properly.

        @since 3.18
        @jira_ticket PYTHON-1064
        @expected_result the appropriate retry policy is called

        @test_category connection
        """
        overloaded_error = {
            "result": "overloaded",
            "message": "overloaded"
        }

        bootstrapping_error = {
            "result": "is_bootstrapping",
            "message": "isbootstrapping"
        }

        truncate_error = {
            "result": "truncate_error",
            "message": "truncate_error"
        }

        server_error = {
            "result": "server_error",
            "message": "server_error"
        }

        # Test the on_request_error call
        retry_policy = CounterRetryPolicy()
        self.set_cluster(retry_policy)

        for prime_error, exc in [
            (overloaded_error, OverloadedErrorMessage),
            (bootstrapping_error, IsBootstrappingErrorMessage),
            (truncate_error, TruncateError),
            (server_error, ServerError)]:

            clear_queries()
            query_to_prime = "SELECT * from simulacron_keyspace.simulacron_table;"
            prime_query(query_to_prime, then=prime_error, rows=None, column_types=None)
            rf = self.session.execute_async(query_to_prime)

            with self.assertRaises(exc):
                rf.result()

            self.assertEqual(len(rf.attempted_hosts), 1)  # no retry

        self.assertEqual(next(retry_policy.request_error), 4)

        # Test that by default, retry on next host
        retry_policy = RetryPolicy()
        self.set_cluster(retry_policy)

        for e in [overloaded_error, bootstrapping_error, truncate_error, server_error]:
            clear_queries()
            query_to_prime = "SELECT * from simulacron_keyspace.simulacron_table;"
            prime_query(query_to_prime, then=e, rows=None, column_types=None)
            rf = self.session.execute_async(query_to_prime)

            with self.assertRaises(NoHostAvailable):
                rf.result()

            self.assertEqual(len(rf.attempted_hosts), 3)  # all 3 nodes failed
