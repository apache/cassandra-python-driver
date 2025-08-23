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

from itertools import cycle
from unittest.mock import Mock
import time
import threading
from queue import PriorityQueue
import sys
import platform

from cassandra.cluster import Cluster, Session
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args, execute_concurrent_async
from cassandra.pool import Host
from cassandra.policies import SimpleConvictionPolicy
from tests.unit.utils import mock_session_pools


class MockResponseResponseFuture():
    """
    This is a mock ResponseFuture. It is used to allow us to hook into the underlying session
    and invoke callback with various timing.
    """

    _query_trace = None
    _col_names = None
    _col_types = None

    # a list pending callbacks, these will be prioritized in reverse or normal order
    pending_callbacks = PriorityQueue()

    def __init__(self, reverse):

        # if this is true invoke callback in the reverse order then what they were insert
        self.reverse = reverse
        # hardcoded to avoid paging logic
        self.has_more_pages = False

        if(reverse):
            self.priority = 100
        else:
            self.priority = 0

    def add_callback(self, fn, *args, **kwargs):
        """
        This is used to add a callback our pending list of callbacks.
        If reverse is specified we will invoke the callback in the opposite order that we added it
        """
        time_added = time.time()
        self.pending_callbacks.put((self.priority, (fn, args, kwargs, time_added)))
        if not reversed:
            self.priority += 1
        else:
            self.priority -= 1

    def add_callbacks(self, callback, errback,
                      callback_args=(), callback_kwargs=None,
                      errback_args=(), errback_kwargs=None):

        self.add_callback(callback, *callback_args, **(callback_kwargs or {}))

    def get_next_callback(self):
        return self.pending_callbacks.get()

    def has_next_callback(self):
        return not self.pending_callbacks.empty()

    def has_more_pages(self):
        return False

    def clear_callbacks(self):
        return


class TimedCallableInvoker(threading.Thread):
    """
    This is a local thread which is runs and invokes all the callbacks on the pending callback queue.
    The slowdown flag can used to invoke random slowdowns in our simulate queries.
    """
    def __init__(self, handler, slowdown=False):
        super(TimedCallableInvoker, self).__init__()
        self.slowdown = slowdown
        self._stopper = threading.Event()
        self.handler = handler

    def stop(self):
        self._stopper.set()

    def stopped(self):
        return self._stopper.isSet()

    def run(self):
        while(not self.stopped()):
            if(self.handler.has_next_callback()):
                pending_callback = self.handler.get_next_callback()
                priority_num = pending_callback[0]
                if (priority_num % 10) == 0 and self.slowdown:
                    self._stopper.wait(.1)
                callback_args = pending_callback[1]
                fn, args, kwargs, time_added = callback_args
                fn([time_added], *args, **kwargs)
            self._stopper.wait(.001)
        return

class ConcurrencyTest((unittest.TestCase)):

    def test_results_ordering_forward(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorListResults
        when queries complete in the order they were executed.
        """
        self.insert_and_validate_list_results(False, False)

    def test_results_ordering_reverse(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorListResults
        when queries complete in the reverse order they were executed.
        """
        self.insert_and_validate_list_results(True, False)

    def test_results_ordering_forward_slowdown(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorListResults
        when queries complete in the order they were executed, with slow queries mixed in.
        """
        self.insert_and_validate_list_results(False, True)

    def test_results_ordering_reverse_slowdown(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorListResults
        when queries complete in the reverse order they were executed, with slow queries mixed in.
        """
        self.insert_and_validate_list_results(True, True)

    def test_results_ordering_forward_generator(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorGenResults
        when queries complete in the order they were executed.
        """
        self.insert_and_validate_list_generator(False, False)

    def test_results_ordering_reverse_generator(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorGenResults
        when queries complete in the reverse order they were executed.
        """
        self.insert_and_validate_list_generator(True, False)

    def test_results_ordering_forward_generator_slowdown(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorGenResults
        when queries complete in the order they were executed, with slow queries mixed in.
        """
        self.insert_and_validate_list_generator(False, True)

    def test_results_ordering_reverse_generator_slowdown(self):
        """
        This tests the ordering of our various concurrent generator class ConcurrentExecutorGenResults
        when queries complete in the reverse order they were executed, with slow queries mixed in.
        """
        self.insert_and_validate_list_generator(True, True)

    def insert_and_validate_list_results(self, reverse, slowdown):
        """
        This utility method will execute submit various statements for execution using the ConcurrentExecutorListResults,
        then invoke a separate thread to execute the callback associated with the futures registered
        for those statements. The parameters will toggle various timing, and ordering changes.
        Finally, it will validate that the results were returned in the order they were submitted
        :param reverse: Execute the callbacks in the opposite order that they were submitted
        :param slowdown: Cause intermittent queries to perform slowly
        """
        our_handler = MockResponseResponseFuture(reverse=reverse)
        mock_session = Mock()
        statements_and_params = zip(cycle(["INSERT INTO test3rf.test (k, v) VALUES (%s, 0)"]),
                                    [(i, ) for i in range(100)])
        mock_session.execute_async.return_value = our_handler

        t = TimedCallableInvoker(our_handler, slowdown=slowdown)
        t.start()
        results = execute_concurrent(mock_session, statements_and_params)

        while(not our_handler.pending_callbacks.empty()):
            time.sleep(.01)
        t.stop()
        self.validate_result_ordering(results)

    def insert_and_validate_list_generator(self, reverse, slowdown):
        """
        This utility method will execute submit various statements for execution using the ConcurrentExecutorGenResults,
        then invoke a separate thread to execute the callback associated with the futures registered
        for those statements. The parameters will toggle various timing, and ordering changes.
        Finally, it will validate that the results were returned in the order they were submitted
        :param reverse: Execute the callbacks in the opposite order that they were submitted
        :param slowdown: Cause intermittent queries to perform slowly
        """
        our_handler = MockResponseResponseFuture(reverse=reverse)
        mock_session = Mock()
        statements_and_params = zip(cycle(["INSERT INTO test3rf.test (k, v) VALUES (%s, 0)"]),
                                    [(i, ) for i in range(100)])
        mock_session.execute_async.return_value = our_handler

        t = TimedCallableInvoker(our_handler, slowdown=slowdown)
        t.start()
        try:
            results = execute_concurrent(mock_session, statements_and_params, results_generator=True)
            self.validate_result_ordering(results)
        finally:
            t.stop()

    def validate_result_ordering(self, results):
        """
        This method will validate that the timestamps returned from the result are in order. This indicates that the
        results were returned in the order they were submitted for execution
        :param results:
        """
        last_time_added = 0
        for success, result in results:
            self.assertTrue(success)
            current_time_added = list(result)[0]

            # Windows clock granularity makes this equal most of the time
            if "Windows" in platform.system():
                self.assertLessEqual(last_time_added, current_time_added)
            else:
                self.assertLess(last_time_added, current_time_added)
            last_time_added = current_time_added

    def insert_and_validate_list_async(self, reverse, slowdown):
        """
        This utility method will execute submit various statements for execution using execute_concurrent_async,
        then invoke a separate thread to execute the callback associated with the futures registered
        for those statements. The parameters will toggle various timing, and ordering changes.
        Finally it will validate that the results were returned in the order they were submitted
        :param reverse: Execute the callbacks in the opposite order that they were submitted
        :param slowdown: Cause intermittent queries to perform slowly
        """
        our_handler = MockResponseResponseFuture(reverse=reverse)
        mock_session = Mock()
        statements_and_params = zip(cycle(["INSERT INTO test3rf.test (k, v) VALUES (%s, 0)"]),
                                    [(i, ) for i in range(100)])
        mock_session.execute_async.return_value = our_handler

        t = TimedCallableInvoker(our_handler, slowdown=slowdown)
        t.start()
        try:
            future = execute_concurrent_async(mock_session, statements_and_params)
            results = future.result()
            self.validate_result_ordering(results)
        finally:
            t.stop()

    def test_results_ordering_async_forward(self):
        """
        This tests the ordering of our execute_concurrent_async function
        when queries complete in the order they were executed.
        """
        self.insert_and_validate_list_async(False, False)

    def test_results_ordering_async_reverse(self):
        """
        This tests the ordering of our execute_concurrent_async function
        when queries complete in the reverse order they were executed.
        """
        self.insert_and_validate_list_async(True, False)

    def test_results_ordering_async_forward_slowdown(self):
        """
        This tests the ordering of our execute_concurrent_async function
        when queries complete in the order they were executed, with slow queries mixed in.
        """
        self.insert_and_validate_list_async(False, True)

    def test_results_ordering_async_reverse_slowdown(self):
        """
        This tests the ordering of our execute_concurrent_async function
        when queries complete in the reverse order they were executed, with slow queries mixed in.
        """
        self.insert_and_validate_list_async(True, True)

    @mock_session_pools
    def test_recursion_limited(self):
        """
        Verify that recursion is controlled when raise_on_first_error=False and something is wrong with the query.

        PYTHON-585
        """
        max_recursion = sys.getrecursionlimit()
        s = Session(Cluster(), [Host("127.0.0.1", SimpleConvictionPolicy)])
        self.assertRaises(TypeError, execute_concurrent_with_args, s, "doesn't matter", [('param',)] * max_recursion, raise_on_first_error=True)

        results = execute_concurrent_with_args(s, "doesn't matter", [('param',)] * max_recursion, raise_on_first_error=False)  # previously
        self.assertEqual(len(results), max_recursion)
        for r in results:
            self.assertFalse(r[0])
            self.assertIsInstance(r[1], TypeError)
