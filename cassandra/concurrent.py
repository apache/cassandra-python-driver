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
from collections import namedtuple
from concurrent.futures import Future
from heapq import heappush, heappop
from itertools import cycle
from threading import Condition

from cassandra.cluster import ResultSet, EXEC_PROFILE_DEFAULT

log = logging.getLogger(__name__)


ExecutionResult = namedtuple('ExecutionResult', ['success', 'result_or_exc'])

def execute_concurrent(session, statements_and_parameters, concurrency=100, raise_on_first_error=True, results_generator=False, execution_profile=EXEC_PROFILE_DEFAULT):
    """
    See :meth:`.Session.execute_concurrent`.
    """
    if concurrency <= 0:
        raise ValueError("concurrency must be greater than 0")

    if not statements_and_parameters:
        return []

    executor = ConcurrentExecutorGenResults(session, statements_and_parameters, execution_profile) \
        if results_generator else ConcurrentExecutorListResults(session, statements_and_parameters, execution_profile)
    return executor.execute(concurrency, raise_on_first_error)


class _ConcurrentExecutor(object):

    max_error_recursion = 100

    def __init__(self, session, statements_and_params, execution_profile):
        self.session = session
        self._enum_statements = enumerate(iter(statements_and_params))
        self._execution_profile = execution_profile
        self._condition = Condition()
        self._fail_fast = False
        self._results_queue = []
        self._current = 0
        self._exec_count = 0
        self._exec_depth = 0

    def execute(self, concurrency, fail_fast):
        self._fail_fast = fail_fast
        self._results_queue = []
        self._current = 0
        self._exec_count = 0
        with self._condition:
            for n in range(concurrency):
                if not self._execute_next():
                    break
        return self._results()

    def _execute_next(self):
        # lock must be held
        try:
            (idx, (statement, params)) = next(self._enum_statements)
            self._exec_count += 1
            self._execute(idx, statement, params)
            return True
        except StopIteration:
            pass

    def _execute(self, idx, statement, params):
        self._exec_depth += 1
        try:
            future = self.session.execute_async(statement, params, timeout=None, execution_profile=self._execution_profile)
            args = (future, idx)
            future.add_callbacks(
                callback=self._on_success, callback_args=args,
                errback=self._on_error, errback_args=args)
        except Exception as exc:
            # If we're not failing fast and all executions are raising, there is a chance of recursing
            # here as subsequent requests are attempted. If we hit this threshold, schedule this result/retry
            # and let the event loop thread return.
            if self._exec_depth < self.max_error_recursion:
                self._put_result(exc, idx, False)
            else:
                self.session.submit(self._put_result, exc, idx, False)
        self._exec_depth -= 1

    def _on_success(self, result, future, idx):
        future.clear_callbacks()
        self._put_result(ResultSet(future, result), idx, True)

    def _on_error(self, result, future, idx):
        self._put_result(result, idx, False)


class ConcurrentExecutorGenResults(_ConcurrentExecutor):

    def _put_result(self, result, idx, success):
        with self._condition:
            heappush(self._results_queue, (idx, ExecutionResult(success, result)))
            self._execute_next()
            self._condition.notify()

    def _results(self):
        with self._condition:
            while self._current < self._exec_count:
                while not self._results_queue or self._results_queue[0][0] != self._current:
                    self._condition.wait()
                while self._results_queue and self._results_queue[0][0] == self._current:
                    _, res = heappop(self._results_queue)
                    try:
                        self._condition.release()
                        if self._fail_fast and not res[0]:
                            raise res[1]
                        yield res
                    finally:
                        self._condition.acquire()
                    self._current += 1


class ConcurrentExecutorListResults(_ConcurrentExecutor):

    _exception = None

    def execute(self, concurrency, fail_fast):
        self._exception = None
        return super(ConcurrentExecutorListResults, self).execute(concurrency, fail_fast)

    def _put_result(self, result, idx, success):
        self._results_queue.append((idx, ExecutionResult(success, result)))
        with self._condition:
            self._current += 1
            if not success and self._fail_fast:
                if not self._exception:
                    self._exception = result
                self._condition.notify()
            elif not self._execute_next() and self._current == self._exec_count:
                self._condition.notify()

    def _results(self):
        with self._condition:
            while self._current < self._exec_count:
                self._condition.wait()
                if self._exception and self._fail_fast:
                    raise self._exception
        if self._exception and self._fail_fast:  # raise the exception even if there was no wait
            raise self._exception
        return [r[1] for r in sorted(self._results_queue)]



def execute_concurrent_with_args(session, statement, parameters, *args, **kwargs):
    """
    See :meth:`.Session.execute_concurrent_with_args`.
    """
    return execute_concurrent(session, zip(cycle((statement,)), parameters), *args, **kwargs)


class ConcurrentExecutorFutureResults(ConcurrentExecutorListResults):
    def __init__(self, session, statements_and_params, execution_profile, future):
        super().__init__(session, statements_and_params, execution_profile)
        self.future = future

    def _put_result(self, result, idx, success):
        super()._put_result(result, idx, success)
        with self._condition:
            if self._current == self._exec_count:
                if self._exception and self._fail_fast:
                    self.future.set_exception(self._exception)
                else:
                    sorted_results = [r[1] for r in sorted(self._results_queue)]
                    self.future.set_result(sorted_results)


def execute_concurrent_async(
    session,
    statements_and_parameters,
    concurrency=100,
    raise_on_first_error=False,
    execution_profile=EXEC_PROFILE_DEFAULT
):
    """
    See :meth:`.Session.execute_concurrent_async`.
    """
    # Create a Future object and initialize the custom ConcurrentExecutor with the Future
    future = Future()
    executor = ConcurrentExecutorFutureResults(
        session=session,
        statements_and_params=statements_and_parameters,
        execution_profile=execution_profile,
        future=future
    )

    # Execute concurrently
    try:
        executor.execute(concurrency=concurrency, fail_fast=raise_on_first_error)
    except Exception as e:
        future.set_exception(e)

    return future
