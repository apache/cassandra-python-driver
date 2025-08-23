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
from unittest import mock

from cassandra import timestamps
from threading import Thread, Lock


class _TimestampTestMixin(object):

    @mock.patch('cassandra.timestamps.time')
    def _call_and_check_results(self,
                                patched_time_module,
                                system_time_expected_stamp_pairs,
                                timestamp_generator=None):
        """
        For each element in an iterable of (system_time, expected_timestamp)
        pairs, call a :class:`cassandra.timestamps.MonotonicTimestampGenerator`
        with system_times as the underlying time.time() result, then assert
        that the result is expected_timestamp. Skips the check if
        expected_timestamp is None.
        """
        patched_time_module.time = mock.Mock()
        system_times, expected_timestamps = zip(*system_time_expected_stamp_pairs)

        patched_time_module.time.side_effect = system_times
        tsg = timestamp_generator or timestamps.MonotonicTimestampGenerator()

        for expected in expected_timestamps:
            actual = tsg()
            if expected is not None:
                self.assertEqual(actual, expected)

        # assert we patched timestamps.time.time correctly
        with self.assertRaises(StopIteration):
            tsg()


class TestTimestampGeneratorOutput(unittest.TestCase, _TimestampTestMixin):
    """
    Mock time.time and test the output of MonotonicTimestampGenerator.__call__
    given different patterns of changing results.
    """

    def test_timestamps_during_and_after_same_system_time(self):
        """
        Timestamps should increase monotonically over repeated system time.

        Test that MonotonicTimestampGenerator's output increases by 1 when the
        underlying system time is the same, then returns to normal when the
        system time increases again.

        @since 3.8.0
        @expected_result Timestamps should increase monotonically over repeated system time.
        @test_category timing
        """
        self._call_and_check_results(
            system_time_expected_stamp_pairs=(
                (15.0, 15 * 1e6),
                (15.0, 15 * 1e6 + 1),
                (15.0, 15 * 1e6 + 2),
                (15.01, 15.01 * 1e6))
        )

    def test_timestamps_during_and_after_backwards_system_time(self):
        """
        Timestamps should increase monotonically over system time going backwards.

        Test that MonotonicTimestampGenerator's output increases by 1 when the
        underlying system time goes backward, then returns to normal when the
        system time increases again.
        """
        self._call_and_check_results(
            system_time_expected_stamp_pairs=(
                (15.0, 15 * 1e6),
                (13.0, 15 * 1e6 + 1),
                (14.0, 15 * 1e6 + 2),
                (13.5, 15 * 1e6 + 3),
                (15.01, 15.01 * 1e6))
        )


class TestTimestampGeneratorLogging(unittest.TestCase):

    def setUp(self):
        self.log_patcher = mock.patch('cassandra.timestamps.log')
        self.addCleanup(self.log_patcher.stop)
        self.patched_timestamp_log = self.log_patcher.start()

    def assertLastCallArgRegex(self, call, pattern):
        last_warn_args, last_warn_kwargs = call
        self.assertEqual(len(last_warn_args), 1)
        self.assertEqual(len(last_warn_kwargs), 0)
        self.assertRegex(last_warn_args[0], pattern)

    def test_basic_log_content(self):
        """
        Tests there are logs

        @since 3.8.0
        @jira_ticket PYTHON-676
        @expected_result logs

        @test_category timing
        """
        tsg = timestamps.MonotonicTimestampGenerator(
            warning_threshold=1e-6,
            warning_interval=1e-6
        )
        #The units of _last_warn is seconds
        tsg._last_warn = 12

        tsg._next_timestamp(20, tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warning.call_args_list), 0)
        tsg._next_timestamp(16, tsg.last)

        self.assertEqual(len(self.patched_timestamp_log.warning.call_args_list), 1)
        self.assertLastCallArgRegex(
            self.patched_timestamp_log.warning.call_args,
            r'Clock skew detected:.*\b16\b.*\b4\b.*\b20\b'
        )

    def test_disable_logging(self):
        """
        Tests there are no logs when there is a clock skew if logging is disabled

        @since 3.8.0
        @jira_ticket PYTHON-676
        @expected_result no logs

        @test_category timing
        """
        no_warn_tsg = timestamps.MonotonicTimestampGenerator(warn_on_drift=False)

        no_warn_tsg.last = 100
        no_warn_tsg._next_timestamp(99, no_warn_tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warning.call_args_list), 0)

    def test_warning_threshold_respected_no_logging(self):
        """
        Tests there are no logs if `warning_threshold` is not exceeded

        @since 3.8.0
        @jira_ticket PYTHON-676
        @expected_result no logs

        @test_category timing
        """
        tsg = timestamps.MonotonicTimestampGenerator(
            warning_threshold=2e-6,
        )
        tsg.last, tsg._last_warn = 100, 97
        tsg._next_timestamp(98, tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warning.call_args_list), 0)

    def test_warning_threshold_respected_logs(self):
        """
        Tests there are logs if `warning_threshold` is exceeded

        @since 3.8.0
        @jira_ticket PYTHON-676
        @expected_result logs

        @test_category timing
        """
        tsg = timestamps.MonotonicTimestampGenerator(
            warning_threshold=1e-6,
            warning_interval=1e-6
        )
        tsg.last, tsg._last_warn = 100, 97
        tsg._next_timestamp(98, tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warning.call_args_list), 1)

    def test_warning_interval_respected_no_logging(self):
        """
        Tests there is only one log in the interval `warning_interval`

        @since 3.8.0
        @jira_ticket PYTHON-676
        @expected_result one log

        @test_category timing
        """
        tsg = timestamps.MonotonicTimestampGenerator(
            warning_threshold=1e-6,
            warning_interval=2e-6
        )
        tsg.last = 100
        tsg._next_timestamp(70, tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warning.call_args_list), 1)

        tsg._next_timestamp(71, tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warning.call_args_list), 1)

    def test_warning_interval_respected_logs(self):
        """
        Tests there are logs again if the
        clock skew happens after`warning_interval`

        @since 3.8.0
        @jira_ticket PYTHON-676
        @expected_result logs

        @test_category timing
        """
        tsg = timestamps.MonotonicTimestampGenerator(
            warning_interval=1e-6,
            warning_threshold=1e-6,
        )
        tsg.last = 100
        tsg._next_timestamp(70, tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warning.call_args_list), 1)

        tsg._next_timestamp(72, tsg.last)
        self.assertEqual(len(self.patched_timestamp_log.warning.call_args_list), 2)


class TestTimestampGeneratorMultipleThreads(unittest.TestCase):

    def test_should_generate_incrementing_timestamps_for_all_threads(self):
        """
        Tests when time is "stopped", values are assigned incrementally

        @since 3.8.0
        @jira_ticket PYTHON-676
        @expected_result the returned values increase

        @test_category timing
        """
        lock = Lock()

        def request_time():
            for _ in range(timestamp_to_generate):
                timestamp = tsg()
                with lock:
                    generated_timestamps.append(timestamp)

        tsg = timestamps.MonotonicTimestampGenerator()
        fixed_time = 1
        num_threads = 5

        timestamp_to_generate = 1000
        generated_timestamps = []

        with mock.patch('time.time', new=mock.Mock(return_value=fixed_time)):
            threads = []
            for _ in range(num_threads):
                threads.append(Thread(target=request_time))

            for t in threads:
                t.start()

            for t in threads:
                t.join()

            self.assertEqual(len(generated_timestamps), num_threads * timestamp_to_generate)
            for i, timestamp in enumerate(sorted(generated_timestamps)):
                self.assertEqual(int(i + 1e6), timestamp)
