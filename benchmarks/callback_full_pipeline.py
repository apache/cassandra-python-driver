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

from itertools import count
from threading import Event

from base import benchmark, BenchmarkThread

log = logging.getLogger(__name__)


sentinel = object()


class Runner(BenchmarkThread):

    def __init__(self, *args, **kwargs):
        BenchmarkThread.__init__(self, *args, **kwargs)
        self.num_started = count()
        self.num_finished = count()
        self.event = Event()

    def insert_next(self, previous_result=sentinel):
        if previous_result is not sentinel:
            if isinstance(previous_result, BaseException):
                log.error("Error on insert: %r", previous_result)
            if next(self.num_finished) >= self.num_queries:
                self.event.set()

        i = next(self.num_started)
        if  i <= self.num_queries:
            key = "{0}-{1}".format(self.thread_num, i)
            future = self.run_query(key, timeout=None)
            future.add_callbacks(self.insert_next, self.insert_next)

    def run(self):
        self.start_profile()

        if self.protocol_version >= 3:
            concurrency = 1000
        else:
            concurrency = 100

        for _ in range(min(concurrency, self.num_queries)):
            self.insert_next()

        self.event.wait()

        self.finish_profile()


if __name__ == "__main__":
    benchmark(Runner)
