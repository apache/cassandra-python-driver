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

"""
Inserts multiple rows in a table limiting the amount of parallel requests.

Note that the driver also provide convenient utility functions to accomplish this.
See https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/concurrent/
"""

import time
import uuid
import threading
from cassandra.cluster import Cluster


CONCURRENCY_LEVEL = 32
TOTAL_QUERIES = 10000
COUNTER = 0
COUNTER_LOCK = threading.Lock()

cluster = Cluster()
session = cluster.connect()

session.execute(("CREATE KEYSPACE IF NOT EXISTS examples "
                 "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' }"))
session.execute("USE examples")
session.execute("CREATE TABLE IF NOT EXISTS tbl_sample_kv (id uuid, value text, PRIMARY KEY (id))")
prepared_insert = session.prepare("INSERT INTO tbl_sample_kv (id, value) VALUES (?, ?)")


class SimpleQueryExecutor(threading.Thread):

    def run(self):
        global COUNTER

        while True:
            with COUNTER_LOCK:
                current = COUNTER
                COUNTER += 1

            if current >= TOTAL_QUERIES:
                break

            session.execute(prepared_insert, (uuid.uuid4(), str(current)))


# Launch in parallel n async operations (n being the concurrency level)
start = time.time()
threads = []
for i in range(CONCURRENCY_LEVEL):
    t = SimpleQueryExecutor()
    threads.append(t)
    t.start()

for thread in threads:
    thread.join()
end = time.time()

print("Finished executing {} queries with a concurrency level of {} in {:.2f} seconds.".
      format(TOTAL_QUERIES, CONCURRENCY_LEVEL, (end-start)))
