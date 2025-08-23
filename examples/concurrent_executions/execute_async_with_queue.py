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
Inserts multiple rows in a table asynchronously, limiting the amount
of parallel requests with a Queue.
"""

import time
import uuid
import queue

from cassandra.cluster import Cluster


CONCURRENCY_LEVEL = 32
TOTAL_QUERIES = 10000

cluster = Cluster()
session = cluster.connect()

session.execute(("CREATE KEYSPACE IF NOT EXISTS examples "
                 "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1' }"))
session.execute("USE examples")
session.execute("CREATE TABLE IF NOT EXISTS tbl_sample_kv (id uuid, value text, PRIMARY KEY (id))")
prepared_insert = session.prepare("INSERT INTO tbl_sample_kv (id, value) VALUES (?, ?)")


def clear_queue():
    while True:
        try:
            futures.get_nowait().result()
        except queue.Empty:
            break


start = time.time()
futures = queue.Queue(maxsize=CONCURRENCY_LEVEL)

# Chunking way, when the max concurrency level is reached, we
# wait the current chunk of requests to finish
for i in range(TOTAL_QUERIES):
    future = session.execute_async(prepared_insert, (uuid.uuid4(), str(i)))
    try:
        futures.put_nowait(future)
    except queue.Full:
        clear_queue()
        futures.put_nowait(future)

clear_queue()
end = time.time()

print("Finished executing {} queries with a concurrency level of {} in {:.2f} seconds.".
      format(TOTAL_QUERIES, CONCURRENCY_LEVEL, (end-start)))
