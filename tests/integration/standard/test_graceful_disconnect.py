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
Exercises CEP-59 graceful disconnect (CASSANDRA-21191) against a real
cluster: when a node is drained, it sends a GRACEFUL_DISCONNECT event on
every registered connection before closing the transport, and the driver
must drain its connections to that node and fail over without surfacing
any exception to the application.

Requires a server that implements the GRACEFUL_DISCONNECT event; on older
servers the whole module is skipped by the version requirement below.
"""

import threading
import time
import unittest

from tests.integration import (get_node, greaterthanorequalcass70,
                               remove_cluster, requirecassandra,
                               use_cluster, TestCluster)
from tests.util import wait_until

GRACEFUL_DISCONNECT_CLUSTER_NAME = 'graceful_disconnect_cluster'
QUERY = "SELECT * FROM system.local"


def setup_module():
    # The server-side feature is disabled by default:
    use_cluster(GRACEFUL_DISCONNECT_CLUSTER_NAME, [2],
                configuration_options={'graceful_disconnect_enabled': True})


def teardown_module():
    remove_cluster()


@requirecassandra
@greaterthanorequalcass70
class GracefulDisconnectTests(unittest.TestCase):

    def test_fail_over_without_disruption_when_node_drains(self):
        cluster = TestCluster(metrics_enabled=True)
        session = cluster.connect(wait_for_all_pools=True)
        try:
            # Sanity check before the drain:
            session.execute(QUERY)

            # Steady query load for the whole duration of the test,
            # collecting any exception that reaches the application:
            successes = [0]
            failures = []
            stopped = threading.Event()

            def load():
                while not stopped.is_set():
                    try:
                        session.execute(QUERY)
                        successes[0] += 1
                        time.sleep(0.005)
                    except Exception as exc:
                        failures.append(exc)

            load_thread = threading.Thread(target=load,
                                           name='graceful-disconnect-load')
            load_thread.start()

            try:
                # Drain node 2: the server stops accepting new requests and
                # sends GRACEFUL_DISCONNECT on every connection registered
                # for it, then closes the transport.
                get_node(2).nodetool('drain')

                # The driver must have observed the event (this is also the
                # end-to-end check for the metrics counter):
                wait_until(
                    lambda: cluster.metrics.stats.graceful_disconnects > 0,
                    0.5, 60)
                self.assertGreater(cluster.metrics.stats.graceful_disconnects, 0)

                # Queries must keep succeeding after the drain (load fails
                # over to the other node):
                successes_after_event = successes[0]
                wait_until(
                    lambda: successes[0] > successes_after_event + 100,
                    0.5, 60)
                self.assertGreater(successes[0], successes_after_event + 100)
            finally:
                stopped.set()
                load_thread.join(timeout=10)

            self.assertFalse(load_thread.is_alive(),
                             "load thread should have terminated")

            # The whole point of graceful disconnect: the shutdown must be
            # invisible to the application, no request may fail.
            self.assertEqual(
                failures, [],
                "expected no disruptive exceptions, got: %r" % (failures,))
        finally:
            cluster.shutdown()
