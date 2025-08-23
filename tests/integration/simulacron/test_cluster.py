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

import logging
from packaging.version import Version

import cassandra
from tests.integration.simulacron import SimulacronCluster, SimulacronBase
from tests.integration import (requiressimulacron, PROTOCOL_VERSION, DSE_VERSION, MockLoggingHandler)
from tests.integration.simulacron.utils import prime_query, start_and_prime_singledc

from cassandra import (WriteTimeout, WriteType,
                       ConsistencyLevel, UnresolvableContactPoints)
from cassandra.cluster import Cluster, ControlConnection


PROTOCOL_VERSION = min(4, PROTOCOL_VERSION if (DSE_VERSION is None or DSE_VERSION >= Version('5.0')) else 3)

@requiressimulacron
class ClusterTests(SimulacronCluster):
    def test_writetimeout(self):
        write_type = "UNLOGGED_BATCH"
        consistency = "LOCAL_QUORUM"
        received_responses = 1
        required_responses = 4

        query_to_prime_simple = "SELECT * from simulacron_keyspace.simple"
        then = {
            "result": "write_timeout",
            "delay_in_ms": 0,
            "consistency_level": consistency,
            "received": received_responses,
            "block_for": required_responses,
            "write_type": write_type,
            "ignore_on_prepare": True
        }
        prime_query(query_to_prime_simple, then=then, rows=None, column_types=None)

        with self.assertRaises(WriteTimeout) as assert_raised_context:
            self.session.execute(query_to_prime_simple)
        wt = assert_raised_context.exception
        self.assertEqual(wt.write_type, WriteType.name_to_value[write_type])
        self.assertEqual(wt.consistency, ConsistencyLevel.name_to_value[consistency])
        self.assertEqual(wt.received_responses, received_responses)
        self.assertEqual(wt.required_responses, required_responses)
        self.assertIn(write_type, str(wt))
        self.assertIn(consistency, str(wt))
        self.assertIn(str(received_responses), str(wt))
        self.assertIn(str(required_responses), str(wt))


@requiressimulacron
class ClusterDNSResolutionTests(SimulacronCluster):

    connect = False

    def tearDown(self):
        if self.cluster:
            self.cluster.shutdown()

    def test_connection_with_one_unresolvable_contact_point(self):
        # shouldn't raise anything due to name resolution failures
        self.cluster = Cluster(['127.0.0.1', 'dns.invalid'],
                               protocol_version=PROTOCOL_VERSION,
                               compression=False)

    def test_connection_with_only_unresolvable_contact_points(self):
        with self.assertRaises(UnresolvableContactPoints):
            self.cluster = Cluster(['dns.invalid'],
                                   protocol_version=PROTOCOL_VERSION,
                                   compression=False)


@requiressimulacron
class DuplicateRpcTest(SimulacronCluster):
    connect = False

    def test_duplicate(self):
        mock_handler = MockLoggingHandler()
        logger = logging.getLogger(cassandra.cluster.__name__)
        logger.addHandler(mock_handler)
        address_column = "native_transport_address" if DSE_VERSION and DSE_VERSION > Version("6.0") else "rpc_address"
        rows = [
            {"peer": "127.0.0.1", "data_center": "dc", "host_id": "dontcare1", "rack": "rack1",
             "release_version": "3.11.4", address_column: "127.0.0.1", "schema_version": "dontcare", "tokens": "1"},
            {"peer": "127.0.0.2", "data_center": "dc", "host_id": "dontcare2", "rack": "rack1",
             "release_version": "3.11.4", address_column: "127.0.0.2", "schema_version": "dontcare", "tokens": "2"},
        ]
        prime_query(ControlConnection._SELECT_PEERS, rows=rows)

        cluster = Cluster(protocol_version=PROTOCOL_VERSION, compression=False)
        session = cluster.connect(wait_for_all_pools=True)

        warnings = mock_handler.messages.get("warning")
        self.assertEqual(len(warnings), 1)
        self.assertTrue('multiple hosts with the same endpoint' in warnings[0])
        logger.removeHandler(mock_handler)
        cluster.shutdown()
