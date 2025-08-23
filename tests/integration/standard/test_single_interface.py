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

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from packaging.version import Version
from tests.integration import use_singledc, PROTOCOL_VERSION, \
    remove_cluster, greaterthanorequalcass40, notdse, \
    CASSANDRA_VERSION, DSE_VERSION, TestCluster, DEFAULT_SINGLE_INTERFACE_PORT


def setup_module():
    if not DSE_VERSION and CASSANDRA_VERSION >= Version('4-a'):
        remove_cluster()
        use_singledc(use_single_interface=True)

def teardown_module():
    remove_cluster()


@notdse
@greaterthanorequalcass40
class SingleInterfaceTest(unittest.TestCase):

    def setUp(self):
        self.cluster = TestCluster(port=DEFAULT_SINGLE_INTERFACE_PORT)
        self.session = self.cluster.connect()

    def tearDown(self):
        if self.cluster is not None:
            self.cluster.shutdown()

    def test_single_interface(self):
        """
        Test that we can connect to a multiple hosts bound to a single interface.
        """
        hosts = self.cluster.metadata._hosts
        broadcast_rpc_ports = []
        broadcast_ports = []
        self.assertEqual(len(hosts), 3)
        for endpoint, host in hosts.items():

            self.assertEqual(endpoint.address, host.broadcast_rpc_address)
            self.assertEqual(endpoint.port, host.broadcast_rpc_port)

            if host.broadcast_rpc_port in broadcast_rpc_ports:
                self.fail("Duplicate broadcast_rpc_port")
            broadcast_rpc_ports.append(host.broadcast_rpc_port)
            if host.broadcast_port in broadcast_ports:
                self.fail("Duplicate broadcast_port")
            broadcast_ports.append(host.broadcast_port)

        for _ in range(1, 100):
            self.session.execute(SimpleStatement("select * from system_distributed.view_build_status",
                                                 consistency_level=ConsistencyLevel.ALL))

        for pool in self.session.get_pools():
            self.assertEqual(1, pool.get_state()['open_count'])
