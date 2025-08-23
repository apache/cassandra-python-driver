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

import os, socket, errno
from ccmlib import common

from cassandra.cluster import NoHostAvailable
from cassandra.io.asyncorereactor import AsyncoreConnection

from tests import is_monkey_patched
from tests.integration import use_cluster, remove_cluster, TestCluster

if is_monkey_patched():
    LibevConnection = -1
    AsyncoreConnection = -1
else:
    try:
        from cassandra.io.libevreactor import LibevConnection
    except ImportError:
        LibevConnection = None

import unittest


# If more modules do IPV6 testing, this can be moved down to integration.__init__.
# For now, just keeping the clutter here
IPV6_CLUSTER_NAME = 'ipv6_test_cluster'


def setup_module(module):
    if os.name != "nt":
        validate_host_viable()
        # We use a dedicated cluster (instead of common singledc, as in other tests) because
        # it's most likely that the test host will only have one local ipv6 address (::1)
        # singledc has three
        use_cluster(IPV6_CLUSTER_NAME, [1], ipformat='::%d')


def teardown_module():
    remove_cluster()


def validate_ccm_viable():
    try:
        common.normalize_interface(('::1', 0))
    except:
        raise unittest.SkipTest('this version of ccm does not support ipv6')


def validate_host_viable():
    # this is something ccm does when starting, but preemptively check to avoid
    # spinning up the cluster if it's not going to work
    try:
        common.assert_socket_available(('::1', 9042))
    except:
        raise unittest.SkipTest('failed binding ipv6 loopback ::1 on 9042')


class IPV6ConnectionTest(object):

    connection_class = None

    def test_connect(self):
        cluster = TestCluster(connection_class=self.connection_class, contact_points=['::1'], connect_timeout=10)
        session = cluster.connect()
        future = session.execute_async("SELECT * FROM system.local")
        future.result()
        self.assertEqual(future._current_host.address, '::1')
        cluster.shutdown()

    def test_error(self):
        cluster = TestCluster(connection_class=self.connection_class, contact_points=['::1'], port=9043,
                              connect_timeout=10)
        self.assertRaisesRegex(NoHostAvailable, '\(\'Unable to connect.*%s.*::1\', 9043.*Connection refused.*'
                                % errno.ECONNREFUSED, cluster.connect)

    def test_error_multiple(self):
        if len(socket.getaddrinfo('localhost', 9043, socket.AF_UNSPEC, socket.SOCK_STREAM)) < 2:
            raise unittest.SkipTest('localhost only resolves one address')
        cluster = TestCluster(connection_class=self.connection_class, contact_points=['localhost'], port=9043,
                              connect_timeout=10)
        self.assertRaisesRegex(NoHostAvailable, '\(\'Unable to connect.*Tried connecting to \[\(.*\(.*\].*Last error',
                                cluster.connect)


class LibevConnectionTests(IPV6ConnectionTest, unittest.TestCase):

    connection_class = LibevConnection

    def setUp(self):
        if os.name == "nt":
            raise unittest.SkipTest("IPv6 is currently not supported under Windows")

        if LibevConnection == -1:
            raise unittest.SkipTest("Can't test libev with monkey patching")
        elif LibevConnection is None:
            raise unittest.SkipTest("Libev does not appear to be installed properly")


class AsyncoreConnectionTests(IPV6ConnectionTest, unittest.TestCase):

    connection_class = AsyncoreConnection

    def setUp(self):
        if os.name == "nt":
            raise unittest.SkipTest("IPv6 is currently not supported under Windows")

        if AsyncoreConnection == -1:
            raise unittest.SkipTest("Can't test asyncore with monkey patching")
