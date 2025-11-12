# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License
import unittest

import time
import subprocess
import logging

from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.endpoint import UnixSocketEndPoint
from cassandra.policies import WhiteListRoundRobinPolicy, RoundRobinPolicy

from tests import notwindows
from tests.integration import use_single_node, TestCluster

log = logging.getLogger()
log.setLevel('DEBUG')

UNIX_SOCKET_PATH = '/tmp/cass.sock'


def setup_module():
    use_single_node()


class UnixSocketWhiteListRoundRobinPolicy(WhiteListRoundRobinPolicy):
    def __init__(self, hosts):
        self._allowed_hosts = self._allowed_hosts_resolved = tuple(hosts)
        RoundRobinPolicy.__init__(self)


@notwindows
class UnixSocketTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        log.debug("Starting socat...")
        cls.proc = subprocess.Popen(
            ['socat',
             'UNIX-LISTEN:%s,fork' % UNIX_SOCKET_PATH,
             'TCP:localhost:9042'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)

        time.sleep(1)
        if cls.proc.poll() is not None:
            for line in cls.proc.stdout.readlines():
                log.debug("socat: " + line.decode('utf-8'))
            raise Exception("Error while starting socat. Return code: %d" % cls.proc.returncode)

        lbp = UnixSocketWhiteListRoundRobinPolicy([UNIX_SOCKET_PATH])
        ep = ExecutionProfile(load_balancing_policy=lbp)
        endpoint = UnixSocketEndPoint(UNIX_SOCKET_PATH)
        cls.cluster = TestCluster(contact_points=[endpoint], execution_profiles={EXEC_PROFILE_DEFAULT: ep})

    @classmethod
    def tearDownClass(cls):
        cls.cluster.shutdown()
        cls.proc.terminate()

    def test_unix_socket_connection(self):
        s = self.cluster.connect()
        s.execute('select * from system.local')
