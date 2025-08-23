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

import os

from packaging.version import Version

from tests import notwindows
from tests.unit.cython.utils import notcython
from tests.integration import (execute_until_pass,
                               execute_with_long_wait_retry, use_cluster, TestCluster)

import unittest


CCM_IS_DSE = (os.environ.get('CCM_IS_DSE', None) == 'true')


@unittest.skipIf(os.environ.get('CCM_ARGS', None), 'environment has custom CCM_ARGS; skipping')
@notwindows
@notcython  # no need to double up on this test; also __default__ setting doesn't work
class DseCCMClusterTest(unittest.TestCase):
    """
    This class can be executed setting the DSE_VERSION variable, for example:
    DSE_VERSION=5.1.4 python2.7 -m nose tests/integration/standard/test_dse.py
    If CASSANDRA_VERSION is set instead, it will be converted to the corresponding DSE_VERSION
    """

    def test_dse_5x(self):
        self._test_basic(Version('5.1.10'))

    def test_dse_60(self):
        self._test_basic(Version('6.0.2'))

    @unittest.skipUnless(CCM_IS_DSE, 'DSE version unavailable')
    def test_dse_67(self):
        self._test_basic(Version('6.7.0'))

    def _test_basic(self, dse_version):
        """
        Test basic connection and usage
        """
        cluster_name = '{}-{}'.format(
            self.__class__.__name__, dse_version.base_version.replace('.', '_')
        )
        use_cluster(cluster_name=cluster_name, nodes=[3], dse_options={})

        cluster = TestCluster()
        session = cluster.connect()
        result = execute_until_pass(
            session,
            """
            CREATE KEYSPACE clustertests
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)
        self.assertFalse(result)

        result = execute_with_long_wait_retry(
            session,
            """
            CREATE TABLE clustertests.cf0 (
                a text,
                b text,
                c text,
                PRIMARY KEY (a, b)
            )
            """)
        self.assertFalse(result)

        result = session.execute(
            """
            INSERT INTO clustertests.cf0 (a, b, c) VALUES ('a', 'b', 'c')
            """)
        self.assertFalse(result)

        result = session.execute("SELECT * FROM clustertests.cf0")
        self.assertEqual([('a', 'b', 'c')], result)

        execute_with_long_wait_retry(session, "DROP KEYSPACE clustertests")

        cluster.shutdown()
