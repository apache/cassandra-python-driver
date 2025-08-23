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

from packaging.version import Version
import logging
import time

from cassandra.cluster import NoHostAvailable
from cassandra.auth import PlainTextAuthProvider, SASLClient, SaslAuthProvider

from tests.integration import use_singledc, get_cluster, remove_cluster, PROTOCOL_VERSION, \
    CASSANDRA_IP, CASSANDRA_VERSION, USE_CASS_EXTERNAL, start_cluster_wait_for_up, TestCluster
from tests.integration.util import assert_quiescent_pool_state

import unittest

log = logging.getLogger(__name__)


#This can be tested for remote hosts, but the cluster has to be configured accordingly
#@local


def setup_module():
    if CASSANDRA_IP.startswith("127.0.0.") and not USE_CASS_EXTERNAL:
        use_singledc(start=False)
        ccm_cluster = get_cluster()
        ccm_cluster.stop()
        config_options = {'authenticator': 'PasswordAuthenticator',
                          'authorizer': 'CassandraAuthorizer'}
        ccm_cluster.set_configuration_options(config_options)
        log.debug("Starting ccm test cluster with %s", config_options)
        start_cluster_wait_for_up(ccm_cluster)

    # PYTHON-1328
    #
    # Give the cluster enough time to startup (and perform necessary initialization)
    # before executing the test.
    if CASSANDRA_VERSION > Version('4.0-a'):
        time.sleep(10)

def teardown_module():
    remove_cluster()  # this test messes with config


class AuthenticationTests(unittest.TestCase):

    """
    Tests to cover basic authentication functionality
    """
    def get_authentication_provider(self, username, password):
        """
        Return correct authentication provider based on protocol version.
        There is a difference in the semantics of authentication provider argument with protocol versions 1 and 2
        For protocol version 2 and higher it should be a PlainTextAuthProvider object.
        For protocol version 1 it should be a function taking hostname as an argument and returning a dictionary
        containing username and password.
        :param username: authentication username
        :param password: authentication password
        :return: authentication object suitable for Cluster.connect()
        """
        if PROTOCOL_VERSION < 2:
            return lambda hostname: dict(username=username, password=password)
        else:
            return PlainTextAuthProvider(username=username, password=password)

    def cluster_as(self, usr, pwd):
        # test we can connect at least once with creds
        # to ensure the role manager is setup
        for _ in range(5):
            try:
                cluster = TestCluster(
                    idle_heartbeat_interval=0,
                    auth_provider=self.get_authentication_provider(username='cassandra', password='cassandra'))
                cluster.connect(wait_for_all_pools=True)

                return TestCluster(
                    idle_heartbeat_interval=0,
                    auth_provider=self.get_authentication_provider(username=usr, password=pwd))
            except Exception as e:
                time.sleep(5)

        raise Exception('Unable to connect with creds: {}/{}'.format(usr, pwd))

    def test_auth_connect(self):

        user = 'u'
        passwd = 'password'

        root_session = self.cluster_as('cassandra', 'cassandra').connect()
        root_session.execute('CREATE USER %s WITH PASSWORD %s', (user, passwd))

        try:
            cluster = self.cluster_as(user, passwd)
            session = cluster.connect(wait_for_all_pools=True)
            try:
                self.assertTrue(session.execute('SELECT release_version FROM system.local'))
                assert_quiescent_pool_state(self, cluster, wait=1)
                for pool in session.get_pools():
                    connection, _ = pool.borrow_connection(timeout=0)
                    self.assertEqual(connection.authenticator.server_authenticator_class, 'org.apache.cassandra.auth.PasswordAuthenticator')
                    pool.return_connection(connection)
            finally:
                cluster.shutdown()
        finally:
            root_session.execute('DROP USER %s', user)
            assert_quiescent_pool_state(self, root_session.cluster, wait=1)
            root_session.cluster.shutdown()

    def test_connect_wrong_pwd(self):
        cluster = self.cluster_as('cassandra', 'wrong_pass')
        try:
            self.assertRaisesRegex(NoHostAvailable,
                                    '.*AuthenticationFailed.',
                                    cluster.connect)
            assert_quiescent_pool_state(self, cluster)
        finally:
            cluster.shutdown()

    def test_connect_wrong_username(self):
        cluster = self.cluster_as('wrong_user', 'cassandra')
        try:
            self.assertRaisesRegex(NoHostAvailable,
                                    '.*AuthenticationFailed.*',
                                    cluster.connect)
            assert_quiescent_pool_state(self, cluster)
        finally:
            cluster.shutdown()

    def test_connect_empty_pwd(self):
        cluster = self.cluster_as('Cassandra', '')
        try:
            self.assertRaisesRegex(NoHostAvailable,
                                    '.*AuthenticationFailed.*',
                                    cluster.connect)
            assert_quiescent_pool_state(self, cluster)
        finally:
            cluster.shutdown()

    def test_connect_no_auth_provider(self):
        cluster = TestCluster()
        try:
            self.assertRaisesRegex(NoHostAvailable,
                                    '.*AuthenticationFailed.*',
                                    cluster.connect)
            assert_quiescent_pool_state(self, cluster)
        finally:
            cluster.shutdown()


class SaslAuthenticatorTests(AuthenticationTests):
    """
    Test SaslAuthProvider as PlainText
    """
    def setUp(self):
        if PROTOCOL_VERSION < 2:
            raise unittest.SkipTest('Sasl authentication not available for protocol v1')
        if SASLClient is None:
            raise unittest.SkipTest('pure-sasl is not installed')

    def get_authentication_provider(self, username, password):
        sasl_kwargs = {'service': 'cassandra',
                       'mechanism': 'PLAIN',
                       'qops': ['auth'],
                       'username': username,
                       'password': password}
        return SaslAuthProvider(**sasl_kwargs)

    # these could equally be unit tests
    def test_host_passthrough(self):
        sasl_kwargs = {'service': 'cassandra',
                       'mechanism': 'PLAIN'}
        provider = SaslAuthProvider(**sasl_kwargs)
        host = 'thehostname'
        authenticator = provider.new_authenticator(host)
        self.assertEqual(authenticator.sasl.host, host)

    def test_host_rejected(self):
        sasl_kwargs = {'host': 'something'}
        self.assertRaises(ValueError, SaslAuthProvider, **sasl_kwargs)
