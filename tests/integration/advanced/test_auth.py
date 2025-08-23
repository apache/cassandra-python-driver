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
import os
import subprocess
import time

from ccmlib.dse_cluster import DseCluster
from nose.plugins.attrib import attr
from packaging.version import Version

from cassandra.auth import (DSEGSSAPIAuthProvider, DSEPlainTextAuthProvider,
                      SaslAuthProvider, TransitionalModePlainTextAuthProvider)
from cassandra.cluster import EXEC_PROFILE_GRAPH_DEFAULT, NoHostAvailable
from cassandra.protocol import Unauthorized
from cassandra.query import SimpleStatement
from tests.integration import (get_cluster, greaterthanorequaldse51,
                               remove_cluster, requiredse, DSE_VERSION, TestCluster)
from tests.integration.advanced import ADS_HOME, use_single_node_with_graph
from tests.integration.advanced.graph import reset_graph, ClassicGraphFixtures


log = logging.getLogger(__name__)


def setup_module():
    if DSE_VERSION:
        use_single_node_with_graph()


def teardown_module():
    if DSE_VERSION:
        remove_cluster()  # this test messes with config


def wait_role_manager_setup_then_execute(session, statements):
    for s in statements:
        exc = None
        for attempt in range(3):
            try:
                session.execute(s)
                break
            except Exception as e:
                exc = e
                time.sleep(5)
        else:  # if we didn't reach `break`
            if exc is not None:
                raise exc


@attr('long')
@requiredse
class BasicDseAuthTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        """
        This will setup the necessary infrastructure to run our authentication tests. It requires the ADS_HOME environment variable
        and our custom embedded apache directory server jar in order to run.
        """
        if not DSE_VERSION:
            return

        clear_kerberos_tickets()
        self.cluster = None

        # Setup variables for various keytab and other files
        self.conf_file_dir = os.path.join(ADS_HOME, "conf/")
        self.krb_conf = os.path.join(self.conf_file_dir, "krb5.conf")
        self.dse_keytab = os.path.join(self.conf_file_dir, "dse.keytab")
        self.dseuser_keytab = os.path.join(self.conf_file_dir, "dseuser.keytab")
        self.cassandra_keytab = os.path.join(self.conf_file_dir, "cassandra.keytab")
        self.bob_keytab = os.path.join(self.conf_file_dir, "bob.keytab")
        self.charlie_keytab = os.path.join(self.conf_file_dir, "charlie.keytab")
        actual_jar = os.path.join(ADS_HOME, "embedded-ads.jar")

        # Create configuration directories if they don't already exist
        if not os.path.exists(self.conf_file_dir):
            os.makedirs(self.conf_file_dir)
        if not os.path.exists(actual_jar):
            raise RuntimeError('could not find {}'.format(actual_jar))
        log.warning("Starting adserver")
        # Start the ADS, this will create the keytab con configuration files listed above
        self.proc = subprocess.Popen(['java', '-jar', actual_jar, '-k', '--confdir', self.conf_file_dir], shell=False)
        time.sleep(10)
        # TODO poll for server to come up

        log.warning("Starting adserver started")
        ccm_cluster = get_cluster()
        log.warning("fetching tickets")
        # Stop cluster if running and configure it with the correct options
        ccm_cluster.stop()
        if isinstance(ccm_cluster, DseCluster):
            # Setup kerberos options in cassandra.yaml
            config_options = {'kerberos_options': {'keytab': self.dse_keytab,
                                                   'service_principal': 'dse/_HOST@DATASTAX.COM',
                                                   'qop': 'auth'},
                              'authentication_options': {'enabled': 'true',
                                                         'default_scheme': 'kerberos',
                                                         'scheme_permissions': 'true',
                                                         'allow_digest_with_kerberos': 'true',
                                                         'plain_text_without_ssl': 'warn',
                                                         'transitional_mode': 'disabled'},
                              'authorization_options': {'enabled': 'true'}}

            krb5java = "-Djava.security.krb5.conf=" + self.krb_conf
            # Setup dse authenticator in cassandra.yaml
            ccm_cluster.set_configuration_options({
                'authenticator': 'com.datastax.bdp.cassandra.auth.DseAuthenticator',
                'authorizer': 'com.datastax.bdp.cassandra.auth.DseAuthorizer'
            })
            ccm_cluster.set_dse_configuration_options(config_options)
            ccm_cluster.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=[krb5java])
        else:
            log.error("Cluster is not dse cluster test will fail")

    @classmethod
    def tearDownClass(self):
        """
        Terminates running ADS (Apache directory server).
        """
        if not DSE_VERSION:
            return

        self.proc.terminate()

    def tearDown(self):
        """
        This will clear any existing kerberos tickets by using kdestroy
        """
        clear_kerberos_tickets()
        if self.cluster:
            self.cluster.shutdown()

    def refresh_kerberos_tickets(self, keytab_file, user_name, krb_conf):
        """
        Fetches a new ticket for using the keytab file and username provided.
        """
        self.ads_pid = subprocess.call(['kinit', '-t', keytab_file, user_name], env={'KRB5_CONFIG': krb_conf}, shell=False)

    def connect_and_query(self, auth_provider, query=None):
        """
        Runs a simple system query with the auth_provided specified.
        """
        os.environ['KRB5_CONFIG'] = self.krb_conf
        self.cluster = TestCluster(auth_provider=auth_provider)
        self.session = self.cluster.connect()
        query = query if query else "SELECT * FROM system.local"
        statement = SimpleStatement(query)
        rs = self.session.execute(statement)
        return rs

    def test_should_not_authenticate_with_bad_user_ticket(self):
        """
        This tests will attempt to authenticate with a user that has a valid ticket, but is not a valid dse user.
        @since 3.20
        @jira_ticket PYTHON-457
        @test_category dse auth
        @expected_result NoHostAvailable exception should be thrown

        """
        self.refresh_kerberos_tickets(self.dseuser_keytab, "dseuser@DATASTAX.COM", self.krb_conf)
        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"])
        self.assertRaises(NoHostAvailable, self.connect_and_query, auth_provider)

    def test_should_not_authenticate_without_ticket(self):
        """
        This tests will attempt to authenticate with a user that is valid but has no ticket
        @since 3.20
        @jira_ticket PYTHON-457
        @test_category dse auth
        @expected_result NoHostAvailable exception should be thrown

        """
        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"])
        self.assertRaises(NoHostAvailable, self.connect_and_query, auth_provider)

    def test_connect_with_kerberos(self):
        """
        This tests will attempt to authenticate with a user that is valid and has a ticket
        @since 3.20
        @jira_ticket PYTHON-457
        @test_category dse auth
        @expected_result Client should be able to connect and run a basic query

        """
        self.refresh_kerberos_tickets(self.cassandra_keytab, "cassandra@DATASTAX.COM", self.krb_conf)
        auth_provider = DSEGSSAPIAuthProvider()
        rs = self.connect_and_query(auth_provider)
        self.assertIsNotNone(rs)
        connections = [c for holders in self.cluster.get_connection_holders() for c in holders.get_connections()]
        # Check to make sure our server_authenticator class is being set appropriate
        for connection in connections:
            self.assertTrue('DseAuthenticator' in connection.authenticator.server_authenticator_class)

    def test_connect_with_kerberos_and_graph(self):
        """
        This tests will attempt to authenticate with a user and execute a graph query
        @since 3.20
        @jira_ticket PYTHON-457
        @test_category dse auth
        @expected_result Client should be able to connect and run a basic graph query with authentication

        """
        self.refresh_kerberos_tickets(self.cassandra_keytab, "cassandra@DATASTAX.COM", self.krb_conf)

        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"])
        rs = self.connect_and_query(auth_provider)
        self.assertIsNotNone(rs)
        reset_graph(self.session, self._testMethodName.lower())
        profiles = self.cluster.profile_manager.profiles
        profiles[EXEC_PROFILE_GRAPH_DEFAULT].graph_options.graph_name = self._testMethodName.lower()
        self.session.execute_graph(ClassicGraphFixtures.classic())

        rs = self.session.execute_graph('g.V()')
        self.assertIsNotNone(rs)

    def test_connect_with_kerberos_host_not_resolved(self):
        """
        This tests will attempt to authenticate with IP, this will fail on osx.
        The success or failure of this test is dependent on a reverse dns lookup which can be impacted by your environment
        if it fails don't panic.
        @since 3.20
        @jira_ticket PYTHON-566
        @test_category dse auth
        @expected_result Client should error when ip is used

        """
        self.refresh_kerberos_tickets(self.cassandra_keytab, "cassandra@DATASTAX.COM", self.krb_conf)
        DSEGSSAPIAuthProvider(service='dse', qops=["auth"], resolve_host_name=False)

    def test_connect_with_explicit_principal(self):
        """
        This tests will attempt to authenticate using valid and invalid user principals
        @since 3.20
        @jira_ticket PYTHON-574
        @test_category dse auth
        @expected_result Client principals should be used by the underlying mechanism

        """

        # Connect with valid principal
        self.refresh_kerberos_tickets(self.cassandra_keytab, "cassandra@DATASTAX.COM", self.krb_conf)
        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"], principal="cassandra@DATASTAX.COM")
        self.connect_and_query(auth_provider)
        connections = [c for holders in self.cluster.get_connection_holders() for c in holders.get_connections()]

        # Check to make sure our server_authenticator class is being set appropriate
        for connection in connections:
            self.assertTrue('DseAuthenticator' in connection.authenticator.server_authenticator_class)

        # Use invalid principal
        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"], principal="notauser@DATASTAX.COM")
        self.assertRaises(NoHostAvailable, self.connect_and_query, auth_provider)

    @greaterthanorequaldse51
    def test_proxy_login_with_kerberos(self):
        """
        Test that the proxy login works with kerberos.
        """
        # Set up users for proxy login test
        self._setup_for_proxy()

        query = "select * from testkrbproxy.testproxy"

        # Try normal login with Charlie
        self.refresh_kerberos_tickets(self.charlie_keytab, "charlie@DATASTAX.COM", self.krb_conf)
        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"], principal="charlie@DATASTAX.COM")
        self.connect_and_query(auth_provider, query=query)

        # Try proxy login with bob
        self.refresh_kerberos_tickets(self.bob_keytab, "bob@DATASTAX.COM", self.krb_conf)
        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"], principal="bob@DATASTAX.COM",
                                              authorization_id='charlie@DATASTAX.COM')
        self.connect_and_query(auth_provider, query=query)

        # Try logging with bob without mentioning charlie
        self.refresh_kerberos_tickets(self.bob_keytab, "bob@DATASTAX.COM", self.krb_conf)
        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"], principal="bob@DATASTAX.COM")
        self.assertRaises(Unauthorized, self.connect_and_query, auth_provider, query=query)

        self._remove_proxy_setup()

    @greaterthanorequaldse51
    def test_proxy_login_with_kerberos_forbidden(self):
        """
        Test that the proxy login fail when proxy role is not granted
        """
        # Set up users for proxy login test
        self._setup_for_proxy(False)
        query = "select * from testkrbproxy.testproxy"

        # Try normal login with Charlie
        self.refresh_kerberos_tickets(self.bob_keytab, "bob@DATASTAX.COM", self.krb_conf)
        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"], principal="bob@DATASTAX.COM",
                                              authorization_id='charlie@DATASTAX.COM')
        self.assertRaises(NoHostAvailable, self.connect_and_query, auth_provider, query=query)

        self.refresh_kerberos_tickets(self.bob_keytab, "bob@DATASTAX.COM", self.krb_conf)
        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"], principal="bob@DATASTAX.COM")
        self.assertRaises(Unauthorized, self.connect_and_query, auth_provider, query=query)

        self._remove_proxy_setup()

    def _remove_proxy_setup(self):
        os.environ['KRB5_CONFIG'] = self.krb_conf
        self.refresh_kerberos_tickets(self.cassandra_keytab, "cassandra@DATASTAX.COM", self.krb_conf)
        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"], principal='cassandra@DATASTAX.COM')
        cluster = TestCluster(auth_provider=auth_provider)
        session = cluster.connect()

        session.execute("REVOKE PROXY.LOGIN ON ROLE '{0}' FROM '{1}'".format('charlie@DATASTAX.COM', 'bob@DATASTAX.COM'))

        session.execute("DROP ROLE IF EXISTS '{0}';".format('bob@DATASTAX.COM'))
        session.execute("DROP ROLE IF EXISTS '{0}';".format('charlie@DATASTAX.COM'))

        # Create a keyspace and allow only charlie to query it.

        session.execute("DROP KEYSPACE testkrbproxy")

        cluster.shutdown()

    def _setup_for_proxy(self, grant=True):
        os.environ['KRB5_CONFIG'] = self.krb_conf
        self.refresh_kerberos_tickets(self.cassandra_keytab, "cassandra@DATASTAX.COM", self.krb_conf)
        auth_provider = DSEGSSAPIAuthProvider(service='dse', qops=["auth"], principal='cassandra@DATASTAX.COM')
        cluster = TestCluster(auth_provider=auth_provider)
        session = cluster.connect()

        stmts = [
            "CREATE ROLE IF NOT EXISTS '{0}' WITH LOGIN = TRUE;".format('bob@DATASTAX.COM'),
            "CREATE ROLE IF NOT EXISTS '{0}' WITH LOGIN = TRUE;".format('bob@DATASTAX.COM'),
            "GRANT EXECUTE ON ALL AUTHENTICATION SCHEMES to 'bob@DATASTAX.COM'",
            "CREATE ROLE IF NOT EXISTS '{0}' WITH LOGIN = TRUE;".format('charlie@DATASTAX.COM'),
            "GRANT EXECUTE ON ALL AUTHENTICATION SCHEMES to 'charlie@DATASTAX.COM'",
            # Create a keyspace and allow only charlie to query it.
            "CREATE KEYSPACE testkrbproxy WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            "CREATE TABLE testkrbproxy.testproxy (id int PRIMARY KEY, value text)",
            "GRANT ALL PERMISSIONS ON KEYSPACE testkrbproxy to '{0}'".format('charlie@DATASTAX.COM'),
        ]

        if grant:
            stmts.append("GRANT PROXY.LOGIN ON ROLE '{0}' to '{1}'".format('charlie@DATASTAX.COM', 'bob@DATASTAX.COM'))

        wait_role_manager_setup_then_execute(session, stmts)

        cluster.shutdown()


def clear_kerberos_tickets():
        subprocess.call(['kdestroy'], shell=False)


@attr('long')
@requiredse
class BaseDseProxyAuthTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        """
        This will setup the necessary infrastructure to run unified authentication tests.
        """
        if not DSE_VERSION or DSE_VERSION < Version('5.1'):
            return
        self.cluster = None

        ccm_cluster = get_cluster()
        # Stop cluster if running and configure it with the correct options
        ccm_cluster.stop()
        if isinstance(ccm_cluster, DseCluster):
            # Setup dse options in dse.yaml
            config_options = {'authentication_options': {'enabled': 'true',
                                                         'default_scheme': 'internal',
                                                         'scheme_permissions': 'true',
                                                         'transitional_mode': 'normal'},
                              'authorization_options': {'enabled': 'true'}
                              }

            # Setup dse authenticator in cassandra.yaml
            ccm_cluster.set_configuration_options({
                'authenticator': 'com.datastax.bdp.cassandra.auth.DseAuthenticator',
                'authorizer': 'com.datastax.bdp.cassandra.auth.DseAuthorizer'
            })
            ccm_cluster.set_dse_configuration_options(config_options)
            ccm_cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        else:
            log.error("Cluster is not dse cluster test will fail")

        # Create users and test keyspace
        self.user_role = 'user1'
        self.server_role = 'server'
        self.root_cluster = TestCluster(auth_provider=DSEPlainTextAuthProvider('cassandra', 'cassandra'))
        self.root_session = self.root_cluster.connect()

        stmts = [
            "CREATE USER {0} WITH PASSWORD '{1}'".format(self.server_role, self.server_role),
            "CREATE USER {0} WITH PASSWORD '{1}'".format(self.user_role, self.user_role),
            "CREATE KEYSPACE testproxy WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            "CREATE TABLE testproxy.testproxy (id int PRIMARY KEY, value text)",
            "GRANT ALL PERMISSIONS ON KEYSPACE testproxy to {0}".format(self.user_role)
        ]

        wait_role_manager_setup_then_execute(self.root_session, stmts)

    @classmethod
    def tearDownClass(self):
        """
        Shutdown the root session.
        """
        if not DSE_VERSION or DSE_VERSION < Version('5.1'):
            return
        self.root_session.execute('DROP KEYSPACE testproxy;')
        self.root_session.execute('DROP USER {0}'.format(self.user_role))
        self.root_session.execute('DROP USER {0}'.format(self.server_role))
        self.root_cluster.shutdown()

    def tearDown(self):
        """
        Shutdown the cluster and reset proxy permissions
        """
        self.cluster.shutdown()

        self.root_session.execute("REVOKE PROXY.LOGIN ON ROLE {0} from {1}".format(self.user_role, self.server_role))
        self.root_session.execute("REVOKE PROXY.EXECUTE ON ROLE {0} from {1}".format(self.user_role, self.server_role))

    def grant_proxy_login(self):
        """
        Grant PROXY.LOGIN permission on a role to a specific user.
        """
        self.root_session.execute("GRANT PROXY.LOGIN on role {0} to {1}".format(self.user_role, self.server_role))

    def grant_proxy_execute(self):
        """
        Grant PROXY.EXECUTE permission on a role to a specific user.
        """
        self.root_session.execute("GRANT PROXY.EXECUTE on role {0} to {1}".format(self.user_role, self.server_role))


@attr('long')
@greaterthanorequaldse51
class DseProxyAuthTest(BaseDseProxyAuthTest):
    """
    Tests Unified Auth. Proxy Login using SASL and Proxy Execute.
    """

    @classmethod
    def get_sasl_options(self, mechanism='PLAIN'):
        sasl_options = {
            "service": 'dse',
            "username": 'server',
            "mechanism": mechanism,
            'password': self.server_role,
            'authorization_id': self.user_role
        }
        return sasl_options

    def connect_and_query(self, auth_provider, execute_as=None, query="SELECT * FROM testproxy.testproxy"):
        self.cluster = TestCluster(auth_provider=auth_provider)
        self.session = self.cluster.connect()
        rs = self.session.execute(query, execute_as=execute_as)
        return rs

    def test_proxy_login_forbidden(self):
        """
        Test that a proxy login is forbidden by default for a user.
        @since 3.20
        @jira_ticket PYTHON-662
        @test_category dse auth
        @expected_result connect and query should not be allowed
        """
        auth_provider = SaslAuthProvider(**self.get_sasl_options())
        with self.assertRaises(Unauthorized):
            self.connect_and_query(auth_provider)

    def test_proxy_login_allowed(self):
        """
        Test that a proxy login is allowed with proper permissions.
        @since 3.20
        @jira_ticket PYTHON-662
        @test_category dse auth
        @expected_result connect and query should be allowed
        """
        auth_provider = SaslAuthProvider(**self.get_sasl_options())
        self.grant_proxy_login()
        self.connect_and_query(auth_provider)

    def test_proxy_execute_forbidden(self):
        """
        Test that a proxy execute is forbidden by default for a user.
        @since 3.20
        @jira_ticket PYTHON-662
        @test_category dse auth
        @expected_result connect and query should not be allowed
        """
        auth_provider = DSEPlainTextAuthProvider(self.server_role, self.server_role)
        with self.assertRaises(Unauthorized):
            self.connect_and_query(auth_provider, execute_as=self.user_role)

    def test_proxy_execute_allowed(self):
        """
        Test that a proxy execute is allowed with proper permissions.
        @since 3.20
        @jira_ticket PYTHON-662
        @test_category dse auth
        @expected_result connect and query should be allowed
        """
        auth_provider = DSEPlainTextAuthProvider(self.server_role, self.server_role)
        self.grant_proxy_execute()
        self.connect_and_query(auth_provider, execute_as=self.user_role)

    def test_connection_with_transitional_mode(self):
        """
        Test that the driver can connect using TransitionalModePlainTextAuthProvider
        @since 3.20
        @jira_ticket PYTHON-831
        @test_category dse auth
        @expected_result connect and query should be allowed
        """
        auth_provider = TransitionalModePlainTextAuthProvider()
        self.assertIsNotNone(self.connect_and_query(auth_provider, query="SELECT * from system.local"))
