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

import os, sys, traceback, logging, ssl, time, math, uuid
from cassandra.cluster import NoHostAvailable
from cassandra.connection import DefaultEndPoint
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from OpenSSL import SSL, crypto

from tests.integration import (
    get_cluster, remove_cluster, use_single_node, start_cluster_wait_for_up, EVENT_LOOP_MANAGER, TestCluster
)

if not hasattr(ssl, 'match_hostname'):
    try:
        from ssl import match_hostname
        ssl.match_hostname = match_hostname
    except ImportError:
        pass  # tests will fail

log = logging.getLogger(__name__)

DEFAULT_PASSWORD = "cassandra"

# Server keystore trust store locations
SERVER_KEYSTORE_PATH = os.path.abspath("tests/integration/long/ssl/127.0.0.1.keystore")
SERVER_TRUSTSTORE_PATH = os.path.abspath("tests/integration/long/ssl/cassandra.truststore")

# Client specific keys/certs
CLIENT_CA_CERTS = os.path.abspath("tests/integration/long/ssl/rootCa.crt")
DRIVER_KEYFILE = os.path.abspath("tests/integration/long/ssl/client.key")
DRIVER_KEYFILE_ENCRYPTED = os.path.abspath("tests/integration/long/ssl/client_encrypted.key")
DRIVER_CERTFILE = os.path.abspath("tests/integration/long/ssl/client.crt_signed")
DRIVER_CERTFILE_BAD = os.path.abspath("tests/integration/long/ssl/client_bad.key")

USES_PYOPENSSL = "twisted" in EVENT_LOOP_MANAGER or "eventlet" in EVENT_LOOP_MANAGER
if "twisted" in EVENT_LOOP_MANAGER:
    import OpenSSL
    ssl_version = OpenSSL.SSL.TLSv1_2_METHOD
    verify_certs = {'cert_reqs': SSL.VERIFY_PEER,
                    'check_hostname': True}
else:
    ssl_version = ssl.PROTOCOL_TLS
    verify_certs = {'cert_reqs': ssl.CERT_REQUIRED,
                    'check_hostname': True}


def verify_callback(connection, x509, errnum, errdepth, ok):
    return ok


def setup_cluster_ssl(client_auth=False):
    """
    We need some custom setup for this module. This will start the ccm cluster with basic
    ssl connectivity, and client authentication if needed.
    """

    use_single_node(start=False)
    ccm_cluster = get_cluster()
    ccm_cluster.stop()

    # Configure ccm to use ssl.
    config_options = {'client_encryption_options': {'enabled': True,
                                                    'keystore': SERVER_KEYSTORE_PATH,
                                                    'keystore_password': DEFAULT_PASSWORD}}

    if(client_auth):
        client_encyrption_options = config_options['client_encryption_options']
        client_encyrption_options['require_client_auth'] = True
        client_encyrption_options['truststore'] = SERVER_TRUSTSTORE_PATH
        client_encyrption_options['truststore_password'] = DEFAULT_PASSWORD

    ccm_cluster.set_configuration_options(config_options)
    start_cluster_wait_for_up(ccm_cluster)


def validate_ssl_options(**kwargs):
        ssl_options = kwargs.get('ssl_options', None)
        ssl_context = kwargs.get('ssl_context', None)
        hostname = kwargs.get('hostname', '127.0.0.1')

        # find absolute path to client CA_CERTS
        tries = 0
        while True:
            if tries > 5:
                raise RuntimeError("Failed to connect to SSL cluster after 5 attempts")
            try:
                cluster = TestCluster(
                    contact_points=[DefaultEndPoint(hostname)],
                    ssl_options=ssl_options,
                    ssl_context=ssl_context
                )
                session = cluster.connect(wait_for_all_pools=True)
                break
            except Exception:
                ex_type, ex, tb = sys.exc_info()
                log.warning("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                del tb
                tries += 1

        # attempt a few simple commands.
        insert_keyspace = """CREATE KEYSPACE ssltest
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}
            """
        statement = SimpleStatement(insert_keyspace)
        statement.consistency_level = 3
        session.execute(statement)

        drop_keyspace = "DROP KEYSPACE ssltest"
        statement = SimpleStatement(drop_keyspace)
        statement.consistency_level = ConsistencyLevel.ANY
        session.execute(statement)

        cluster.shutdown()


class SSLConnectionTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setup_cluster_ssl()

    @classmethod
    def tearDownClass(cls):
        ccm_cluster = get_cluster()
        ccm_cluster.stop()
        remove_cluster()

    def test_can_connect_with_ssl_ca(self):
        """
        Test to validate that we are able to connect to a cluster using ssl.

        test_can_connect_with_ssl_ca performs a simple sanity check to ensure that we can connect to a cluster with ssl
        authentication via simple server-side shared certificate authority. The client is able to validate the identity
        of the server, however by using this method the server can't trust the client unless additional authentication
        has been provided.

        @since 2.6.0
        @jira_ticket PYTHON-332
        @expected_result The client can connect via SSL and preform some basic operations

        @test_category connection:ssl
        """

        # find absolute path to client CA_CERTS
        ssl_options = {'ca_certs': CLIENT_CA_CERTS,'ssl_version': ssl_version}
        validate_ssl_options(ssl_options=ssl_options)

    def test_can_connect_with_ssl_long_running(self):
        """
        Test to validate that long running ssl connections continue to function past thier timeout window

        @since 3.6.0
        @jira_ticket PYTHON-600
        @expected_result The client can connect via SSL and preform some basic operations over a period of longer then a minute

        @test_category connection:ssl
        """

        # find absolute path to client CA_CERTS
        abs_path_ca_cert_path = os.path.abspath(CLIENT_CA_CERTS)
        ssl_options = {'ca_certs': abs_path_ca_cert_path,
                       'ssl_version': ssl_version}
        tries = 0
        while True:
            if tries > 5:
                raise RuntimeError("Failed to connect to SSL cluster after 5 attempts")
            try:
                cluster = TestCluster(ssl_options=ssl_options)
                session = cluster.connect(wait_for_all_pools=True)
                break
            except Exception:
                ex_type, ex, tb = sys.exc_info()
                log.warning("{0}: {1} Backtrace: {2}".format(ex_type.__name__, ex, traceback.extract_tb(tb)))
                del tb
                tries += 1

        # attempt a few simple commands.

        for i in range(8):
            rs = session.execute("SELECT * FROM system.local")
            time.sleep(10)

        cluster.shutdown()

    def test_can_connect_with_ssl_ca_host_match(self):
        """
        Test to validate that we are able to connect to a cluster using ssl, and host matching

        test_can_connect_with_ssl_ca_host_match performs a simple sanity check to ensure that we can connect to a cluster with ssl
        authentication via simple server-side shared certificate authority. It also validates that the host ip matches what is expected

        @since 3.3
        @jira_ticket PYTHON-296
        @expected_result The client can connect via SSL and preform some basic operations, with check_hostname specified

        @test_category connection:ssl
        """

        ssl_options = {'ca_certs': CLIENT_CA_CERTS,
                       'ssl_version': ssl_version}
        ssl_options.update(verify_certs)

        validate_ssl_options(ssl_options=ssl_options)


class SSLConnectionAuthTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setup_cluster_ssl(client_auth=True)

    @classmethod
    def tearDownClass(cls):
        ccm_cluster = get_cluster()
        ccm_cluster.stop()
        remove_cluster()

    def test_can_connect_with_ssl_client_auth(self):
        """
        Test to validate that we can connect to a C* cluster that has client_auth enabled.

        This test will setup and use a c* cluster that has client authentication enabled. It will then attempt
        to connect using valid client keys, and certs (that are in the server's truststore), and attempt to preform some
        basic operations
        @since 2.7.0

        @expected_result The client can connect via SSL and preform some basic operations

        @test_category connection:ssl
        """

        ssl_options = {'ca_certs': CLIENT_CA_CERTS,
                       'ssl_version': ssl_version,
                       'keyfile': DRIVER_KEYFILE,
                       'certfile': DRIVER_CERTFILE}
        validate_ssl_options(ssl_options=ssl_options)

    def test_can_connect_with_ssl_client_auth_host_name(self):
        """
        Test to validate that we can connect to a C* cluster that has client_auth enabled, and hostmatching

        This test will setup and use a c* cluster that has client authentication enabled. It will then attempt
        to connect using valid client keys, and certs (that are in the server's truststore), and attempt to preform some
        basic operations, with check_hostname specified
        @jira_ticket PYTHON-296
        @since 3.3

        @expected_result The client can connect via SSL and preform some basic operations

        @test_category connection:ssl
        """

        ssl_options = {'ca_certs': CLIENT_CA_CERTS,
                       'ssl_version': ssl_version,
                       'keyfile': DRIVER_KEYFILE,
                       'certfile': DRIVER_CERTFILE}
        ssl_options.update(verify_certs)

        validate_ssl_options(ssl_options=ssl_options)

    def test_cannot_connect_without_client_auth(self):
        """
        Test to validate that we cannot connect without client auth.

        This test will omit the keys/certs needed to preform client authentication. It will then attempt to connect
        to a server that has client authentication enabled.

        @since 2.7.0
        @expected_result The client will throw an exception on connect

        @test_category connection:ssl
        """

        cluster = TestCluster(ssl_options={'ca_certs': CLIENT_CA_CERTS,
                                           'ssl_version': ssl_version})

        with self.assertRaises(NoHostAvailable) as _:
            cluster.connect()
        cluster.shutdown()

    def test_cannot_connect_with_bad_client_auth(self):
        """
        Test to validate that we cannot connect with invalid client auth.

        This test will use bad keys/certs to preform client authentication. It will then attempt to connect
        to a server that has client authentication enabled.


        @since 2.7.0
        @expected_result The client will throw an exception on connect

        @test_category connection:ssl
        """

        ssl_options = {'ca_certs': CLIENT_CA_CERTS,
                       'ssl_version': ssl_version,
                       'keyfile': DRIVER_KEYFILE}

        if not USES_PYOPENSSL:
            # I don't set the bad certfile for pyopenssl because it hangs
            ssl_options['certfile'] = DRIVER_CERTFILE_BAD

        cluster = TestCluster(
            ssl_options={'ca_certs': CLIENT_CA_CERTS,
                         'ssl_version': ssl_version,
                         'keyfile': DRIVER_KEYFILE}
        )

        with self.assertRaises(NoHostAvailable) as _:
            cluster.connect()
        cluster.shutdown()

    def test_cannot_connect_with_invalid_hostname(self):
        ssl_options = {'ca_certs': CLIENT_CA_CERTS,
                       'ssl_version': ssl_version,
                       'keyfile': DRIVER_KEYFILE,
                       'certfile': DRIVER_CERTFILE}
        ssl_options.update(verify_certs)

        with self.assertRaises(Exception):
            validate_ssl_options(ssl_options=ssl_options, hostname='localhost')


class SSLSocketErrorTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        setup_cluster_ssl()

    @classmethod
    def tearDownClass(cls):
        ccm_cluster = get_cluster()
        ccm_cluster.stop()
        remove_cluster()

    def test_ssl_want_write_errors_are_retried(self):
        """
        Test that when a socket receives a WANT_WRITE error, the message chunk sending is retried.

        @since 3.17.0
        @jira_ticket PYTHON-891
        @expected_result The query is executed successfully

        @test_category connection:ssl
        """
        ssl_options = {'ca_certs': CLIENT_CA_CERTS,
                       'ssl_version': ssl_version}
        cluster = TestCluster(ssl_options=ssl_options)
        session = cluster.connect(wait_for_all_pools=True)
        try:
            session.execute('drop keyspace ssl_error_test')
        except:
            pass
        session.execute(
            "CREATE KEYSPACE ssl_error_test WITH replication = {'class':'SimpleStrategy','replication_factor':1};")
        session.execute("CREATE TABLE ssl_error_test.big_text (id uuid PRIMARY KEY, data text);")

        params = {
            '0': uuid.uuid4(),
            '1': "0" * int(math.pow(10, 7))
        }

        session.execute('INSERT INTO ssl_error_test.big_text ("id", "data") VALUES (%(0)s, %(1)s)', params)


class SSLConnectionWithSSLContextTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        setup_cluster_ssl()

    @classmethod
    def tearDownClass(cls):
        ccm_cluster = get_cluster()
        ccm_cluster.stop()
        remove_cluster()

    def test_can_connect_with_sslcontext_certificate(self):
        """
        Test to validate that we are able to connect to a cluster using a SSLContext.

        @since 3.17.0
        @jira_ticket PYTHON-995
        @expected_result The client can connect via SSL and preform some basic operations

        @test_category connection:ssl
        """
        if USES_PYOPENSSL:
            ssl_context = SSL.Context(SSL.TLSv1_2_METHOD)
            ssl_context.load_verify_locations(CLIENT_CA_CERTS)
        else:
            ssl_context = ssl.SSLContext(ssl_version)
            ssl_context.load_verify_locations(CLIENT_CA_CERTS)
            ssl_context.verify_mode = ssl.CERT_REQUIRED
        validate_ssl_options(ssl_context=ssl_context)

    def test_can_connect_with_ssl_client_auth_password_private_key(self):
        """
        Identical test to SSLConnectionAuthTests.test_can_connect_with_ssl_client_auth,
        the only difference is that the DRIVER_KEYFILE is encrypted with a password.

        @since 3.17.0
        @jira_ticket PYTHON-995
        @expected_result The client can connect via SSL and preform some basic operations

        @test_category connection:ssl
        """
        abs_driver_keyfile = os.path.abspath(DRIVER_KEYFILE_ENCRYPTED)
        abs_driver_certfile = os.path.abspath(DRIVER_CERTFILE)
        ssl_options = {}

        if USES_PYOPENSSL:
            ssl_context = SSL.Context(SSL.TLSv1_2_METHOD)
            ssl_context.use_certificate_file(abs_driver_certfile)
            with open(abs_driver_keyfile) as keyfile:
                key = crypto.load_privatekey(crypto.FILETYPE_PEM, keyfile.read(), b'cassandra')
            ssl_context.use_privatekey(key)
            ssl_context.set_verify(SSL.VERIFY_NONE, verify_callback)
        else:
            ssl_context = ssl.SSLContext(ssl_version)
            ssl_context.load_cert_chain(certfile=abs_driver_certfile,
                                        keyfile=abs_driver_keyfile,
                                        password="cassandra")
            ssl_context.verify_mode = ssl.CERT_NONE
        validate_ssl_options(ssl_context=ssl_context, ssl_options=ssl_options)

    def test_can_connect_with_ssl_context_ca_host_match(self):
        """
        Test to validate that we are able to connect to a cluster using a SSLContext
        using client auth, an encrypted keyfile, and host matching
        """
        ssl_options = {}
        if USES_PYOPENSSL:
            ssl_context = SSL.Context(SSL.TLSv1_2_METHOD)
            ssl_context.use_certificate_file(DRIVER_CERTFILE)
            with open(DRIVER_KEYFILE_ENCRYPTED) as keyfile:
                key = crypto.load_privatekey(crypto.FILETYPE_PEM, keyfile.read(), b'cassandra')
            ssl_context.use_privatekey(key)
            ssl_context.load_verify_locations(CLIENT_CA_CERTS)
            ssl_options["check_hostname"] = True
        else:
            ssl_context = ssl.SSLContext(ssl_version)
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            ssl_context.load_verify_locations(CLIENT_CA_CERTS)
            ssl_context.load_cert_chain(
                certfile=DRIVER_CERTFILE,
                keyfile=DRIVER_KEYFILE_ENCRYPTED,
                password="cassandra",
            )
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            ssl_options["check_hostname"] = True
        validate_ssl_options(ssl_context=ssl_context, ssl_options=ssl_options)

    def test_cannot_connect_ssl_context_with_invalid_hostname(self):
        ssl_options = {}
        if USES_PYOPENSSL:
            ssl_context = SSL.Context(SSL.TLSv1_2_METHOD)
            ssl_context.use_certificate_file(DRIVER_CERTFILE)
            with open(DRIVER_KEYFILE_ENCRYPTED) as keyfile:
                key = crypto.load_privatekey(crypto.FILETYPE_PEM, keyfile.read(), b"cassandra")
            ssl_context.use_privatekey(key)
            ssl_context.load_verify_locations(CLIENT_CA_CERTS)
            ssl_options["check_hostname"] = True
        else:
            ssl_context = ssl.SSLContext(ssl_version)
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            ssl_context.load_verify_locations(CLIENT_CA_CERTS)
            ssl_context.load_cert_chain(
                certfile=DRIVER_CERTFILE,
                keyfile=DRIVER_KEYFILE_ENCRYPTED,
                password="cassandra",
            )
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            ssl_options["check_hostname"] = True
        with self.assertRaises(Exception):
            validate_ssl_options(ssl_context=ssl_context, ssl_options=ssl_options, hostname="localhost")

    @unittest.skipIf(USES_PYOPENSSL, "This test is for the built-in ssl.Context")
    def test_can_connect_with_sslcontext_default_context(self):
        """
        Test to validate that we are able to connect to a cluster using a SSLContext created from create_default_context().
        @expected_result The client can connect via SSL and preform some basic operations
        @test_category connection:ssl
        """
        ssl_context = ssl.create_default_context(cafile=CLIENT_CA_CERTS)
        validate_ssl_options(ssl_context=ssl_context)
