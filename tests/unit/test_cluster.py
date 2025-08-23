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
import socket
import uuid

from unittest.mock import patch, Mock

from cassandra import ConsistencyLevel, DriverException, Timeout, Unavailable, RequestExecutionException, ReadTimeout, WriteTimeout, CoordinationFailure, ReadFailure, WriteFailure, FunctionFailure, AlreadyExists,\
    InvalidRequest, Unauthorized, AuthenticationFailed, OperationTimedOut, UnsupportedOperation, RequestValidationException, ConfigurationException, ProtocolVersion
from cassandra.cluster import _Scheduler, Session, Cluster, default_lbp_factory, \
    ExecutionProfile, _ConfigMode, EXEC_PROFILE_DEFAULT
from cassandra.connection import SniEndPoint, SniEndPointFactory
from cassandra.pool import Host
from cassandra.policies import HostDistance, RetryPolicy, RoundRobinPolicy, DowngradingConsistencyRetryPolicy, SimpleConvictionPolicy
from cassandra.query import SimpleStatement, named_tuple_factory, tuple_factory
from tests.unit.utils import mock_session_pools
from tests import connection_class


log = logging.getLogger(__name__)


class ExceptionTypeTest(unittest.TestCase):

    def test_exception_types(self):
        """
        PYTHON-443
        Sanity check to ensure we don't unintentionally change class hierarchy of exception types
        """
        self.assertTrue(issubclass(Unavailable, DriverException))
        self.assertTrue(issubclass(Unavailable, RequestExecutionException))

        self.assertTrue(issubclass(ReadTimeout, DriverException))
        self.assertTrue(issubclass(ReadTimeout, RequestExecutionException))
        self.assertTrue(issubclass(ReadTimeout, Timeout))

        self.assertTrue(issubclass(WriteTimeout, DriverException))
        self.assertTrue(issubclass(WriteTimeout, RequestExecutionException))
        self.assertTrue(issubclass(WriteTimeout, Timeout))

        self.assertTrue(issubclass(CoordinationFailure, DriverException))
        self.assertTrue(issubclass(CoordinationFailure, RequestExecutionException))

        self.assertTrue(issubclass(ReadFailure, DriverException))
        self.assertTrue(issubclass(ReadFailure, RequestExecutionException))
        self.assertTrue(issubclass(ReadFailure, CoordinationFailure))

        self.assertTrue(issubclass(WriteFailure, DriverException))
        self.assertTrue(issubclass(WriteFailure, RequestExecutionException))
        self.assertTrue(issubclass(WriteFailure, CoordinationFailure))

        self.assertTrue(issubclass(FunctionFailure, DriverException))
        self.assertTrue(issubclass(FunctionFailure, RequestExecutionException))

        self.assertTrue(issubclass(RequestValidationException, DriverException))

        self.assertTrue(issubclass(ConfigurationException, DriverException))
        self.assertTrue(issubclass(ConfigurationException, RequestValidationException))

        self.assertTrue(issubclass(AlreadyExists, DriverException))
        self.assertTrue(issubclass(AlreadyExists, RequestValidationException))
        self.assertTrue(issubclass(AlreadyExists, ConfigurationException))

        self.assertTrue(issubclass(InvalidRequest, DriverException))
        self.assertTrue(issubclass(InvalidRequest, RequestValidationException))

        self.assertTrue(issubclass(Unauthorized, DriverException))
        self.assertTrue(issubclass(Unauthorized, RequestValidationException))

        self.assertTrue(issubclass(AuthenticationFailed, DriverException))

        self.assertTrue(issubclass(OperationTimedOut, DriverException))

        self.assertTrue(issubclass(UnsupportedOperation, DriverException))


class MockOrderedPolicy(RoundRobinPolicy):
    all_hosts = set()

    def make_query_plan(self, working_keyspace=None, query=None):
        return sorted(self.all_hosts, key=lambda x: x.endpoint.ssl_options['server_hostname'])

class ClusterTest(unittest.TestCase):

    def test_tuple_for_contact_points(self):
        cluster = Cluster(contact_points=[('localhost', 9045), ('127.0.0.2', 9046), '127.0.0.3'], port=9999)
        localhost_addr = set([addr[0] for addr in [t for (_,_,_,_,t) in socket.getaddrinfo("localhost",80)]])
        for cp in cluster.endpoints_resolved:
            if cp.address in localhost_addr:
                self.assertEqual(cp.port, 9045)
            elif cp.address == '127.0.0.2':
                self.assertEqual(cp.port, 9046)
            else:
                self.assertEqual(cp.address, '127.0.0.3')
                self.assertEqual(cp.port, 9999)

    def test_invalid_contact_point_types(self):
        with self.assertRaises(ValueError):
            Cluster(contact_points=[None], protocol_version=4, connect_timeout=1)
        with self.assertRaises(TypeError):
            Cluster(contact_points="not a sequence", protocol_version=4, connect_timeout=1)

    def test_requests_in_flight_threshold(self):
        d = HostDistance.LOCAL
        mn = 3
        mx = 5
        c = Cluster(protocol_version=2)
        c.set_min_requests_per_connection(d, mn)
        c.set_max_requests_per_connection(d, mx)
        # min underflow, max, overflow
        for n in (-1, mx, 127):
            self.assertRaises(ValueError, c.set_min_requests_per_connection, d, n)
        # max underflow, under min, overflow
        for n in (0, mn, 128):
            self.assertRaises(ValueError, c.set_max_requests_per_connection, d, n)

    # Validate that at least the default LBP can create a query plan with end points that resolve
    # to different addresses initially. This may not be exactly how things play out in practice
    # (the control connection will muck with this even if nothing else does) but it should be
    # a pretty good approximation.
    def test_query_plan_for_sni_contains_unique_addresses(self):
        node_cnt = 5
        def _mocked_proxy_dns_resolution(self):
            return [(socket.AF_UNIX, socket.SOCK_STREAM, 0, None, ('127.0.0.%s' % (i,), 9042)) for i in range(node_cnt)]

        c = Cluster()
        lbp = c.load_balancing_policy
        lbp.local_dc = "dc1"
        factory = SniEndPointFactory("proxy.foo.bar", 9042)
        for host in (Host(factory.create({"host_id": uuid.uuid4().hex, "dc": "dc1"}), SimpleConvictionPolicy) for _ in range(node_cnt)):
            lbp.on_up(host)
        with patch.object(SniEndPoint, '_resolve_proxy_addresses', _mocked_proxy_dns_resolution):
            addrs = [host.endpoint.resolve() for host in lbp.make_query_plan()]
            # single SNI endpoint should be resolved to multiple unique IP addresses
            self.assertEqual(len(addrs), len(set(addrs)))


class SchedulerTest(unittest.TestCase):
    # TODO: this suite could be expanded; for now just adding a test covering a ticket

    @patch('time.time', return_value=3)  # always queue at same time
    @patch('cassandra.cluster._Scheduler.run')  # don't actually run the thread
    def test_event_delay_timing(self, *_):
        """
        Schedule something with a time collision to make sure the heap comparison works

        PYTHON-473
        """
        sched = _Scheduler(None)
        sched.schedule(0, lambda: None)
        sched.schedule(0, lambda: None)  # pre-473: "TypeError: unorderable types: function() < function()"t


class SessionTest(unittest.TestCase):
    def setUp(self):
        if connection_class is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        connection_class.initialize_reactor()

    # TODO: this suite could be expanded; for now just adding a test covering a PR
    @mock_session_pools
    def test_default_serial_consistency_level_ep(self, *_):
        """
        Make sure default_serial_consistency_level passes through to a query message using execution profiles.
        Also make sure Statement.serial_consistency_level overrides the default.

        PR #510
        """
        c = Cluster(protocol_version=4)
        s = Session(c, [Host("127.0.0.1", SimpleConvictionPolicy)])

        # default is None
        default_profile = c.profile_manager.default
        self.assertIsNone(default_profile.serial_consistency_level)

        for cl in (None, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL):
            s.get_execution_profile(EXEC_PROFILE_DEFAULT).serial_consistency_level = cl

            # default is passed through
            f = s.execute_async(query='')
            self.assertEqual(f.message.serial_consistency_level, cl)

            # any non-None statement setting takes precedence
            for cl_override in (ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL):
                f = s.execute_async(SimpleStatement(query_string='', serial_consistency_level=cl_override))
                self.assertEqual(default_profile.serial_consistency_level, cl)
                self.assertEqual(f.message.serial_consistency_level, cl_override)

    @mock_session_pools
    def test_default_serial_consistency_level_legacy(self, *_):
        """
        Make sure default_serial_consistency_level passes through to a query message using legacy settings.
        Also make sure Statement.serial_consistency_level overrides the default.

        PR #510
        """
        c = Cluster(protocol_version=4)
        s = Session(c, [Host("127.0.0.1", SimpleConvictionPolicy)])

        # default is None
        self.assertIsNone(s.default_serial_consistency_level)

        # Should fail
        with self.assertRaises(ValueError):
            s.default_serial_consistency_level = ConsistencyLevel.ANY
        with self.assertRaises(ValueError):
            s.default_serial_consistency_level = 1001

        for cl in (None, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL):
            s.default_serial_consistency_level = cl

            # any non-None statement setting takes precedence
            for cl_override in (ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL):
                f = s.execute_async(SimpleStatement(query_string='', serial_consistency_level=cl_override))
                self.assertEqual(s.default_serial_consistency_level, cl)
                self.assertEqual(f.message.serial_consistency_level, cl_override)


class ProtocolVersionTests(unittest.TestCase):

    def test_protocol_downgrade_test(self):
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.DSE_V2)
        self.assertEqual(ProtocolVersion.DSE_V1, lower)
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.DSE_V1)
        self.assertEqual(ProtocolVersion.V5,lower)
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.V5)
        self.assertEqual(ProtocolVersion.V4,lower)
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.V4)
        self.assertEqual(ProtocolVersion.V3,lower)
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.V3)
        self.assertEqual(ProtocolVersion.V2,lower)
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.V2)
        self.assertEqual(ProtocolVersion.V1, lower)
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.V1)
        self.assertEqual(0, lower)

        self.assertTrue(ProtocolVersion.uses_error_code_map(ProtocolVersion.DSE_V1))
        self.assertTrue(ProtocolVersion.uses_int_query_flags(ProtocolVersion.DSE_V1))

        self.assertFalse(ProtocolVersion.uses_error_code_map(ProtocolVersion.V4))
        self.assertFalse(ProtocolVersion.uses_int_query_flags(ProtocolVersion.V4))


class ExecutionProfileTest(unittest.TestCase):
    def setUp(self):
        if connection_class is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        connection_class.initialize_reactor()

    def _verify_response_future_profile(self, rf, prof):
        self.assertEqual(rf._load_balancer, prof.load_balancing_policy)
        self.assertEqual(rf._retry_policy, prof.retry_policy)
        self.assertEqual(rf.message.consistency_level, prof.consistency_level)
        self.assertEqual(rf.message.serial_consistency_level, prof.serial_consistency_level)
        self.assertEqual(rf.timeout, prof.request_timeout)
        self.assertEqual(rf.row_factory, prof.row_factory)

    @mock_session_pools
    def test_default_exec_parameters(self):
        cluster = Cluster()
        self.assertEqual(cluster._config_mode, _ConfigMode.UNCOMMITTED)
        self.assertEqual(cluster.load_balancing_policy.__class__, default_lbp_factory().__class__)
        self.assertEqual(cluster.profile_manager.default.load_balancing_policy.__class__, default_lbp_factory().__class__)
        self.assertEqual(cluster.default_retry_policy.__class__, RetryPolicy)
        self.assertEqual(cluster.profile_manager.default.retry_policy.__class__, RetryPolicy)
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])
        self.assertEqual(session.default_timeout, 10.0)
        self.assertEqual(cluster.profile_manager.default.request_timeout, 10.0)
        self.assertEqual(session.default_consistency_level, ConsistencyLevel.LOCAL_ONE)
        self.assertEqual(cluster.profile_manager.default.consistency_level, ConsistencyLevel.LOCAL_ONE)
        self.assertEqual(session.default_serial_consistency_level, None)
        self.assertEqual(cluster.profile_manager.default.serial_consistency_level, None)
        self.assertEqual(session.row_factory, named_tuple_factory)
        self.assertEqual(cluster.profile_manager.default.row_factory, named_tuple_factory)

    @mock_session_pools
    def test_default_legacy(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), default_retry_policy=DowngradingConsistencyRetryPolicy())
        self.assertEqual(cluster._config_mode, _ConfigMode.LEGACY)
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])
        session.default_timeout = 3.7
        session.default_consistency_level = ConsistencyLevel.ALL
        session.default_serial_consistency_level = ConsistencyLevel.SERIAL
        rf = session.execute_async("query")
        expected_profile = ExecutionProfile(cluster.load_balancing_policy, cluster.default_retry_policy,
                                            session.default_consistency_level, session.default_serial_consistency_level,
                                            session.default_timeout, session.row_factory)
        self._verify_response_future_profile(rf, expected_profile)

    @mock_session_pools
    def test_default_profile(self):
        non_default_profile = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(2)])
        cluster = Cluster(execution_profiles={'non-default': non_default_profile})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])

        self.assertEqual(cluster._config_mode, _ConfigMode.PROFILES)

        default_profile = cluster.profile_manager.profiles[EXEC_PROFILE_DEFAULT]
        rf = session.execute_async("query")
        self._verify_response_future_profile(rf, default_profile)

        rf = session.execute_async("query", execution_profile='non-default')
        self._verify_response_future_profile(rf, non_default_profile)

        for name, ep in cluster.profile_manager.profiles.items():
            self.assertEqual(ep, session.get_execution_profile(name))

        # invalid ep
        with self.assertRaises(ValueError):
            session.get_execution_profile('non-existent')

    def test_serial_consistency_level_validation(self):
        # should pass
        ep = ExecutionProfile(RoundRobinPolicy(), serial_consistency_level=ConsistencyLevel.SERIAL)
        ep = ExecutionProfile(RoundRobinPolicy(), serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL)

        # should not pass
        with self.assertRaises(ValueError):
            ep = ExecutionProfile(RoundRobinPolicy(), serial_consistency_level=ConsistencyLevel.ANY)
        with self.assertRaises(ValueError):
            ep = ExecutionProfile(RoundRobinPolicy(), serial_consistency_level=42)

    @mock_session_pools
    def test_statement_params_override_legacy(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), default_retry_policy=DowngradingConsistencyRetryPolicy())
        self.assertEqual(cluster._config_mode, _ConfigMode.LEGACY)
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])

        ss = SimpleStatement("query", retry_policy=DowngradingConsistencyRetryPolicy(),
                             consistency_level=ConsistencyLevel.ALL, serial_consistency_level=ConsistencyLevel.SERIAL)
        my_timeout = 1.1234

        self.assertNotEqual(ss.retry_policy.__class__, cluster.default_retry_policy)
        self.assertNotEqual(ss.consistency_level, session.default_consistency_level)
        self.assertNotEqual(ss._serial_consistency_level, session.default_serial_consistency_level)
        self.assertNotEqual(my_timeout, session.default_timeout)

        rf = session.execute_async(ss, timeout=my_timeout)
        expected_profile = ExecutionProfile(load_balancing_policy=cluster.load_balancing_policy, retry_policy=ss.retry_policy,
                                            request_timeout=my_timeout, consistency_level=ss.consistency_level,
                                            serial_consistency_level=ss._serial_consistency_level)
        self._verify_response_future_profile(rf, expected_profile)

    @mock_session_pools
    def test_statement_params_override_profile(self):
        non_default_profile = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(2)])
        cluster = Cluster(execution_profiles={'non-default': non_default_profile})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])

        self.assertEqual(cluster._config_mode, _ConfigMode.PROFILES)

        rf = session.execute_async("query", execution_profile='non-default')

        ss = SimpleStatement("query", retry_policy=DowngradingConsistencyRetryPolicy(),
                             consistency_level=ConsistencyLevel.ALL, serial_consistency_level=ConsistencyLevel.SERIAL)
        my_timeout = 1.1234

        self.assertNotEqual(ss.retry_policy.__class__, rf._load_balancer.__class__)
        self.assertNotEqual(ss.consistency_level, rf.message.consistency_level)
        self.assertNotEqual(ss._serial_consistency_level, rf.message.serial_consistency_level)
        self.assertNotEqual(my_timeout, rf.timeout)

        rf = session.execute_async(ss, timeout=my_timeout, execution_profile='non-default')
        expected_profile = ExecutionProfile(non_default_profile.load_balancing_policy, ss.retry_policy,
                                            ss.consistency_level, ss._serial_consistency_level, my_timeout, non_default_profile.row_factory)
        self._verify_response_future_profile(rf, expected_profile)

    @mock_session_pools
    def test_no_profile_with_legacy(self):
        # don't construct with both
        self.assertRaises(ValueError, Cluster, load_balancing_policy=RoundRobinPolicy(), execution_profiles={'a': ExecutionProfile()})
        self.assertRaises(ValueError, Cluster, default_retry_policy=DowngradingConsistencyRetryPolicy(), execution_profiles={'a': ExecutionProfile()})
        self.assertRaises(ValueError, Cluster, load_balancing_policy=RoundRobinPolicy(),
                          default_retry_policy=DowngradingConsistencyRetryPolicy(), execution_profiles={'a': ExecutionProfile()})

        # can't add after
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy())
        self.assertRaises(ValueError, cluster.add_execution_profile, 'name', ExecutionProfile())

        # session settings lock out profiles
        cluster = Cluster()
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])
        for attr, value in (('default_timeout', 1),
                            ('default_consistency_level', ConsistencyLevel.ANY),
                            ('default_serial_consistency_level', ConsistencyLevel.SERIAL),
                            ('row_factory', tuple_factory)):
            cluster._config_mode = _ConfigMode.UNCOMMITTED
            setattr(session, attr, value)
            self.assertRaises(ValueError, cluster.add_execution_profile, 'name' + attr, ExecutionProfile())

        # don't accept profile
        self.assertRaises(ValueError, session.execute_async, "query", execution_profile='some name here')

    @mock_session_pools
    def test_no_legacy_with_profile(self):
        cluster_init = Cluster(execution_profiles={'name': ExecutionProfile()})
        cluster_add = Cluster()
        cluster_add.add_execution_profile('name', ExecutionProfile())
        # for clusters with profiles added either way...
        for cluster in (cluster_init, cluster_init):
            # don't allow legacy parameters set
            for attr, value in (('default_retry_policy', RetryPolicy()),
                                ('load_balancing_policy', default_lbp_factory())):
                self.assertRaises(ValueError, setattr, cluster, attr, value)
            session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])
            for attr, value in (('default_timeout', 1),
                                ('default_consistency_level', ConsistencyLevel.ANY),
                                ('default_serial_consistency_level', ConsistencyLevel.SERIAL),
                                ('row_factory', tuple_factory)):
                self.assertRaises(ValueError, setattr, session, attr, value)

    @mock_session_pools
    def test_profile_name_value(self):

        internalized_profile = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(2)])
        cluster = Cluster(execution_profiles={'by-name': internalized_profile})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])
        self.assertEqual(cluster._config_mode, _ConfigMode.PROFILES)

        rf = session.execute_async("query", execution_profile='by-name')
        self._verify_response_future_profile(rf, internalized_profile)

        by_value = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(2)])
        rf = session.execute_async("query", execution_profile=by_value)
        self._verify_response_future_profile(rf, by_value)

    @mock_session_pools
    def test_exec_profile_clone(self):

        cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(), 'one': ExecutionProfile()})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy)])

        profile_attrs = {'request_timeout': 1,
                         'consistency_level': ConsistencyLevel.ANY,
                         'serial_consistency_level': ConsistencyLevel.SERIAL,
                         'row_factory': tuple_factory,
                         'retry_policy': RetryPolicy(),
                         'load_balancing_policy': default_lbp_factory()}
        reference_attributes = ('retry_policy', 'load_balancing_policy')

        # default and one named
        for profile in (EXEC_PROFILE_DEFAULT, 'one'):
            active = session.get_execution_profile(profile)
            clone = session.execution_profile_clone_update(profile)
            self.assertIsNot(clone, active)

            all_updated = session.execution_profile_clone_update(clone, **profile_attrs)
            self.assertIsNot(all_updated, clone)
            for attr, value in profile_attrs.items():
                self.assertEqual(getattr(clone, attr), getattr(active, attr))
                if attr in reference_attributes:
                    self.assertIs(getattr(clone, attr), getattr(active, attr))
                self.assertNotEqual(getattr(all_updated, attr), getattr(active, attr))

        # cannot clone nonexistent profile
        self.assertRaises(ValueError, session.execution_profile_clone_update, 'DOES NOT EXIST', **profile_attrs)

    def test_no_profiles_same_name(self):
        # can override default in init
        cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(), 'one': ExecutionProfile()})

        # cannot update default
        self.assertRaises(ValueError, cluster.add_execution_profile, EXEC_PROFILE_DEFAULT, ExecutionProfile())

        # cannot update named init
        self.assertRaises(ValueError, cluster.add_execution_profile, 'one', ExecutionProfile())

        # can add new name
        cluster.add_execution_profile('two', ExecutionProfile())

        # cannot add a profile added dynamically
        self.assertRaises(ValueError, cluster.add_execution_profile, 'two', ExecutionProfile())

    def test_warning_on_no_lbp_with_contact_points_legacy_mode(self):
        """
        Test that users are warned when they instantiate a Cluster object in
        legacy mode with contact points but no load-balancing policy.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result logs

        @test_category configuration
        """
        self._check_warning_on_no_lbp_with_contact_points(
            cluster_kwargs={'contact_points': ['127.0.0.1']}
        )

    def test_warning_on_no_lbp_with_contact_points_profile_mode(self):
        """
        Test that users are warned when they instantiate a Cluster object in
        execution profile mode with contact points but no load-balancing
        policy.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result logs

        @test_category configuration
        """
        self._check_warning_on_no_lbp_with_contact_points(cluster_kwargs={
            'contact_points': ['127.0.0.1'],
            'execution_profiles': {EXEC_PROFILE_DEFAULT: ExecutionProfile()}
        })

    @mock_session_pools
    def _check_warning_on_no_lbp_with_contact_points(self, cluster_kwargs):
        with patch('cassandra.cluster.log') as patched_logger:
            Cluster(**cluster_kwargs)
        patched_logger.warning.assert_called_once()
        warning_message = patched_logger.warning.call_args[0][0]
        self.assertIn('please specify a load-balancing policy', warning_message)
        self.assertIn("contact_points = ['127.0.0.1']", warning_message)

    def test_no_warning_on_contact_points_with_lbp_legacy_mode(self):
        """
        Test that users aren't warned when they instantiate a Cluster object
        with contact points and a load-balancing policy in legacy mode.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result no logs

        @test_category configuration
        """
        self._check_no_warning_on_contact_points_with_lbp({
            'contact_points': ['127.0.0.1'],
            'load_balancing_policy': object()
        })

    def test_no_warning_on_contact_points_with_lbp_profiles_mode(self):
        """
        Test that users aren't warned when they instantiate a Cluster object
        with contact points and a load-balancing policy in execution profile
        mode.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result no logs

        @test_category configuration
        """
        ep_with_lbp = ExecutionProfile(load_balancing_policy=object())
        self._check_no_warning_on_contact_points_with_lbp(cluster_kwargs={
            'contact_points': ['127.0.0.1'],
            'execution_profiles': {
                EXEC_PROFILE_DEFAULT: ep_with_lbp
            }
        })

    @mock_session_pools
    def _check_no_warning_on_contact_points_with_lbp(self, cluster_kwargs):
        """
        Test that users aren't warned when they instantiate a Cluster object
        with contact points and a load-balancing policy.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result no logs

        @test_category configuration
        """
        with patch('cassandra.cluster.log') as patched_logger:
            Cluster(**cluster_kwargs)
        patched_logger.warning.assert_not_called()

    @mock_session_pools
    def test_warning_adding_no_lbp_ep_to_cluster_with_contact_points(self):
        ep_with_lbp = ExecutionProfile(load_balancing_policy=object())
        cluster = Cluster(
            contact_points=['127.0.0.1'],
            execution_profiles={EXEC_PROFILE_DEFAULT: ep_with_lbp})
        with patch('cassandra.cluster.log') as patched_logger:
            cluster.add_execution_profile(
                name='no_lbp',
                profile=ExecutionProfile()
            )

        patched_logger.warning.assert_called_once()
        warning_message = patched_logger.warning.call_args[0][0]
        self.assertIn('no_lbp', warning_message)
        self.assertIn('trying to add', warning_message)
        self.assertIn('please specify a load-balancing policy', warning_message)

    @mock_session_pools
    def test_no_warning_adding_lbp_ep_to_cluster_with_contact_points(self):
        ep_with_lbp = ExecutionProfile(load_balancing_policy=object())
        cluster = Cluster(
            contact_points=['127.0.0.1'],
            execution_profiles={EXEC_PROFILE_DEFAULT: ep_with_lbp})
        with patch('cassandra.cluster.log') as patched_logger:
            cluster.add_execution_profile(
                name='with_lbp',
                profile=ExecutionProfile(load_balancing_policy=Mock(name='lbp'))
            )

        patched_logger.warning.assert_not_called()
