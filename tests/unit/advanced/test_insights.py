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
import sys
from unittest.mock import sentinel

from cassandra import ConsistencyLevel
from cassandra.cluster import (
    ExecutionProfile, GraphExecutionProfile, ProfileManager,
    GraphAnalyticsExecutionProfile,
    EXEC_PROFILE_DEFAULT, EXEC_PROFILE_GRAPH_DEFAULT,
    EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT,
    EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT
)
from cassandra.datastax.graph.query import GraphOptions
from cassandra.datastax.insights.registry import insights_registry
from cassandra.datastax.insights.serializers import initialize_registry
from cassandra.datastax.insights.util import namespace
from cassandra.policies import (
    RoundRobinPolicy,
    LoadBalancingPolicy,
    DCAwareRoundRobinPolicy,
    TokenAwarePolicy,
    WhiteListRoundRobinPolicy,
    HostFilterPolicy,
    ConstantReconnectionPolicy,
    ExponentialReconnectionPolicy,
    RetryPolicy,
    SpeculativeExecutionPolicy,
    ConstantSpeculativeExecutionPolicy,
    WrapperPolicy
)


log = logging.getLogger(__name__)

initialize_registry(insights_registry)


class TestGetConfig(unittest.TestCase):

    def test_invalid_object(self):
        class NoConfAsDict(object):
            pass

        obj = NoConfAsDict()

        ns = 'tests.unit.advanced.test_insights'
        if sys.version_info > (3,):
            ns += '.TestGetConfig.test_invalid_object.<locals>'

        # no default
        # ... as a policy
        self.assertEqual(insights_registry.serialize(obj, policy=True),
                         {'type': 'NoConfAsDict',
                          'namespace': ns,
                          'options': {}})
        # ... not as a policy (default)
        self.assertEqual(insights_registry.serialize(obj),
                         {'type': 'NoConfAsDict',
                          'namespace': ns,
                          })
        # with default
        self.assertIs(insights_registry.serialize(obj, default=sentinel.attr_err_default),
                      sentinel.attr_err_default)

    def test_successful_return(self):

        class SuperclassSentinel(object):
            pass

        class SubclassSentinel(SuperclassSentinel):
            pass

        @insights_registry.register_serializer_for(SuperclassSentinel)
        def superclass_sentinel_serializer(obj):
            return sentinel.serialized_superclass

        self.assertIs(insights_registry.serialize(SuperclassSentinel()),
                      sentinel.serialized_superclass)
        self.assertIs(insights_registry.serialize(SubclassSentinel()),
                      sentinel.serialized_superclass)

        # with default -- same behavior
        self.assertIs(insights_registry.serialize(SubclassSentinel(), default=object()),
                      sentinel.serialized_superclass)

class TestConfigAsDict(unittest.TestCase):

    #  graph/query.py
    def test_graph_options(self):
        self.maxDiff = None

        go = GraphOptions(graph_name='name_for_test',
                          graph_source='source_for_test',
                          graph_language='lang_for_test',
                          graph_protocol='protocol_for_test',
                          graph_read_consistency_level=ConsistencyLevel.ANY,
                          graph_write_consistency_level=ConsistencyLevel.ONE,
                          graph_invalid_option='invalid')

        log.debug(go._graph_options)

        self.assertEqual(
            insights_registry.serialize(go),
            {'source': 'source_for_test',
             'language': 'lang_for_test',
             'graphProtocol': 'protocol_for_test',
             # no graph_invalid_option
             }
        )

    # cluster.py
    def test_execution_profile(self):
        self.maxDiff = None
        self.assertEqual(
            insights_registry.serialize(ExecutionProfile()),
            {'consistency': 'LOCAL_ONE',
             'continuousPagingOptions': None,
             'loadBalancing': {'namespace': 'cassandra.policies',
                               'options': {'child_policy': {'namespace': 'cassandra.policies',
                                                            'options': {'local_dc': '',
                                                                        'used_hosts_per_remote_dc': 0},
                                                            'type': 'DCAwareRoundRobinPolicy'},
                                           'shuffle_replicas': False},
                               'type': 'TokenAwarePolicy'},
             'readTimeout': 10.0,
             'retry': {'namespace': 'cassandra.policies', 'options': {}, 'type': 'RetryPolicy'},
             'serialConsistency': None,
             'speculativeExecution': {'namespace': 'cassandra.policies',
                                      'options': {}, 'type': 'NoSpeculativeExecutionPolicy'},
             'graphOptions': None
             }
        )

    def test_graph_execution_profile(self):
        self.maxDiff = None
        self.assertEqual(
            insights_registry.serialize(GraphExecutionProfile()),
            {'consistency': 'LOCAL_ONE',
             'continuousPagingOptions': None,
             'loadBalancing': {'namespace': 'cassandra.policies',
                               'options': {'child_policy': {'namespace': 'cassandra.policies',
                                                            'options': {'local_dc': '',
                                                                        'used_hosts_per_remote_dc': 0},
                                                            'type': 'DCAwareRoundRobinPolicy'},
                                           'shuffle_replicas': False},
                               'type': 'TokenAwarePolicy'},
             'readTimeout': 30.0,
             'retry': {'namespace': 'cassandra.policies', 'options': {}, 'type': 'NeverRetryPolicy'},
             'serialConsistency': None,
             'speculativeExecution': {'namespace': 'cassandra.policies',
                                      'options': {}, 'type': 'NoSpeculativeExecutionPolicy'},
             'graphOptions': {'graphProtocol': None,
                              'language': 'gremlin-groovy',
                              'source': 'g'},
             }
        )

    def test_graph_analytics_execution_profile(self):
        self.maxDiff = None
        self.assertEqual(
            insights_registry.serialize(GraphAnalyticsExecutionProfile()),
            {'consistency': 'LOCAL_ONE',
             'continuousPagingOptions': None,
             'loadBalancing': {'namespace': 'cassandra.policies',
                               'options': {'child_policy': {'namespace': 'cassandra.policies',
                                                            'options': {'child_policy': {'namespace': 'cassandra.policies',
                                                                                         'options': {'local_dc': '',
                                                                                                     'used_hosts_per_remote_dc': 0},
                                                                                         'type': 'DCAwareRoundRobinPolicy'},
                                                                        'shuffle_replicas': False},
                                                            'type': 'TokenAwarePolicy'}},
                               'type': 'DefaultLoadBalancingPolicy'},
             'readTimeout': 604800.0,
             'retry': {'namespace': 'cassandra.policies', 'options': {}, 'type': 'NeverRetryPolicy'},
             'serialConsistency': None,
             'speculativeExecution': {'namespace': 'cassandra.policies',
                                      'options': {}, 'type': 'NoSpeculativeExecutionPolicy'},
             'graphOptions': {'graphProtocol': None,
                              'language': 'gremlin-groovy',
                              'source': 'a'},
             }
        )

    # policies.py
    def test_DC_aware_round_robin_policy(self):
        self.assertEqual(
            insights_registry.serialize(DCAwareRoundRobinPolicy()),
            {'namespace': 'cassandra.policies',
             'options': {'local_dc': '', 'used_hosts_per_remote_dc': 0},
             'type': 'DCAwareRoundRobinPolicy'}
        )
        self.assertEqual(
            insights_registry.serialize(DCAwareRoundRobinPolicy(local_dc='fake_local_dc',
                                                                used_hosts_per_remote_dc=15)),
            {'namespace': 'cassandra.policies',
             'options': {'local_dc': 'fake_local_dc', 'used_hosts_per_remote_dc': 15},
             'type': 'DCAwareRoundRobinPolicy'}
        )

    def test_token_aware_policy(self):
        self.assertEqual(
            insights_registry.serialize(TokenAwarePolicy(child_policy=LoadBalancingPolicy())),
            {'namespace': 'cassandra.policies',
             'options': {'child_policy': {'namespace': 'cassandra.policies',
                                          'options': {},
                                          'type': 'LoadBalancingPolicy'},
                         'shuffle_replicas': False},
             'type': 'TokenAwarePolicy'}
        )

    def test_whitelist_round_robin_policy(self):
        self.assertEqual(
            insights_registry.serialize(WhiteListRoundRobinPolicy(['127.0.0.3'])),
            {'namespace': 'cassandra.policies',
             'options': {'allowed_hosts': ('127.0.0.3',)},
             'type': 'WhiteListRoundRobinPolicy'}
        )

    def test_host_filter_policy(self):
        def my_predicate(s):
            return False

        self.assertEqual(
            insights_registry.serialize(HostFilterPolicy(LoadBalancingPolicy(), my_predicate)),
            {'namespace': 'cassandra.policies',
             'options': {'child_policy': {'namespace': 'cassandra.policies',
                                          'options': {},
                                          'type': 'LoadBalancingPolicy'},
                         'predicate': 'my_predicate'},
             'type': 'HostFilterPolicy'}
        )

    def test_constant_reconnection_policy(self):
        self.assertEqual(
            insights_registry.serialize(ConstantReconnectionPolicy(3, 200)),
            {'type': 'ConstantReconnectionPolicy',
            'namespace': 'cassandra.policies',
            'options': {'delay': 3, 'max_attempts': 200}
             }
        )

    def test_exponential_reconnection_policy(self):
        self.assertEqual(
            insights_registry.serialize(ExponentialReconnectionPolicy(4, 100, 10)),
            {'type': 'ExponentialReconnectionPolicy',
            'namespace': 'cassandra.policies',
            'options': {'base_delay': 4, 'max_delay': 100, 'max_attempts': 10}
             }
        )

    def test_retry_policy(self):
        self.assertEqual(
            insights_registry.serialize(RetryPolicy()),
            {'type': 'RetryPolicy',
            'namespace': 'cassandra.policies',
            'options': {}
             }
        )

    def test_spec_exec_policy(self):
        self.assertEqual(
            insights_registry.serialize(SpeculativeExecutionPolicy()),
            {'type': 'SpeculativeExecutionPolicy',
            'namespace': 'cassandra.policies',
            'options': {}
             }
        )

    def test_constant_spec_exec_policy(self):
        self.assertEqual(
            insights_registry.serialize(ConstantSpeculativeExecutionPolicy(100, 101)),
            {'type': 'ConstantSpeculativeExecutionPolicy',
             'namespace': 'cassandra.policies',
             'options': {'delay': 100,
                         'max_attempts': 101}
             }
        )

    def test_wrapper_policy(self):
        self.assertEqual(
            insights_registry.serialize(WrapperPolicy(LoadBalancingPolicy())),
            {'namespace': 'cassandra.policies',
             'options': {'child_policy': {'namespace': 'cassandra.policies',
                                          'options': {},
                                          'type': 'LoadBalancingPolicy'}
                         },
             'type': 'WrapperPolicy'}
        )
