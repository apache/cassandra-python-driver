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


def initialize_registry(insights_registry):
    # This will be called from the cluster module, so we put all this behavior
    # in a function to avoid circular imports

    if insights_registry.initialized:
        return False

    from cassandra import ConsistencyLevel
    from cassandra.cluster import (
        ExecutionProfile, GraphExecutionProfile,
        ProfileManager, ContinuousPagingOptions,
        EXEC_PROFILE_DEFAULT, EXEC_PROFILE_GRAPH_DEFAULT,
        EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT,
        EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT,
        _NOT_SET
    )
    from cassandra.datastax.graph import GraphOptions
    from cassandra.datastax.insights.registry import insights_registry
    from cassandra.datastax.insights.util import namespace
    from cassandra.policies import (
        RoundRobinPolicy,
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

    import logging

    log = logging.getLogger(__name__)

    @insights_registry.register_serializer_for(RoundRobinPolicy)
    def round_robin_policy_insights_serializer(policy):
        return {'type': policy.__class__.__name__,
                'namespace': namespace(policy.__class__),
                'options': {}}

    @insights_registry.register_serializer_for(DCAwareRoundRobinPolicy)
    def dc_aware_round_robin_policy_insights_serializer(policy):
        return {'type': policy.__class__.__name__,
                'namespace': namespace(policy.__class__),
                'options': {'local_dc': policy.local_dc,
                            'used_hosts_per_remote_dc': policy.used_hosts_per_remote_dc}
                }

    @insights_registry.register_serializer_for(TokenAwarePolicy)
    def token_aware_policy_insights_serializer(policy):
        return {'type': policy.__class__.__name__,
                'namespace': namespace(policy.__class__),
                'options': {'child_policy': insights_registry.serialize(policy._child_policy,
                                                                        policy=True),
                            'shuffle_replicas': policy.shuffle_replicas}
                }

    @insights_registry.register_serializer_for(WhiteListRoundRobinPolicy)
    def whitelist_round_robin_policy_insights_serializer(policy):
        return {'type': policy.__class__.__name__,
                'namespace': namespace(policy.__class__),
                'options': {'allowed_hosts': policy._allowed_hosts}
                }

    @insights_registry.register_serializer_for(HostFilterPolicy)
    def host_filter_policy_insights_serializer(policy):
        return {
            'type': policy.__class__.__name__,
            'namespace': namespace(policy.__class__),
            'options': {'child_policy': insights_registry.serialize(policy._child_policy,
                                                                    policy=True),
                        'predicate': policy.predicate.__name__}
        }

    @insights_registry.register_serializer_for(ConstantReconnectionPolicy)
    def constant_reconnection_policy_insights_serializer(policy):
        return {'type': policy.__class__.__name__,
                'namespace': namespace(policy.__class__),
                'options': {'delay': policy.delay,
                            'max_attempts': policy.max_attempts}
                }

    @insights_registry.register_serializer_for(ExponentialReconnectionPolicy)
    def exponential_reconnection_policy_insights_serializer(policy):
        return {'type': policy.__class__.__name__,
                'namespace': namespace(policy.__class__),
                'options': {'base_delay': policy.base_delay,
                            'max_delay': policy.max_delay,
                            'max_attempts': policy.max_attempts}
                }

    @insights_registry.register_serializer_for(RetryPolicy)
    def retry_policy_insights_serializer(policy):
        return {'type': policy.__class__.__name__,
                'namespace': namespace(policy.__class__),
                'options': {}}

    @insights_registry.register_serializer_for(SpeculativeExecutionPolicy)
    def speculative_execution_policy_insights_serializer(policy):
        return {'type': policy.__class__.__name__,
                'namespace': namespace(policy.__class__),
                'options': {}}

    @insights_registry.register_serializer_for(ConstantSpeculativeExecutionPolicy)
    def constant_speculative_execution_policy_insights_serializer(policy):
        return {'type': policy.__class__.__name__,
                'namespace': namespace(policy.__class__),
                    'options': {'delay': policy.delay,
                                'max_attempts': policy.max_attempts}
                }

    @insights_registry.register_serializer_for(WrapperPolicy)
    def wrapper_policy_insights_serializer(policy):
        return {'type': policy.__class__.__name__,
                'namespace': namespace(policy.__class__),
                'options': {
                    'child_policy': insights_registry.serialize(policy._child_policy,
                                                                policy=True)
                }}

    @insights_registry.register_serializer_for(ExecutionProfile)
    def execution_profile_insights_serializer(profile):
        return {
            'loadBalancing': insights_registry.serialize(profile.load_balancing_policy,
                                                         policy=True),
            'retry': insights_registry.serialize(profile.retry_policy,
                                                 policy=True),
            'readTimeout': profile.request_timeout,
            'consistency': ConsistencyLevel.value_to_name.get(profile.consistency_level, None),
            'serialConsistency': ConsistencyLevel.value_to_name.get(profile.serial_consistency_level, None),
            'continuousPagingOptions': (insights_registry.serialize(profile.continuous_paging_options)
                                        if (profile.continuous_paging_options is not None and
                                           profile.continuous_paging_options is not _NOT_SET) else
                                        None),
            'speculativeExecution': insights_registry.serialize(profile.speculative_execution_policy),
            'graphOptions': None
        }

    @insights_registry.register_serializer_for(GraphExecutionProfile)
    def graph_execution_profile_insights_serializer(profile):
        rv = insights_registry.serialize(profile, cls=ExecutionProfile)
        rv['graphOptions'] = insights_registry.serialize(profile.graph_options)
        return rv

    _EXEC_PROFILE_DEFAULT_KEYS = (EXEC_PROFILE_DEFAULT,
                                  EXEC_PROFILE_GRAPH_DEFAULT,
                                  EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT,
                                  EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT)

    @insights_registry.register_serializer_for(ProfileManager)
    def profile_manager_insights_serializer(manager):
        defaults = {
            # Insights's expected default
            'default': insights_registry.serialize(manager.profiles[EXEC_PROFILE_DEFAULT]),
            # remaining named defaults for driver's defaults, including duplicated default
            'EXEC_PROFILE_DEFAULT': insights_registry.serialize(manager.profiles[EXEC_PROFILE_DEFAULT]),
            'EXEC_PROFILE_GRAPH_DEFAULT': insights_registry.serialize(manager.profiles[EXEC_PROFILE_GRAPH_DEFAULT]),
            'EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT': insights_registry.serialize(
                manager.profiles[EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT]
            ),
            'EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT': insights_registry.serialize(
                manager.profiles[EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT]
            )
        }
        other = {
            key: insights_registry.serialize(value)
            for key, value in manager.profiles.items()
            if key not in _EXEC_PROFILE_DEFAULT_KEYS
        }
        overlapping_keys = set(defaults) & set(other)
        if overlapping_keys:
            log.debug('The following key names overlap default key sentinel keys '
                      'and these non-default EPs will not be displayed in Insights '
                      ': {}'.format(list(overlapping_keys)))

        other.update(defaults)
        return other

    @insights_registry.register_serializer_for(GraphOptions)
    def graph_options_insights_serializer(options):
        rv = {
            'source': options.graph_source,
            'language': options.graph_language,
            'graphProtocol': options.graph_protocol
        }
        updates = {k: v.decode('utf-8') for k, v in rv.items()
                   if isinstance(v, bytes)}
        rv.update(updates)
        return rv

    @insights_registry.register_serializer_for(ContinuousPagingOptions)
    def continuous_paging_options_insights_serializer(paging_options):
        return {
                'page_unit': paging_options.page_unit,
                'max_pages': paging_options.max_pages,
                'max_pages_per_second': paging_options.max_pages_per_second,
                'max_queue_size': paging_options.max_queue_size
        }

    insights_registry.initialized = True
    return True
