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

from collections import Counter
import datetime
import json
import logging
import multiprocessing
import random
import platform
import socket
import ssl
import sys
from threading import Event, Thread
import time

from cassandra.policies import HostDistance
from cassandra.util import ms_timestamp_from_datetime
from cassandra.datastax.insights.registry import insights_registry
from cassandra.datastax.insights.serializers  import initialize_registry

log = logging.getLogger(__name__)


class MonitorReporter(Thread):

    def __init__(self, interval_sec, session):
        """
        takes an int indicating interval between requests, a function returning
        the connection to be used, and the timeout per request
        """
        # Thread is an old-style class so we can't super()
        Thread.__init__(self, name='monitor_reporter')

        initialize_registry(insights_registry)

        self._interval, self._session = interval_sec, session

        self._shutdown_event = Event()
        self.daemon = True
        self.start()

    def run(self):
        self._send_via_rpc(self._get_startup_data())

        # introduce some jitter -- send up to 1/10 of _interval early
        self._shutdown_event.wait(self._interval * random.uniform(.9, 1))

        while not self._shutdown_event.is_set():
            start_time = time.time()

            self._send_via_rpc(self._get_status_data())

            elapsed = time.time() - start_time
            self._shutdown_event.wait(max(self._interval - elapsed, 0.01))

    # TODO: redundant with ConnectionHeartbeat.ShutdownException
    class ShutDownException(Exception):
        pass

    def _send_via_rpc(self, data):
        try:
            self._session.execute(
                "CALL InsightsRpc.reportInsight(%s)", (json.dumps(data),)
            )
            log.debug('Insights RPC data: {}'.format(data))
        except Exception as e:
            log.debug('Insights RPC send failed with {}'.format(e))
            log.debug('Insights RPC data: {}'.format(data))

    def _get_status_data(self):
        cc = self._session.cluster.control_connection

        connected_nodes = {
            host.address: {
                'connections': state['open_count'],
                'inFlightQueries': state['in_flights']
            }
            for (host, state) in self._session.get_pool_state().items()
        }

        return {
            'metadata': {
                # shared across drivers; never change
                'name': 'driver.status',
                # format version
                'insightMappingId': 'v1',
                'insightType': 'EVENT',
                # since epoch
                'timestamp': ms_timestamp_from_datetime(datetime.datetime.utcnow()),
                'tags': {
                    'language': 'python'
                }
            },
            # // 'clientId', 'sessionId' and 'controlConnection' are mandatory
            # // the rest of the properties are optional
            'data': {
                # // 'clientId' must be the same as the one provided in the startup message
                'clientId': str(self._session.cluster.client_id),
                # // 'sessionId' must be the same as the one provided in the startup message
                'sessionId': str(self._session.session_id),
                'controlConnection': cc._connection.host if cc._connection else None,
                'connectedNodes': connected_nodes
            }
        }

    def _get_startup_data(self):
        cc = self._session.cluster.control_connection
        try:
            local_ipaddr = cc._connection._socket.getsockname()[0]
        except Exception as e:
            local_ipaddr = None
            log.debug('Unable to get local socket addr from {}: {}'.format(cc._connection, e))
        hostname = socket.getfqdn()

        host_distances_counter = Counter(
            self._session.cluster.profile_manager.distance(host)
            for host in self._session.hosts
        )
        host_distances_dict = {
            'local': host_distances_counter[HostDistance.LOCAL],
            'remote': host_distances_counter[HostDistance.REMOTE],
            'ignored': host_distances_counter[HostDistance.IGNORED]
        }

        try:
            compression_type = cc._connection._compression_type
        except AttributeError:
            compression_type = 'NONE'

        cert_validation = None
        try:
            if self._session.cluster.ssl_context:
                if isinstance(self._session.cluster.ssl_context, ssl.SSLContext):
                    cert_validation = self._session.cluster.ssl_context.verify_mode == ssl.CERT_REQUIRED
                else:  # pyopenssl
                    from OpenSSL import SSL
                    cert_validation = self._session.cluster.ssl_context.get_verify_mode() != SSL.VERIFY_NONE
            elif self._session.cluster.ssl_options:
                cert_validation = self._session.cluster.ssl_options.get('cert_reqs') == ssl.CERT_REQUIRED
        except Exception as e:
            log.debug('Unable to get the cert validation: {}'.format(e))

        uname_info = platform.uname()

        return {
            'metadata': {
                'name': 'driver.startup',
                'insightMappingId': 'v1',
                'insightType': 'EVENT',
                'timestamp': ms_timestamp_from_datetime(datetime.datetime.utcnow()),
                'tags': {
                    'language': 'python'
                },
            },
            'data': {
                'driverName': 'DataStax Python Driver',
                'driverVersion': sys.modules['cassandra'].__version__,
                'clientId': str(self._session.cluster.client_id),
                'sessionId': str(self._session.session_id),
                'applicationName': self._session.cluster.application_name or 'python',
                'applicationNameWasGenerated': not self._session.cluster.application_name,
                'applicationVersion': self._session.cluster.application_version,
                'contactPoints': self._session.cluster._endpoint_map_for_insights,
                'dataCenters': list(set(h.datacenter for h in self._session.cluster.metadata.all_hosts()
                                        if (h.datacenter and
                                            self._session.cluster.profile_manager.distance(h) == HostDistance.LOCAL))),
                'initialControlConnection': cc._connection.host if cc._connection else None,
                'protocolVersion': self._session.cluster.protocol_version,
                'localAddress': local_ipaddr,
                'hostName': hostname,
                'executionProfiles': insights_registry.serialize(self._session.cluster.profile_manager),
                'configuredConnectionLength': host_distances_dict,
                'heartbeatInterval': self._session.cluster.idle_heartbeat_interval,
                'compression': compression_type.upper() if compression_type else 'NONE',
                'reconnectionPolicy': insights_registry.serialize(self._session.cluster.reconnection_policy),
                'sslConfigured': {
                    'enabled': bool(self._session.cluster.ssl_options or self._session.cluster.ssl_context),
                    'certValidation': cert_validation
                },
                'authProvider': {
                    'type': (self._session.cluster.auth_provider.__class__.__name__
                             if self._session.cluster.auth_provider else
                             None)
                },
                'otherOptions': {
                },
                'platformInfo': {
                    'os': {
                        'name': uname_info.system,
                        'version': uname_info.release,
                        'arch': uname_info.machine
                    },
                    'cpus': {
                        'length': multiprocessing.cpu_count(),
                        'model': platform.processor()
                    },
                    'runtime': {
                        'python': sys.version,
                        'event_loop': self._session.cluster.connection_class.__name__
                    }
                },
                'periodicStatusInterval': self._interval
            }
        }

    def stop(self):
        log.debug("Shutting down Monitor Reporter")
        self._shutdown_event.set()
        self.join()
