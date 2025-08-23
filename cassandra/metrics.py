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

from itertools import chain
import logging

try:
    from greplin import scales
except ImportError:
    raise ImportError(
        "The scales library is required for metrics support: "
        "https://pypi.org/project/scales/")

log = logging.getLogger(__name__)


class Metrics(object):
    """
    A collection of timers and counters for various performance metrics.

    Timer metrics are represented as floating point seconds.
    """

    request_timer = None
    """
    A :class:`greplin.scales.PmfStat` timer for requests. This is a dict-like
    object with the following keys:

    * count - number of requests that have been timed
    * min - min latency
    * max - max latency
    * mean - mean latency
    * stddev - standard deviation for latencies
    * median - median latency
    * 75percentile - 75th percentile latencies
    * 95percentile - 95th percentile latencies
    * 98percentile - 98th percentile latencies
    * 99percentile - 99th percentile latencies
    * 999percentile - 99.9th percentile latencies
    """

    connection_errors = None
    """
    A :class:`greplin.scales.IntStat` count of the number of times that a
    request to a Cassandra node has failed due to a connection problem.
    """

    write_timeouts = None
    """
    A :class:`greplin.scales.IntStat` count of write requests that resulted
    in a timeout.
    """

    read_timeouts = None
    """
    A :class:`greplin.scales.IntStat` count of read requests that resulted
    in a timeout.
    """

    unavailables = None
    """
    A :class:`greplin.scales.IntStat` count of write or read requests that
    failed due to an insufficient number of replicas being alive to meet
    the requested :class:`.ConsistencyLevel`.
    """

    other_errors = None
    """
    A :class:`greplin.scales.IntStat` count of all other request failures,
    including failures caused by invalid requests, bootstrapping nodes,
    overloaded nodes, etc.
    """

    retries = None
    """
    A :class:`greplin.scales.IntStat` count of the number of times a
    request was retried based on the :class:`.RetryPolicy` decision.
    """

    ignores = None
    """
    A :class:`greplin.scales.IntStat` count of the number of times a
    failed request was ignored based on the :class:`.RetryPolicy` decision.
    """

    known_hosts = None
    """
    A :class:`greplin.scales.IntStat` count of the number of nodes in
    the cluster that the driver is aware of, regardless of whether any
    connections are opened to those nodes.
    """

    connected_to = None
    """
    A :class:`greplin.scales.IntStat` count of the number of nodes that
    the driver currently has at least one connection open to.
    """

    open_connections = None
    """
    A :class:`greplin.scales.IntStat` count of the number connections
    the driver currently has open.
    """

    _stats_counter = 0

    def __init__(self, cluster_proxy):
        log.debug("Starting metric capture")

        self.stats_name = 'cassandra-{0}'.format(str(self._stats_counter))
        Metrics._stats_counter += 1
        self.stats = scales.collection(self.stats_name,
            scales.PmfStat('request_timer'),
            scales.IntStat('connection_errors'),
            scales.IntStat('write_timeouts'),
            scales.IntStat('read_timeouts'),
            scales.IntStat('unavailables'),
            scales.IntStat('other_errors'),
            scales.IntStat('retries'),
            scales.IntStat('ignores'),

            # gauges
            scales.Stat('known_hosts',
                lambda: len(cluster_proxy.metadata.all_hosts())),
            scales.Stat('connected_to',
                lambda: len(set(chain.from_iterable(s._pools.keys() for s in cluster_proxy.sessions)))),
            scales.Stat('open_connections',
                lambda: sum(sum(p.open_count for p in s._pools.values()) for s in cluster_proxy.sessions)))

        # TODO, to be removed in 4.0
        # /cassandra contains the metrics of the first cluster registered
        if 'cassandra' not in scales._Stats.stats:
            scales._Stats.stats['cassandra'] = scales._Stats.stats[self.stats_name]

        self.request_timer = self.stats.request_timer
        self.connection_errors = self.stats.connection_errors
        self.write_timeouts = self.stats.write_timeouts
        self.read_timeouts = self.stats.read_timeouts
        self.unavailables = self.stats.unavailables
        self.other_errors = self.stats.other_errors
        self.retries = self.stats.retries
        self.ignores = self.stats.ignores
        self.known_hosts = self.stats.known_hosts
        self.connected_to = self.stats.connected_to
        self.open_connections = self.stats.open_connections

    def on_connection_error(self):
        self.stats.connection_errors += 1

    def on_write_timeout(self):
        self.stats.write_timeouts += 1

    def on_read_timeout(self):
        self.stats.read_timeouts += 1

    def on_unavailable(self):
        self.stats.unavailables += 1

    def on_other_error(self):
        self.stats.other_errors += 1

    def on_ignore(self):
        self.stats.ignores += 1

    def on_retry(self):
        self.stats.retries += 1

    def get_stats(self):
        """
        Returns the metrics for the registered cluster instance.
        """
        return scales.getStats()[self.stats_name]

    def set_stats_name(self, stats_name):
        """
        Set the metrics stats name.
        The stats_name is a string used to access the metrics through scales: scales.getStats()[<stats_name>]
        Default is 'cassandra-<num>'.
        """

        if self.stats_name == stats_name:
            return

        if stats_name in scales._Stats.stats:
            raise ValueError('"{0}" already exists in stats.'.format(stats_name))

        stats = scales._Stats.stats[self.stats_name]
        del scales._Stats.stats[self.stats_name]
        self.stats_name = stats_name
        scales._Stats.stats[self.stats_name] = stats
