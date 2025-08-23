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

from collections import namedtuple
from functools import lru_cache
from itertools import islice, cycle, groupby, repeat
import logging
from random import randint, shuffle
from threading import Lock
import socket
import warnings

log = logging.getLogger(__name__)

from cassandra import WriteType as WT

# This is done this way because WriteType was originally
# defined here and in order not to break the API.
# It may be removed in the next major.
WriteType = WT

from cassandra import ConsistencyLevel, OperationTimedOut

class HostDistance(object):
    """
    A measure of how "distant" a node is from the client, which
    may influence how the load balancer distributes requests
    and how many connections are opened to the node.
    """

    IGNORED = -1
    """
    A node with this distance should never be queried or have
    connections opened to it.
    """

    LOCAL = 0
    """
    Nodes with ``LOCAL`` distance will be preferred for operations
    under some load balancing policies (such as :class:`.DCAwareRoundRobinPolicy`)
    and will have a greater number of connections opened against
    them by default.

    This distance is typically used for nodes within the same
    datacenter as the client.
    """

    REMOTE = 1
    """
    Nodes with ``REMOTE`` distance will be treated as a last resort
    by some load balancing policies (such as :class:`.DCAwareRoundRobinPolicy`)
    and will have a smaller number of connections opened against
    them by default.

    This distance is typically used for nodes outside of the
    datacenter that the client is running in.
    """


class HostStateListener(object):

    def on_up(self, host):
        """ Called when a node is marked up. """
        raise NotImplementedError()

    def on_down(self, host):
        """ Called when a node is marked down. """
        raise NotImplementedError()

    def on_add(self, host):
        """
        Called when a node is added to the cluster.  The newly added node
        should be considered up.
        """
        raise NotImplementedError()

    def on_remove(self, host):
        """ Called when a node is removed from the cluster. """
        raise NotImplementedError()


class LoadBalancingPolicy(HostStateListener):
    """
    Load balancing policies are used to decide how to distribute
    requests among all possible coordinator nodes in the cluster.

    In particular, they may focus on querying "near" nodes (those
    in a local datacenter) or on querying nodes who happen to
    be replicas for the requested data.

    You may also use subclasses of :class:`.LoadBalancingPolicy` for
    custom behavior.
    """

    _hosts_lock = None

    def __init__(self):
        self._hosts_lock = Lock()

    def distance(self, host):
        """
        Returns a measure of how remote a :class:`~.pool.Host` is in
        terms of the :class:`.HostDistance` enums.
        """
        raise NotImplementedError()

    def populate(self, cluster, hosts):
        """
        This method is called to initialize the load balancing
        policy with a set of :class:`.Host` instances before its
        first use.  The `cluster` parameter is an instance of
        :class:`.Cluster`.
        """
        raise NotImplementedError()

    def make_query_plan(self, working_keyspace=None, query=None):
        """
        Given a :class:`~.query.Statement` instance, return an iterable
        of :class:`.Host` instances which should be queried in that
        order.  A generator may work well for custom implementations
        of this method.

        Note that the `query` argument may be :const:`None` when preparing
        statements.

        `working_keyspace` should be the string name of the current keyspace,
        as set through :meth:`.Session.set_keyspace()` or with a ``USE``
        statement.
        """
        raise NotImplementedError()

    def check_supported(self):
        """
        This will be called after the cluster Metadata has been initialized.
        If the load balancing policy implementation cannot be supported for
        some reason (such as a missing C extension), this is the point at
        which it should raise an exception.
        """
        pass


class RoundRobinPolicy(LoadBalancingPolicy):
    """
    A subclass of :class:`.LoadBalancingPolicy` which evenly
    distributes queries across all nodes in the cluster,
    regardless of what datacenter the nodes may be in.
    """
    _live_hosts = frozenset(())
    _position = 0

    def populate(self, cluster, hosts):
        self._live_hosts = frozenset(hosts)
        if len(hosts) > 1:
            self._position = randint(0, len(hosts) - 1)

    def distance(self, host):
        return HostDistance.LOCAL

    def make_query_plan(self, working_keyspace=None, query=None):
        # not thread-safe, but we don't care much about lost increments
        # for the purposes of load balancing
        pos = self._position
        self._position += 1

        hosts = self._live_hosts
        length = len(hosts)
        if length:
            pos %= length
            return islice(cycle(hosts), pos, pos + length)
        else:
            return []

    def on_up(self, host):
        with self._hosts_lock:
            self._live_hosts = self._live_hosts.union((host, ))

    def on_down(self, host):
        with self._hosts_lock:
            self._live_hosts = self._live_hosts.difference((host, ))

    def on_add(self, host):
        with self._hosts_lock:
            self._live_hosts = self._live_hosts.union((host, ))

    def on_remove(self, host):
        with self._hosts_lock:
            self._live_hosts = self._live_hosts.difference((host, ))


class DCAwareRoundRobinPolicy(LoadBalancingPolicy):
    """
    Similar to :class:`.RoundRobinPolicy`, but prefers hosts
    in the local datacenter and only uses nodes in remote
    datacenters as a last resort.
    """

    local_dc = None
    used_hosts_per_remote_dc = 0

    def __init__(self, local_dc='', used_hosts_per_remote_dc=0):
        """
        The `local_dc` parameter should be the name of the datacenter
        (such as is reported by ``nodetool ring``) that should
        be considered local. If not specified, the driver will choose
        a local_dc based on the first host among :attr:`.Cluster.contact_points`
        having a valid DC. If relying on this mechanism, all specified
        contact points should be nodes in a single, local DC.

        `used_hosts_per_remote_dc` controls how many nodes in
        each remote datacenter will have connections opened
        against them. In other words, `used_hosts_per_remote_dc` hosts
        will be considered :attr:`~.HostDistance.REMOTE` and the
        rest will be considered :attr:`~.HostDistance.IGNORED`.
        By default, all remote hosts are ignored.
        """
        self.local_dc = local_dc
        self.used_hosts_per_remote_dc = used_hosts_per_remote_dc
        self._dc_live_hosts = {}
        self._position = 0
        self._endpoints = []
        LoadBalancingPolicy.__init__(self)

    def _dc(self, host):
        return host.datacenter or self.local_dc

    def populate(self, cluster, hosts):
        for dc, dc_hosts in groupby(hosts, lambda h: self._dc(h)):
            self._dc_live_hosts[dc] = tuple(set(dc_hosts))

        if not self.local_dc:
            self._endpoints = [
                endpoint
                for endpoint in cluster.endpoints_resolved]

        self._position = randint(0, len(hosts) - 1) if hosts else 0

    def distance(self, host):
        dc = self._dc(host)
        if dc == self.local_dc:
            return HostDistance.LOCAL

        if not self.used_hosts_per_remote_dc:
            return HostDistance.IGNORED
        else:
            dc_hosts = self._dc_live_hosts.get(dc)
            if not dc_hosts:
                return HostDistance.IGNORED

            if host in list(dc_hosts)[:self.used_hosts_per_remote_dc]:
                return HostDistance.REMOTE
            else:
                return HostDistance.IGNORED

    def make_query_plan(self, working_keyspace=None, query=None):
        # not thread-safe, but we don't care much about lost increments
        # for the purposes of load balancing
        pos = self._position
        self._position += 1

        local_live = self._dc_live_hosts.get(self.local_dc, ())
        pos = (pos % len(local_live)) if local_live else 0
        for host in islice(cycle(local_live), pos, pos + len(local_live)):
            yield host

        # the dict can change, so get candidate DCs iterating over keys of a copy
        other_dcs = [dc for dc in self._dc_live_hosts.copy().keys() if dc != self.local_dc]
        for dc in other_dcs:
            remote_live = self._dc_live_hosts.get(dc, ())
            for host in remote_live[:self.used_hosts_per_remote_dc]:
                yield host

    def on_up(self, host):
        # not worrying about threads because this will happen during
        # control connection startup/refresh
        if not self.local_dc and host.datacenter:
            if host.endpoint in self._endpoints:
                self.local_dc = host.datacenter
                log.info("Using datacenter '%s' for DCAwareRoundRobinPolicy (via host '%s'); "
                         "if incorrect, please specify a local_dc to the constructor, "
                         "or limit contact points to local cluster nodes" %
                         (self.local_dc, host.endpoint))
                del self._endpoints

        dc = self._dc(host)
        with self._hosts_lock:
            current_hosts = self._dc_live_hosts.get(dc, ())
            if host not in current_hosts:
                self._dc_live_hosts[dc] = current_hosts + (host, )

    def on_down(self, host):
        dc = self._dc(host)
        with self._hosts_lock:
            current_hosts = self._dc_live_hosts.get(dc, ())
            if host in current_hosts:
                hosts = tuple(h for h in current_hosts if h != host)
                if hosts:
                    self._dc_live_hosts[dc] = hosts
                else:
                    del self._dc_live_hosts[dc]

    def on_add(self, host):
        self.on_up(host)

    def on_remove(self, host):
        self.on_down(host)


class TokenAwarePolicy(LoadBalancingPolicy):
    """
    A :class:`.LoadBalancingPolicy` wrapper that adds token awareness to
    a child policy.

    This alters the child policy's behavior so that it first attempts to
    send queries to :attr:`~.HostDistance.LOCAL` replicas (as determined
    by the child policy) based on the :class:`.Statement`'s
    :attr:`~.Statement.routing_key`. If :attr:`.shuffle_replicas` is
    truthy, these replicas will be yielded in a random order. Once those
    hosts are exhausted, the remaining hosts in the child policy's query
    plan will be used in the order provided by the child policy.

    If no :attr:`~.Statement.routing_key` is set on the query, the child
    policy's query plan will be used as is.
    """

    _child_policy = None
    _cluster_metadata = None
    shuffle_replicas = False
    """
    Yield local replicas in a random order.
    """

    def __init__(self, child_policy, shuffle_replicas=False):
        self._child_policy = child_policy
        self.shuffle_replicas = shuffle_replicas

    def populate(self, cluster, hosts):
        self._cluster_metadata = cluster.metadata
        self._child_policy.populate(cluster, hosts)

    def check_supported(self):
        if not self._cluster_metadata.can_support_partitioner():
            raise RuntimeError(
                '%s cannot be used with the cluster partitioner (%s) because '
                'the relevant C extension for this driver was not compiled. '
                'See the installation instructions for details on building '
                'and installing the C extensions.' %
                (self.__class__.__name__, self._cluster_metadata.partitioner))

    def distance(self, *args, **kwargs):
        return self._child_policy.distance(*args, **kwargs)

    def make_query_plan(self, working_keyspace=None, query=None):
        if query and query.keyspace:
            keyspace = query.keyspace
        else:
            keyspace = working_keyspace

        child = self._child_policy
        if query is None:
            for host in child.make_query_plan(keyspace, query):
                yield host
        else:
            routing_key = query.routing_key
            if routing_key is None or keyspace is None:
                for host in child.make_query_plan(keyspace, query):
                    yield host
            else:
                replicas = self._cluster_metadata.get_replicas(keyspace, routing_key)
                if self.shuffle_replicas:
                    shuffle(replicas)
                for replica in replicas:
                    if replica.is_up and \
                            child.distance(replica) == HostDistance.LOCAL:
                        yield replica

                for host in child.make_query_plan(keyspace, query):
                    # skip if we've already listed this host
                    if host not in replicas or \
                            child.distance(host) == HostDistance.REMOTE:
                        yield host

    def on_up(self, *args, **kwargs):
        return self._child_policy.on_up(*args, **kwargs)

    def on_down(self, *args, **kwargs):
        return self._child_policy.on_down(*args, **kwargs)

    def on_add(self, *args, **kwargs):
        return self._child_policy.on_add(*args, **kwargs)

    def on_remove(self, *args, **kwargs):
        return self._child_policy.on_remove(*args, **kwargs)


class WhiteListRoundRobinPolicy(RoundRobinPolicy):
    """
    A subclass of :class:`.RoundRobinPolicy` which evenly
    distributes queries across all nodes in the cluster,
    regardless of what datacenter the nodes may be in, but
    only if that node exists in the list of allowed nodes

    This policy is addresses the issue described in
    https://datastax-oss.atlassian.net/browse/JAVA-145
    Where connection errors occur when connection
    attempts are made to private IP addresses remotely
    """

    def __init__(self, hosts):
        """
        The `hosts` parameter should be a sequence of hosts to permit
        connections to.
        """
        self._allowed_hosts = tuple(hosts)
        self._allowed_hosts_resolved = [endpoint[4][0] for a in self._allowed_hosts
                                        for endpoint in socket.getaddrinfo(a, None, socket.AF_UNSPEC, socket.SOCK_STREAM)]

        RoundRobinPolicy.__init__(self)

    def populate(self, cluster, hosts):
        self._live_hosts = frozenset(h for h in hosts if h.address in self._allowed_hosts_resolved)

        if len(hosts) <= 1:
            self._position = 0
        else:
            self._position = randint(0, len(hosts) - 1)

    def distance(self, host):
        if host.address in self._allowed_hosts_resolved:
            return HostDistance.LOCAL
        else:
            return HostDistance.IGNORED

    def on_up(self, host):
        if host.address in self._allowed_hosts_resolved:
            RoundRobinPolicy.on_up(self, host)

    def on_add(self, host):
        if host.address in self._allowed_hosts_resolved:
            RoundRobinPolicy.on_add(self, host)


class HostFilterPolicy(LoadBalancingPolicy):
    """
    A :class:`.LoadBalancingPolicy` subclass configured with a child policy,
    and a single-argument predicate. This policy defers to the child policy for
    hosts where ``predicate(host)`` is truthy. Hosts for which
    ``predicate(host)`` is falsy will be considered :attr:`.IGNORED`, and will
    not be used in a query plan.

    This can be used in the cases where you need a whitelist or blacklist
    policy, e.g. to prepare for decommissioning nodes or for testing:

    .. code-block:: python

        def address_is_ignored(host):
            return host.address in [ignored_address0, ignored_address1]

        blacklist_filter_policy = HostFilterPolicy(
            child_policy=RoundRobinPolicy(),
            predicate=address_is_ignored
        )

        cluster = Cluster(
            primary_host,
            load_balancing_policy=blacklist_filter_policy,
        )

    See the note in the :meth:`.make_query_plan` documentation for a caveat on
    how wrapping ordering polices (e.g. :class:`.RoundRobinPolicy`) may break
    desirable properties of the wrapped policy.

    Please note that whitelist and blacklist policies are not recommended for
    general, day-to-day use. You probably want something like
    :class:`.DCAwareRoundRobinPolicy`, which prefers a local DC but has
    fallbacks, over a brute-force method like whitelisting or blacklisting.
    """

    def __init__(self, child_policy, predicate):
        """
        :param child_policy: an instantiated :class:`.LoadBalancingPolicy`
                             that this one will defer to.
        :param predicate: a one-parameter function that takes a :class:`.Host`.
                          If it returns a falsy value, the :class:`.Host` will
                          be :attr:`.IGNORED` and not returned in query plans.
        """
        super(HostFilterPolicy, self).__init__()
        self._child_policy = child_policy
        self._predicate = predicate

    def on_up(self, host, *args, **kwargs):
        return self._child_policy.on_up(host, *args, **kwargs)

    def on_down(self, host, *args, **kwargs):
        return self._child_policy.on_down(host, *args, **kwargs)

    def on_add(self, host, *args, **kwargs):
        return self._child_policy.on_add(host, *args, **kwargs)

    def on_remove(self, host, *args, **kwargs):
        return self._child_policy.on_remove(host, *args, **kwargs)

    @property
    def predicate(self):
        """
        A predicate, set on object initialization, that takes a :class:`.Host`
        and returns a value. If the value is falsy, the :class:`.Host` is
        :class:`~HostDistance.IGNORED`. If the value is truthy,
        :class:`.HostFilterPolicy` defers to the child policy to determine the
        host's distance.

        This is a read-only value set in ``__init__``, implemented as a
        ``property``.
        """
        return self._predicate

    def distance(self, host):
        """
        Checks if ``predicate(host)``, then returns
        :attr:`~HostDistance.IGNORED` if falsy, and defers to the child policy
        otherwise.
        """
        if self.predicate(host):
            return self._child_policy.distance(host)
        else:
            return HostDistance.IGNORED

    def populate(self, cluster, hosts):
        self._child_policy.populate(cluster=cluster, hosts=hosts)

    def make_query_plan(self, working_keyspace=None, query=None):
        """
        Defers to the child policy's
        :meth:`.LoadBalancingPolicy.make_query_plan` and filters the results.

        Note that this filtering may break desirable properties of the wrapped
        policy in some cases. For instance, imagine if you configure this
        policy to filter out ``host2``, and to wrap a round-robin policy that
        rotates through three hosts in the order ``host1, host2, host3``,
        ``host2, host3, host1``, ``host3, host1, host2``, repeating. This
        policy will yield ``host1, host3``, ``host3, host1``, ``host3, host1``,
        disproportionately favoring ``host3``.
        """
        child_qp = self._child_policy.make_query_plan(
            working_keyspace=working_keyspace, query=query
        )
        for host in child_qp:
            if self.predicate(host):
                yield host

    def check_supported(self):
        return self._child_policy.check_supported()


class ConvictionPolicy(object):
    """
    A policy which decides when hosts should be considered down
    based on the types of failures and the number of failures.

    If custom behavior is needed, this class may be subclassed.
    """

    def __init__(self, host):
        """
        `host` is an instance of :class:`.Host`.
        """
        self.host = host

    def add_failure(self, connection_exc):
        """
        Implementations should return :const:`True` if the host should be
        convicted, :const:`False` otherwise.
        """
        raise NotImplementedError()

    def reset(self):
        """
        Implementations should clear out any convictions or state regarding
        the host.
        """
        raise NotImplementedError()


class SimpleConvictionPolicy(ConvictionPolicy):
    """
    The default implementation of :class:`ConvictionPolicy`,
    which simply marks a host as down after the first failure
    of any kind.
    """

    def add_failure(self, connection_exc):
        return not isinstance(connection_exc, OperationTimedOut)

    def reset(self):
        pass


class ReconnectionPolicy(object):
    """
    This class and its subclasses govern how frequently an attempt is made
    to reconnect to nodes that are marked as dead.

    If custom behavior is needed, this class may be subclassed.
    """

    def new_schedule(self):
        """
        This should return a finite or infinite iterable of delays (each as a
        floating point number of seconds) in-between each failed reconnection
        attempt.  Note that if the iterable is finite, reconnection attempts
        will cease once the iterable is exhausted.
        """
        raise NotImplementedError()


class ConstantReconnectionPolicy(ReconnectionPolicy):
    """
    A :class:`.ReconnectionPolicy` subclass which sleeps for a fixed delay
    in-between each reconnection attempt.
    """

    def __init__(self, delay, max_attempts=64):
        """
        `delay` should be a floating point number of seconds to wait in-between
        each attempt.

        `max_attempts` should be a total number of attempts to be made before
        giving up, or :const:`None` to continue reconnection attempts forever.
        The default is 64.
        """
        if delay < 0:
            raise ValueError("delay must not be negative")
        if max_attempts is not None and max_attempts < 0:
            raise ValueError("max_attempts must not be negative")

        self.delay = delay
        self.max_attempts = max_attempts

    def new_schedule(self):
        if self.max_attempts:
            return repeat(self.delay, self.max_attempts)
        return repeat(self.delay)


class ExponentialReconnectionPolicy(ReconnectionPolicy):
    """
    A :class:`.ReconnectionPolicy` subclass which exponentially increases
    the length of the delay in-between each reconnection attempt up to
    a set maximum delay.

    A random amount of jitter (+/- 15%) will be added to the pure exponential
    delay value to avoid the situations where many reconnection handlers are
    trying to reconnect at exactly the same time.
    """

    # TODO: max_attempts is 64 to preserve legacy default behavior
    # consider changing to None in major release to prevent the policy
    # giving up forever
    def __init__(self, base_delay, max_delay, max_attempts=64):
        """
        `base_delay` and `max_delay` should be in floating point units of
        seconds.

        `max_attempts` should be a total number of attempts to be made before
        giving up, or :const:`None` to continue reconnection attempts forever.
        The default is 64.
        """
        if base_delay < 0 or max_delay < 0:
            raise ValueError("Delays may not be negative")

        if max_delay < base_delay:
            raise ValueError("Max delay must be greater than base delay")

        if max_attempts is not None and max_attempts < 0:
            raise ValueError("max_attempts must not be negative")

        self.base_delay = base_delay
        self.max_delay = max_delay
        self.max_attempts = max_attempts

    def new_schedule(self):
        i, overflowed = 0, False
        while self.max_attempts is None or i < self.max_attempts:
            if overflowed:
                yield self.max_delay
            else:
                try:
                    yield self._add_jitter(min(self.base_delay * (2 ** i), self.max_delay))
                except OverflowError:
                    overflowed = True
                    yield self.max_delay

            i += 1

    # Adds -+ 15% to the delay provided
    def _add_jitter(self, value):
        jitter = randint(85, 115)
        delay = (jitter * value) / 100
        return min(max(self.base_delay, delay), self.max_delay)


class RetryPolicy(object):
    """
    A policy that describes whether to retry, rethrow, or ignore coordinator
    timeout and unavailable failures. These are failures reported from the
    server side. Timeouts are configured by
    `settings in cassandra.yaml <https://github.com/apache/cassandra/blob/cassandra-2.1.4/conf/cassandra.yaml#L568-L584>`_.
    Unavailable failures occur when the coordinator cannot achieve the consistency
    level for a request. For further information see the method descriptions
    below.

    To specify a default retry policy, set the
    :attr:`.Cluster.default_retry_policy` attribute to an instance of this
    class or one of its subclasses.

    To specify a retry policy per query, set the :attr:`.Statement.retry_policy`
    attribute to an instance of this class or one of its subclasses.

    If custom behavior is needed for retrying certain operations,
    this class may be subclassed.
    """

    RETRY = 0
    """
    This should be returned from the below methods if the operation
    should be retried on the same connection.
    """

    RETHROW = 1
    """
    This should be returned from the below methods if the failure
    should be propagated and no more retries attempted.
    """

    IGNORE = 2
    """
    This should be returned from the below methods if the failure
    should be ignored but no more retries should be attempted.
    """

    RETRY_NEXT_HOST = 3
    """
    This should be returned from the below methods if the operation
    should be retried on another connection.
    """

    def on_read_timeout(self, query, consistency, required_responses,
                        received_responses, data_retrieved, retry_num):
        """
        This is called when a read operation times out from the coordinator's
        perspective (i.e. a replica did not respond to the coordinator in time).
        It should return a tuple with two items: one of the class enums (such
        as :attr:`.RETRY`) and a :class:`.ConsistencyLevel` to retry the
        operation at or :const:`None` to keep the same consistency level.

        `query` is the :class:`.Statement` that timed out.

        `consistency` is the :class:`.ConsistencyLevel` that the operation was
        attempted at.

        The `required_responses` and `received_responses` parameters describe
        how many replicas needed to respond to meet the requested consistency
        level and how many actually did respond before the coordinator timed
        out the request. `data_retrieved` is a boolean indicating whether
        any of those responses contained data (as opposed to just a digest).

        `retry_num` counts how many times the operation has been retried, so
        the first time this method is called, `retry_num` will be 0.

        By default, operations will be retried at most once, and only if
        a sufficient number of replicas responded (with data digests).
        """
        if retry_num != 0:
            return self.RETHROW, None
        elif received_responses >= required_responses and not data_retrieved:
            return self.RETRY, consistency
        else:
            return self.RETHROW, None

    def on_write_timeout(self, query, consistency, write_type,
                         required_responses, received_responses, retry_num):
        """
        This is called when a write operation times out from the coordinator's
        perspective (i.e. a replica did not respond to the coordinator in time).

        `query` is the :class:`.Statement` that timed out.

        `consistency` is the :class:`.ConsistencyLevel` that the operation was
        attempted at.

        `write_type` is one of the :class:`.WriteType` enums describing the
        type of write operation.

        The `required_responses` and `received_responses` parameters describe
        how many replicas needed to acknowledge the write to meet the requested
        consistency level and how many replicas actually did acknowledge the
        write before the coordinator timed out the request.

        `retry_num` counts how many times the operation has been retried, so
        the first time this method is called, `retry_num` will be 0.

        By default, a failed write operations will be retried at most once, and
        will only be retried if the `write_type` was
        :attr:`~.WriteType.BATCH_LOG`.
        """
        if retry_num != 0:
            return self.RETHROW, None
        elif write_type == WriteType.BATCH_LOG:
            return self.RETRY, consistency
        else:
            return self.RETHROW, None

    def on_unavailable(self, query, consistency, required_replicas, alive_replicas, retry_num):
        """
        This is called when the coordinator node determines that a read or
        write operation cannot be successful because the number of live
        replicas are too low to meet the requested :class:`.ConsistencyLevel`.
        This means that the read or write operation was never forwarded to
        any replicas.

        `query` is the :class:`.Statement` that failed.

        `consistency` is the :class:`.ConsistencyLevel` that the operation was
        attempted at.

        `required_replicas` is the number of replicas that would have needed to
        acknowledge the operation to meet the requested consistency level.
        `alive_replicas` is the number of replicas that the coordinator
        considered alive at the time of the request.

        `retry_num` counts how many times the operation has been retried, so
        the first time this method is called, `retry_num` will be 0.

        By default, if this is the first retry, it triggers a retry on the next
        host in the query plan with the same consistency level. If this is not the
        first retry, no retries will be attempted and the error will be re-raised.
        """
        return (self.RETRY_NEXT_HOST, None) if retry_num == 0 else (self.RETHROW, None)

    def on_request_error(self, query, consistency, error, retry_num):
        """
        This is called when an unexpected error happens. This can be in the
        following situations:

        * On a connection error
        * On server errors: overloaded, isBootstrapping, serverError, etc.

        `query` is the :class:`.Statement` that timed out.

        `consistency` is the :class:`.ConsistencyLevel` that the operation was
        attempted at.

        `error` the instance of the exception.

        `retry_num` counts how many times the operation has been retried, so
        the first time this method is called, `retry_num` will be 0.

        By default, it triggers a retry on the next host in the query plan
        with the same consistency level.
        """
        # TODO revisit this for the next major
        # To preserve the same behavior than before, we don't take retry_num into account
        return self.RETRY_NEXT_HOST, None


class FallthroughRetryPolicy(RetryPolicy):
    """
    A retry policy that never retries and always propagates failures to
    the application.
    """

    def on_read_timeout(self, *args, **kwargs):
        return self.RETHROW, None

    def on_write_timeout(self, *args, **kwargs):
        return self.RETHROW, None

    def on_unavailable(self, *args, **kwargs):
        return self.RETHROW, None

    def on_request_error(self, *args, **kwargs):
        return self.RETHROW, None


class DowngradingConsistencyRetryPolicy(RetryPolicy):
    """
    *Deprecated:* This retry policy will be removed in the next major release.

    A retry policy that sometimes retries with a lower consistency level than
    the one initially requested.

    **BEWARE**: This policy may retry queries using a lower consistency
    level than the one initially requested. By doing so, it may break
    consistency guarantees. In other words, if you use this retry policy,
    there are cases (documented below) where a read at :attr:`~.QUORUM`
    *may not* see a preceding write at :attr:`~.QUORUM`. Do not use this
    policy unless you have understood the cases where this can happen and
    are ok with that. It is also recommended to subclass this class so
    that queries that required a consistency level downgrade can be
    recorded (so that repairs can be made later, etc.).

    This policy implements the same retries as :class:`.RetryPolicy`,
    but on top of that, it also retries in the following cases:

    * On a read timeout: if the number of replicas that responded is
      greater than one but lower than is required by the requested
      consistency level, the operation is retried at a lower consistency
      level.
    * On a write timeout: if the operation is an :attr:`~.UNLOGGED_BATCH`
      and at least one replica acknowledged the write, the operation is
      retried at a lower consistency level.  Furthermore, for other
      write types, if at least one replica acknowledged the write, the
      timeout is ignored.
    * On an unavailable exception: if at least one replica is alive, the
      operation is retried at a lower consistency level.

    The reasoning behind this retry policy is as follows: if, based
    on the information the Cassandra coordinator node returns, retrying the
    operation with the initially requested consistency has a chance to
    succeed, do it. Otherwise, if based on that information we know the
    initially requested consistency level cannot be achieved currently, then:

    * For writes, ignore the exception (thus silently failing the
      consistency requirement) if we know the write has been persisted on at
      least one replica.
    * For reads, try reading at a lower consistency level (thus silently
      failing the consistency requirement).

    In other words, this policy implements the idea that if the requested
    consistency level cannot be achieved, the next best thing for writes is
    to make sure the data is persisted, and that reading something is better
    than reading nothing, even if there is a risk of reading stale data.
    """
    def __init__(self, *args, **kwargs):
        super(DowngradingConsistencyRetryPolicy, self).__init__(*args, **kwargs)
        warnings.warn('DowngradingConsistencyRetryPolicy is deprecated '
                      'and will be removed in the next major release.',
                      DeprecationWarning)

    def _pick_consistency(self, num_responses):
        if num_responses >= 3:
            return self.RETRY, ConsistencyLevel.THREE
        elif num_responses >= 2:
            return self.RETRY, ConsistencyLevel.TWO
        elif num_responses >= 1:
            return self.RETRY, ConsistencyLevel.ONE
        else:
            return self.RETHROW, None

    def on_read_timeout(self, query, consistency, required_responses,
                        received_responses, data_retrieved, retry_num):
        if retry_num != 0:
            return self.RETHROW, None
        elif ConsistencyLevel.is_serial(consistency):
            # Downgrading does not make sense for a CAS read query
            return self.RETHROW, None
        elif received_responses < required_responses:
            return self._pick_consistency(received_responses)
        elif not data_retrieved:
            return self.RETRY, consistency
        else:
            return self.RETHROW, None

    def on_write_timeout(self, query, consistency, write_type,
                         required_responses, received_responses, retry_num):
        if retry_num != 0:
            return self.RETHROW, None

        if write_type in (WriteType.SIMPLE, WriteType.BATCH, WriteType.COUNTER):
            if received_responses > 0:
                # persisted on at least one replica
                return self.IGNORE, None
            else:
                return self.RETHROW, None
        elif write_type == WriteType.UNLOGGED_BATCH:
            return self._pick_consistency(received_responses)
        elif write_type == WriteType.BATCH_LOG:
            return self.RETRY, consistency

        return self.RETHROW, None

    def on_unavailable(self, query, consistency, required_replicas, alive_replicas, retry_num):
        if retry_num != 0:
            return self.RETHROW, None
        elif ConsistencyLevel.is_serial(consistency):
            # failed at the paxos phase of a LWT, retry on the next host
            return self.RETRY_NEXT_HOST, None
        else:
            return self._pick_consistency(alive_replicas)


class AddressTranslator(object):
    """
    Interface for translating cluster-defined endpoints.

    The driver discovers nodes using server metadata and topology change events. Normally,
    the endpoint defined by the server is the right way to connect to a node. In some environments,
    these addresses may not be reachable, or not preferred (public vs. private IPs in cloud environments,
    suboptimal routing, etc.). This interface allows for translating from server defined endpoints to
    preferred addresses for driver connections.

    *Note:* :attr:`~Cluster.contact_points` provided while creating the :class:`~.Cluster` instance are not
    translated using this mechanism -- only addresses received from Cassandra nodes are.
    """
    def translate(self, addr):
        """
        Accepts the node ip address, and returns a translated address to be used connecting to this node.
        """
        raise NotImplementedError()


class IdentityTranslator(AddressTranslator):
    """
    Returns the endpoint with no translation
    """
    def translate(self, addr):
        return addr


class EC2MultiRegionTranslator(AddressTranslator):
    """
    Resolves private ips of the hosts in the same datacenter as the client, and public ips of hosts in other datacenters.
    """
    def translate(self, addr):
        """
        Reverse DNS the public broadcast_address, then lookup that hostname to get the AWS-resolved IP, which
        will point to the private IP address within the same datacenter.
        """
        # get family of this address, so we translate to the same
        family = socket.getaddrinfo(addr, 0, socket.AF_UNSPEC, socket.SOCK_STREAM)[0][0]
        host = socket.getfqdn(addr)
        for a in socket.getaddrinfo(host, 0, family, socket.SOCK_STREAM):
            try:
                return a[4][0]
            except Exception:
                pass
        return addr


class SpeculativeExecutionPolicy(object):
    """
    Interface for specifying speculative execution plans
    """

    def new_plan(self, keyspace, statement):
        """
        Returns

        :param keyspace:
        :param statement:
        :return:
        """
        raise NotImplementedError()


class SpeculativeExecutionPlan(object):
    def next_execution(self, host):
        raise NotImplementedError()


class NoSpeculativeExecutionPlan(SpeculativeExecutionPlan):
    def next_execution(self, host):
        return -1


class NoSpeculativeExecutionPolicy(SpeculativeExecutionPolicy):

    def new_plan(self, keyspace, statement):
        return NoSpeculativeExecutionPlan()


class ConstantSpeculativeExecutionPolicy(SpeculativeExecutionPolicy):
    """
    A speculative execution policy that sends a new query every X seconds (**delay**) for a maximum of Y attempts (**max_attempts**).
    """

    def __init__(self, delay, max_attempts):
        self.delay = delay
        self.max_attempts = max_attempts

    class ConstantSpeculativeExecutionPlan(SpeculativeExecutionPlan):
        def __init__(self, delay, max_attempts):
            self.delay = delay
            self.remaining = max_attempts

        def next_execution(self, host):
            if self.remaining > 0:
                self.remaining -= 1
                return self.delay
            else:
                return -1

    def new_plan(self, keyspace, statement):
        return self.ConstantSpeculativeExecutionPlan(self.delay, self.max_attempts)


class WrapperPolicy(LoadBalancingPolicy):

    def __init__(self, child_policy):
        self._child_policy = child_policy

    def distance(self, *args, **kwargs):
        return self._child_policy.distance(*args, **kwargs)

    def populate(self, cluster, hosts):
        self._child_policy.populate(cluster, hosts)

    def on_up(self, *args, **kwargs):
        return self._child_policy.on_up(*args, **kwargs)

    def on_down(self, *args, **kwargs):
        return self._child_policy.on_down(*args, **kwargs)

    def on_add(self, *args, **kwargs):
        return self._child_policy.on_add(*args, **kwargs)

    def on_remove(self, *args, **kwargs):
        return self._child_policy.on_remove(*args, **kwargs)


class DefaultLoadBalancingPolicy(WrapperPolicy):
    """
    A :class:`.LoadBalancingPolicy` wrapper that adds the ability to target a specific host first.

    If no host is set on the query, the child policy's query plan will be used as is.
    """

    _cluster_metadata = None

    def populate(self, cluster, hosts):
        self._cluster_metadata = cluster.metadata
        self._child_policy.populate(cluster, hosts)

    def make_query_plan(self, working_keyspace=None, query=None):
        if query and query.keyspace:
            keyspace = query.keyspace
        else:
            keyspace = working_keyspace

        # TODO remove next major since execute(..., host=XXX) is now available
        addr = getattr(query, 'target_host', None) if query else None
        target_host = self._cluster_metadata.get_host(addr)

        child = self._child_policy
        if target_host and target_host.is_up:
            yield target_host
            for h in child.make_query_plan(keyspace, query):
                if h != target_host:
                    yield h
        else:
            for h in child.make_query_plan(keyspace, query):
                yield h


# TODO for backward compatibility, remove in next major
class DSELoadBalancingPolicy(DefaultLoadBalancingPolicy):
    """
    *Deprecated:* This will be removed in the next major release,
    consider using :class:`.DefaultLoadBalancingPolicy`.
    """
    def __init__(self, *args, **kwargs):
        super(DSELoadBalancingPolicy, self).__init__(*args, **kwargs)
        warnings.warn("DSELoadBalancingPolicy will be removed in 4.0. Consider using "
                      "DefaultLoadBalancingPolicy.", DeprecationWarning)


class NeverRetryPolicy(RetryPolicy):
    def _rethrow(self, *args, **kwargs):
        return self.RETHROW, None

    on_read_timeout = _rethrow
    on_write_timeout = _rethrow
    on_unavailable = _rethrow


ColDesc = namedtuple('ColDesc', ['ks', 'table', 'col'])

class ColumnEncryptionPolicy(object):
    """
    A policy enabling (mostly) transparent encryption and decryption of data before it is
    sent to the cluster.

    Key materials and other configurations are specified on a per-column basis.  This policy can
    then be used by driver structures which are aware of the underlying columns involved in their
    work.  In practice this includes the following cases:

    * Prepared statements - data for columns specified by the cluster's policy will be transparently
      encrypted before they are sent
    * Rows returned from any query - data for columns specified by the cluster's policy will be
      transparently decrypted before they are returned to the user

    To enable this functionality, create an instance of this class (or more likely a subclass)
    before creating a cluster.  This policy should then be configured and supplied to the Cluster
    at creation time via the :attr:`.Cluster.column_encryption_policy` attribute.
    """

    def encrypt(self, coldesc, obj_bytes):
        """
        Encrypt the specified bytes using the cryptography materials for the specified column.
        Largely used internally, although this could also be used to encrypt values supplied
        to non-prepared statements in a way that is consistent with this policy.
        """
        raise NotImplementedError()

    def decrypt(self, coldesc, encrypted_bytes):
        """
        Decrypt the specified (encrypted) bytes using the cryptography materials for the
        specified column.  Used internally; could be used externally as well but there's
        not currently an obvious use case.
        """
        raise NotImplementedError()

    def add_column(self, coldesc, key):
        """
        Provide cryptography materials to be used when encrypted and/or decrypting data
        for the specified column.
        """
        raise NotImplementedError()

    def contains_column(self, coldesc):
        """
        Predicate to determine if a specific column is supported by this policy.
        Currently only used internally.
        """
        raise NotImplementedError()

    def encode_and_encrypt(self, coldesc, obj):
        """
        Helper function to enable use of this policy on simple (i.e. non-prepared)
        statements.
        """
        raise NotImplementedError()
