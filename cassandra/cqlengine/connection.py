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

from collections import defaultdict
import logging
import threading

from cassandra.cluster import Cluster, _ConfigMode, _NOT_SET, NoHostAvailable, UserTypeDoesNotExist, ConsistencyLevel
from cassandra.query import SimpleStatement, dict_factory

from cassandra.cqlengine import CQLEngineException
from cassandra.cqlengine.statements import BaseCQLStatement


log = logging.getLogger(__name__)

NOT_SET = _NOT_SET  # required for passing timeout to Session.execute

cluster = None
session = None

# connections registry
DEFAULT_CONNECTION = object()
_connections = {}

# Because type models may be registered before a connection is present,
# and because sessions may be replaced, we must register UDTs here, in order
# to have them registered when a new session is established.
udt_by_keyspace = defaultdict(dict)


def format_log_context(msg, connection=None, keyspace=None):
    """Format log message to add keyspace and connection context"""
    connection_info = connection or 'DEFAULT_CONNECTION'

    if keyspace:
        msg = '[Connection: {0}, Keyspace: {1}] {2}'.format(connection_info, keyspace, msg)
    else:
        msg = '[Connection: {0}] {1}'.format(connection_info, msg)
    return msg


class UndefinedKeyspaceException(CQLEngineException):
    pass


class Connection(object):
    """CQLEngine Connection"""

    name = None
    hosts = None

    consistency = None
    retry_connect = False
    lazy_connect = False
    lazy_connect_lock = None
    cluster_options = None

    cluster = None
    session = None

    def __init__(self, name, hosts, consistency=None,
                 lazy_connect=False, retry_connect=False, cluster_options=None):
        self.hosts = hosts
        self.name = name
        self.consistency = consistency
        self.lazy_connect = lazy_connect
        self.retry_connect = retry_connect
        self.cluster_options = cluster_options if cluster_options else {}
        self.lazy_connect_lock = threading.RLock()

    @classmethod
    def from_session(cls, name, session):
        instance = cls(name=name, hosts=session.hosts)
        instance.cluster, instance.session = session.cluster, session
        instance.setup_session()
        return instance

    def setup(self):
        """Set up the connection"""
        global cluster, session

        if 'username' in self.cluster_options or 'password' in self.cluster_options:
            raise CQLEngineException("Username & Password are now handled by using the native driver's auth_provider")

        if self.lazy_connect:
            return

        if 'cloud' in self.cluster_options:
            if self.hosts:
                log.warning("Ignoring hosts %s because a cloud config was provided.", self.hosts)
            self.cluster = Cluster(**self.cluster_options)
        else:
            self.cluster = Cluster(self.hosts, **self.cluster_options)

        try:
            self.session = self.cluster.connect()
            log.debug(format_log_context("connection initialized with internally created session", connection=self.name))
        except NoHostAvailable:
            if self.retry_connect:
                log.warning(format_log_context("connect failed, setting up for re-attempt on first use", connection=self.name))
                self.lazy_connect = True
            raise

        if DEFAULT_CONNECTION in _connections and _connections[DEFAULT_CONNECTION] == self:
            cluster = _connections[DEFAULT_CONNECTION].cluster
            session = _connections[DEFAULT_CONNECTION].session

        self.setup_session()

    def setup_session(self):
        if self.cluster._config_mode == _ConfigMode.PROFILES:
            self.cluster.profile_manager.default.row_factory = dict_factory
            if self.consistency is not None:
                self.cluster.profile_manager.default.consistency_level = self.consistency
        else:
            self.session.row_factory = dict_factory
            if self.consistency is not None:
                self.session.default_consistency_level = self.consistency
        enc = self.session.encoder
        enc.mapping[tuple] = enc.cql_encode_tuple
        _register_known_types(self.session.cluster)

    def handle_lazy_connect(self):

        # if lazy_connect is False, it means the cluster is set up and ready
        # No need to acquire the lock
        if not self.lazy_connect:
            return

        with self.lazy_connect_lock:
            # lazy_connect might have been set to False by another thread while waiting the lock
            # In this case, do nothing.
            if self.lazy_connect:
                log.debug(format_log_context("Lazy connect enabled", connection=self.name))
                self.lazy_connect = False
                self.setup()


def register_connection(name, hosts=None, consistency=None, lazy_connect=False,
                        retry_connect=False, cluster_options=None, default=False,
                        session=None):
    """
    Add a connection to the connection registry. ``hosts`` and ``session`` are
    mutually exclusive, and ``consistency``, ``lazy_connect``,
    ``retry_connect``, and ``cluster_options`` only work with ``hosts``. Using
    ``hosts`` will create a new :class:`cassandra.cluster.Cluster` and
    :class:`cassandra.cluster.Session`.

    :param list hosts: list of hosts, (``contact_points`` for :class:`cassandra.cluster.Cluster`).
    :param int consistency: The default :class:`~.ConsistencyLevel` for the
        registered connection's new session. Default is the same as
        :attr:`.Session.default_consistency_level`. For use with ``hosts`` only;
        will fail when used with ``session``.
    :param bool lazy_connect: True if should not connect until first use. For
        use with ``hosts`` only; will fail when used with ``session``.
    :param bool retry_connect: True if we should retry to connect even if there
        was a connection failure initially. For use with ``hosts`` only; will
        fail when used with ``session``.
    :param dict cluster_options: A dict of options to be used as keyword
        arguments to :class:`cassandra.cluster.Cluster`. For use with ``hosts``
        only; will fail when used with ``session``.
    :param bool default: If True, set the new connection as the cqlengine
        default
    :param Session session: A :class:`cassandra.cluster.Session` to be used in
        the created connection.
    """

    if name in _connections:
        log.warning("Registering connection '{0}' when it already exists.".format(name))

    if session is not None:
        invalid_config_args = (hosts is not None or
                               consistency is not None or
                               lazy_connect is not False or
                               retry_connect is not False or
                               cluster_options is not None)
        if invalid_config_args:
            raise CQLEngineException(
                "Session configuration arguments and 'session' argument are mutually exclusive"
            )
        conn = Connection.from_session(name, session=session)
    else:  # use hosts argument
        conn = Connection(
            name, hosts=hosts,
            consistency=consistency, lazy_connect=lazy_connect,
            retry_connect=retry_connect, cluster_options=cluster_options
        )
        conn.setup()

    _connections[name] = conn

    if default:
        set_default_connection(name)

    return conn


def unregister_connection(name):
    global cluster, session

    if name not in _connections:
        return

    if DEFAULT_CONNECTION in _connections and _connections[name] == _connections[DEFAULT_CONNECTION]:
        del _connections[DEFAULT_CONNECTION]
        cluster = None
        session = None

    conn = _connections[name]
    if conn.cluster:
        conn.cluster.shutdown()
    del _connections[name]
    log.debug("Connection '{0}' has been removed from the registry.".format(name))


def set_default_connection(name):
    global cluster, session

    if name not in _connections:
        raise CQLEngineException("Connection '{0}' doesn't exist.".format(name))

    log.debug("Connection '{0}' has been set as default.".format(name))
    _connections[DEFAULT_CONNECTION] = _connections[name]
    cluster = _connections[name].cluster
    session = _connections[name].session


def get_connection(name=None):

    if not name:
        name = DEFAULT_CONNECTION

    if name not in _connections:
        raise CQLEngineException("Connection name '{0}' doesn't exist in the registry.".format(name))

    conn = _connections[name]
    conn.handle_lazy_connect()

    return conn


def default():
    """
    Configures the default connection to localhost, using the driver defaults
    (except for row_factory)
    """

    try:
        conn = get_connection()
        if conn.session:
            log.warning("configuring new default connection for cqlengine when one was already set")
    except:
        pass

    register_connection('default', hosts=None, default=True)

    log.debug("cqlengine connection initialized with default session to localhost")


def set_session(s):
    """
    Configures the default connection with a preexisting :class:`cassandra.cluster.Session`

    Note: the mapper presently requires a Session :attr:`~.row_factory` set to ``dict_factory``.
    This may be relaxed in the future
    """

    try:
        conn = get_connection()
    except CQLEngineException:
        # no default connection set; initialize one
        register_connection('default', session=s, default=True)
        conn = get_connection()
    else:
        if conn.session:
            log.warning("configuring new default session for cqlengine when one was already set")

    if not any([
        s.cluster.profile_manager.default.row_factory is dict_factory and s.cluster._config_mode in [_ConfigMode.PROFILES, _ConfigMode.UNCOMMITTED],
        s.row_factory is dict_factory and s.cluster._config_mode in [_ConfigMode.LEGACY, _ConfigMode.UNCOMMITTED],
    ]):
        raise CQLEngineException("Failed to initialize: row_factory must be 'dict_factory'")

    conn.session = s
    conn.cluster = s.cluster

    # Set default keyspace from given session's keyspace
    if conn.session.keyspace:
        from cassandra.cqlengine import models
        models.DEFAULT_KEYSPACE = conn.session.keyspace

    conn.setup_session()

    log.debug("cqlengine default connection initialized with %s", s)


# TODO next major: if a cloud config is specified in kwargs, hosts will be ignored.
# This function should be refactored to reflect this change. PYTHON-1265
def setup(
        hosts,
        default_keyspace,
        consistency=None,
        lazy_connect=False,
        retry_connect=False,
        **kwargs):
    """
    Set up the driver connection used by the mapper

    :param list hosts: list of hosts, (``contact_points`` for :class:`cassandra.cluster.Cluster`)
    :param str default_keyspace: The default keyspace to use
    :param int consistency: The global default :class:`~.ConsistencyLevel` - default is the same as :attr:`.Session.default_consistency_level`
    :param bool lazy_connect: True if should not connect until first use
    :param bool retry_connect: True if we should retry to connect even if there was a connection failure initially
    :param kwargs: Pass-through keyword arguments for :class:`cassandra.cluster.Cluster`
    """

    from cassandra.cqlengine import models
    models.DEFAULT_KEYSPACE = default_keyspace

    register_connection('default', hosts=hosts, consistency=consistency, lazy_connect=lazy_connect,
                        retry_connect=retry_connect, cluster_options=kwargs, default=True)


def execute(query, params=None, consistency_level=None, timeout=NOT_SET, connection=None):

    conn = get_connection(connection)

    if not conn.session:
        raise CQLEngineException("It is required to setup() cqlengine before executing queries")

    if isinstance(query, SimpleStatement):
        pass  #
    elif isinstance(query, BaseCQLStatement):
        params = query.get_context()
        query = SimpleStatement(str(query), consistency_level=consistency_level, fetch_size=query.fetch_size)
    elif isinstance(query, str):
        query = SimpleStatement(query, consistency_level=consistency_level)
    log.debug(format_log_context('Query: {}, Params: {}'.format(query.query_string, params), connection=connection))

    result = conn.session.execute(query, params, timeout=timeout)

    return result


def get_session(connection=None):
    conn = get_connection(connection)
    return conn.session


def get_cluster(connection=None):
    conn = get_connection(connection)
    if not conn.cluster:
        raise CQLEngineException("%s.cluster is not configured. Call one of the setup or default functions first." % __name__)
    return conn.cluster


def register_udt(keyspace, type_name, klass, connection=None):
    udt_by_keyspace[keyspace][type_name] = klass

    try:
        cluster = get_cluster(connection)
    except CQLEngineException:
        cluster = None

    if cluster:
        try:
            cluster.register_user_type(keyspace, type_name, klass)
        except UserTypeDoesNotExist:
            pass  # new types are covered in management sync functions


def _register_known_types(cluster):
    from cassandra.cqlengine import models
    for ks_name, name_type_map in udt_by_keyspace.items():
        for type_name, klass in name_type_map.items():
            try:
                cluster.register_user_type(ks_name or models.DEFAULT_KEYSPACE, type_name, klass)
            except UserTypeDoesNotExist:
                pass  # new types are covered in management sync functions
