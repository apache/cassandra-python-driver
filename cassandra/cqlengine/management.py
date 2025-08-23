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
import json
import logging
import os
import warnings
from itertools import product

from cassandra import metadata
from cassandra.cqlengine import CQLEngineException
from cassandra.cqlengine import columns, query
from cassandra.cqlengine.connection import execute, get_cluster, format_log_context
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.named import NamedTable
from cassandra.cqlengine.usertype import UserType

CQLENG_ALLOW_SCHEMA_MANAGEMENT = 'CQLENG_ALLOW_SCHEMA_MANAGEMENT'

Field = namedtuple('Field', ['name', 'type'])

log = logging.getLogger(__name__)

# system keyspaces
schema_columnfamilies = NamedTable('system', 'schema_columnfamilies')


def _get_context(keyspaces, connections):
    """Return all the execution contexts"""

    if keyspaces:
        if not isinstance(keyspaces, (list, tuple)):
            raise ValueError('keyspaces must be a list or a tuple.')

    if connections:
        if not isinstance(connections, (list, tuple)):
            raise ValueError('connections must be a list or a tuple.')

    keyspaces = keyspaces if keyspaces else [None]
    connections = connections if connections else [None]

    return product(connections, keyspaces)


def create_keyspace_simple(name, replication_factor, durable_writes=True, connections=None):
    """
    Creates a keyspace with SimpleStrategy for replica placement

    If the keyspace already exists, it will not be modified.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*

    :param str name: name of keyspace to create
    :param int replication_factor: keyspace replication factor, used with :attr:`~.SimpleStrategy`
    :param bool durable_writes: Write log is bypassed if set to False
    :param list connections: List of connection names
    """
    _create_keyspace(name, durable_writes, 'SimpleStrategy',
                     {'replication_factor': replication_factor}, connections=connections)


def create_keyspace_network_topology(name, dc_replication_map, durable_writes=True, connections=None):
    """
    Creates a keyspace with NetworkTopologyStrategy for replica placement

    If the keyspace already exists, it will not be modified.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*

    :param str name: name of keyspace to create
    :param dict dc_replication_map: map of dc_names: replication_factor
    :param bool durable_writes: Write log is bypassed if set to False
    :param list connections: List of connection names
    """
    _create_keyspace(name, durable_writes, 'NetworkTopologyStrategy', dc_replication_map, connections=connections)


def _create_keyspace(name, durable_writes, strategy_class, strategy_options, connections=None):
    if not _allow_schema_modification():
        return

    if connections:
        if not isinstance(connections, (list, tuple)):
            raise ValueError('Connections must be a list or a tuple.')

    def __create_keyspace(name, durable_writes, strategy_class, strategy_options, connection=None):
        cluster = get_cluster(connection)

        if name not in cluster.metadata.keyspaces:
            log.info(format_log_context("Creating keyspace %s", connection=connection), name)
            ks_meta = metadata.KeyspaceMetadata(name, durable_writes, strategy_class, strategy_options)
            execute(ks_meta.as_cql_query(), connection=connection)
        else:
            log.info(format_log_context("Not creating keyspace %s because it already exists", connection=connection), name)

    if connections:
        for connection in connections:
            __create_keyspace(name, durable_writes, strategy_class, strategy_options, connection=connection)
    else:
        __create_keyspace(name, durable_writes, strategy_class, strategy_options)


def drop_keyspace(name, connections=None):
    """
    Drops a keyspace, if it exists.

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    :param str name: name of keyspace to drop
    :param list connections: List of connection names
    """
    if not _allow_schema_modification():
        return

    if connections:
        if not isinstance(connections, (list, tuple)):
            raise ValueError('Connections must be a list or a tuple.')

    def _drop_keyspace(name, connection=None):
        cluster = get_cluster(connection)
        if name in cluster.metadata.keyspaces:
            execute("DROP KEYSPACE {0}".format(metadata.protect_name(name)), connection=connection)

    if connections:
        for connection in connections:
            _drop_keyspace(name, connection)
    else:
        _drop_keyspace(name)

def _get_index_name_by_column(table, column_name):
    """
    Find the index name for a given table and column.
    """
    protected_name = metadata.protect_name(column_name)
    possible_index_values = [protected_name, "values(%s)" % protected_name]
    for index_metadata in table.indexes.values():
        options = dict(index_metadata.index_options)
        if options.get('target') in possible_index_values:
            return index_metadata.name


def sync_table(model, keyspaces=None, connections=None):
    """
    Inspects the model and creates / updates the corresponding table and columns.

    If `keyspaces` is specified, the table will be synched for all specified keyspaces.
    Note that the `Model.__keyspace__` is ignored in that case.

    If `connections` is specified, the table will be synched for all specified connections. Note that the `Model.__connection__` is ignored in that case.
    If not specified, it will try to get the connection from the Model.

    Any User Defined Types used in the table are implicitly synchronized.

    This function can only add fields that are not part of the primary key.

    Note that the attributes removed from the model are not deleted on the database.
    They become effectively ignored by (will not show up on) the model.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*
    """

    context = _get_context(keyspaces, connections)
    for connection, keyspace in context:
        with query.ContextQuery(model, keyspace=keyspace) as m:
            _sync_table(m, connection=connection)


def _sync_table(model, connection=None):
    if not _allow_schema_modification():
        return

    if not issubclass(model, Model):
        raise CQLEngineException("Models must be derived from base Model.")

    if model.__abstract__:
        raise CQLEngineException("cannot create table from abstract model")

    cf_name = model.column_family_name()
    raw_cf_name = model._raw_column_family_name()

    ks_name = model._get_keyspace()
    connection = connection or model._get_connection()

    cluster = get_cluster(connection)

    try:
        keyspace = cluster.metadata.keyspaces[ks_name]
    except KeyError:
        msg = format_log_context("Keyspace '{0}' for model {1} does not exist.", connection=connection)
        raise CQLEngineException(msg.format(ks_name, model))

    tables = keyspace.tables

    syncd_types = set()
    for col in model._columns.values():
        udts = []
        columns.resolve_udts(col, udts)
        for udt in [u for u in udts if u not in syncd_types]:
            _sync_type(ks_name, udt, syncd_types, connection=connection)

    if raw_cf_name not in tables:
        log.debug(format_log_context("sync_table creating new table %s", keyspace=ks_name, connection=connection), cf_name)
        qs = _get_create_table(model)

        try:
            execute(qs, connection=connection)
        except CQLEngineException as ex:
            # 1.2 doesn't return cf names, so we have to examine the exception
            # and ignore if it says the column family already exists
            if "Cannot add already existing column family" not in str(ex):
                raise
    else:
        log.debug(format_log_context("sync_table checking existing table %s", keyspace=ks_name, connection=connection), cf_name)
        table_meta = tables[raw_cf_name]

        _validate_pk(model, table_meta)

        table_columns = table_meta.columns
        model_fields = set()

        for model_name, col in model._columns.items():
            db_name = col.db_field_name
            model_fields.add(db_name)
            if db_name in table_columns:
                col_meta = table_columns[db_name]
                if col_meta.cql_type != col.db_type:
                    msg = format_log_context('Existing table {0} has column "{1}" with a type ({2}) differing from the model type ({3}).'
                                  ' Model should be updated.', keyspace=ks_name, connection=connection)
                    msg = msg.format(cf_name, db_name, col_meta.cql_type, col.db_type)
                    warnings.warn(msg)
                    log.warning(msg)

                continue

            if col.primary_key:
                msg = format_log_context("Cannot add primary key '{0}' (with db_field '{1}') to existing table {2}", keyspace=ks_name, connection=connection)
                raise CQLEngineException(msg.format(model_name, db_name, cf_name))

            query = "ALTER TABLE {0} add {1}".format(cf_name, col.get_column_def())
            execute(query, connection=connection)

        db_fields_not_in_model = model_fields.symmetric_difference(table_columns)
        if db_fields_not_in_model:
            msg = format_log_context("Table {0} has fields not referenced by model: {1}", keyspace=ks_name, connection=connection)
            log.info(msg.format(cf_name, db_fields_not_in_model))

        _update_options(model, connection=connection)

    table = cluster.metadata.keyspaces[ks_name].tables[raw_cf_name]

    indexes = [c for n, c in model._columns.items() if c.index]

    # TODO: support multiple indexes in C* 3.0+
    for column in indexes:
        index_name = _get_index_name_by_column(table, column.db_field_name)
        if index_name:
            continue

        qs = ['CREATE INDEX']
        qs += ['ON {0}'.format(cf_name)]
        qs += ['("{0}")'.format(column.db_field_name)]
        qs = ' '.join(qs)
        execute(qs, connection=connection)


def _validate_pk(model, table_meta):
    model_partition = [c.db_field_name for c in model._partition_keys.values()]
    meta_partition = [c.name for c in table_meta.partition_key]
    model_clustering = [c.db_field_name for c in model._clustering_keys.values()]
    meta_clustering = [c.name for c in table_meta.clustering_key]

    if model_partition != meta_partition or model_clustering != meta_clustering:
        def _pk_string(partition, clustering):
            return "PRIMARY KEY (({0}){1})".format(', '.join(partition), ', ' + ', '.join(clustering) if clustering else '')
        raise CQLEngineException("Model {0} PRIMARY KEY composition does not match existing table {1}. "
                                 "Model: {2}; Table: {3}. "
                                 "Update model or drop the table.".format(model, model.column_family_name(),
                                                                          _pk_string(model_partition, model_clustering),
                                                                          _pk_string(meta_partition, meta_clustering)))


def sync_type(ks_name, type_model, connection=None):
    """
    Inspects the type_model and creates / updates the corresponding type.

    Note that the attributes removed from the type_model are not deleted on the database (this operation is not supported).
    They become effectively ignored by (will not show up on) the type_model.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*
    """
    if not _allow_schema_modification():
        return

    if not issubclass(type_model, UserType):
        raise CQLEngineException("Types must be derived from base UserType.")

    _sync_type(ks_name, type_model, connection=connection)


def _sync_type(ks_name, type_model, omit_subtypes=None, connection=None):

    syncd_sub_types = omit_subtypes or set()
    for field in type_model._fields.values():
        udts = []
        columns.resolve_udts(field, udts)
        for udt in [u for u in udts if u not in syncd_sub_types]:
            _sync_type(ks_name, udt, syncd_sub_types, connection=connection)
            syncd_sub_types.add(udt)

    type_name = type_model.type_name()
    type_name_qualified = "%s.%s" % (ks_name, type_name)

    cluster = get_cluster(connection)

    keyspace = cluster.metadata.keyspaces[ks_name]
    defined_types = keyspace.user_types

    if type_name not in defined_types:
        log.debug(format_log_context("sync_type creating new type %s", keyspace=ks_name, connection=connection), type_name_qualified)
        cql = get_create_type(type_model, ks_name)
        execute(cql, connection=connection)
        cluster.refresh_user_type_metadata(ks_name, type_name)
        type_model.register_for_keyspace(ks_name, connection=connection)
    else:
        type_meta = defined_types[type_name]
        defined_fields = type_meta.field_names
        model_fields = set()
        for field in type_model._fields.values():
            model_fields.add(field.db_field_name)
            if field.db_field_name not in defined_fields:
                execute("ALTER TYPE {0} ADD {1}".format(type_name_qualified, field.get_column_def()), connection=connection)
            else:
                field_type = type_meta.field_types[defined_fields.index(field.db_field_name)]
                if field_type != field.db_type:
                    msg = format_log_context('Existing user type {0} has field "{1}" with a type ({2}) differing from the model user type ({3}).'
                                  ' UserType should be updated.', keyspace=ks_name, connection=connection)
                    msg = msg.format(type_name_qualified, field.db_field_name, field_type, field.db_type)
                    warnings.warn(msg)
                    log.warning(msg)

        type_model.register_for_keyspace(ks_name, connection=connection)

        if len(defined_fields) == len(model_fields):
            log.info(format_log_context("Type %s did not require synchronization", keyspace=ks_name, connection=connection), type_name_qualified)
            return

        db_fields_not_in_model = model_fields.symmetric_difference(defined_fields)
        if db_fields_not_in_model:
            msg = format_log_context("Type %s has fields not referenced by model: %s", keyspace=ks_name, connection=connection)
            log.info(msg, type_name_qualified, db_fields_not_in_model)


def get_create_type(type_model, keyspace):
    type_meta = metadata.UserType(keyspace,
                                  type_model.type_name(),
                                  (f.db_field_name for f in type_model._fields.values()),
                                  (v.db_type for v in type_model._fields.values()))
    return type_meta.as_cql_query()


def _get_create_table(model):
    ks_table_name = model.column_family_name()
    query_strings = ['CREATE TABLE {0}'.format(ks_table_name)]

    # add column types
    pkeys = []  # primary keys
    ckeys = []  # clustering keys
    qtypes = []  # field types

    def add_column(col):
        s = col.get_column_def()
        if col.primary_key:
            keys = (pkeys if col.partition_key else ckeys)
            keys.append('"{0}"'.format(col.db_field_name))
        qtypes.append(s)

    for name, col in model._columns.items():
        add_column(col)

    qtypes.append('PRIMARY KEY (({0}){1})'.format(', '.join(pkeys), ckeys and ', ' + ', '.join(ckeys) or ''))

    query_strings += ['({0})'.format(', '.join(qtypes))]

    property_strings = []

    _order = ['"{0}" {1}'.format(c.db_field_name, c.clustering_order or 'ASC') for c in model._clustering_keys.values()]
    if _order:
        property_strings.append('CLUSTERING ORDER BY ({0})'.format(', '.join(_order)))

    # options strings use the V3 format, which matches CQL more closely and does not require mapping
    property_strings += metadata.TableMetadataV3._make_option_strings(model.__options__ or {})

    if property_strings:
        query_strings += ['WITH {0}'.format(' AND '.join(property_strings))]

    return ' '.join(query_strings)


def _get_table_metadata(model, connection=None):
    # returns the table as provided by the native driver for a given model
    cluster = get_cluster(connection)
    ks = model._get_keyspace()
    table = model._raw_column_family_name()
    table = cluster.metadata.keyspaces[ks].tables[table]
    return table


def _options_map_from_strings(option_strings):
    # converts options strings to a mapping to strings or dict
    options = {}
    for option in option_strings:
        name, value = option.split('=')
        i = value.find('{')
        if i >= 0:
            value = value[i:value.rfind('}') + 1].replace("'", '"')  # from cql single quotes to json double; not aware of any values that would be escaped right now
            value = json.loads(value)
        else:
            value = value.strip()
        options[name.strip()] = value
    return options


def _update_options(model, connection=None):
    """Updates the table options for the given model if necessary.

    :param model: The model to update.
    :param connection: Name of the connection to use

    :return: `True`, if the options were modified in Cassandra,
        `False` otherwise.
    :rtype: bool
    """
    ks_name = model._get_keyspace()
    msg = format_log_context("Checking %s for option differences", keyspace=ks_name, connection=connection)
    log.debug(msg, model)
    model_options = model.__options__ or {}

    table_meta = _get_table_metadata(model, connection=connection)
    # go to CQL string first to normalize meta from different versions
    existing_option_strings = set(table_meta._make_option_strings(table_meta.options))
    existing_options = _options_map_from_strings(existing_option_strings)
    model_option_strings = metadata.TableMetadataV3._make_option_strings(model_options)
    model_options = _options_map_from_strings(model_option_strings)

    update_options = {}
    for name, value in model_options.items():
        try:
            existing_value = existing_options[name]
        except KeyError:
            msg = format_log_context("Invalid table option: '%s'; known options: %s", keyspace=ks_name, connection=connection)
            raise KeyError(msg % (name, existing_options.keys()))
        if isinstance(existing_value, str):
            if value != existing_value:
                update_options[name] = value
        else:
            try:
                for k, v in value.items():
                    if existing_value[k] != v:
                        update_options[name] = value
                        break
            except KeyError:
                update_options[name] = value

    if update_options:
        options = ' AND '.join(metadata.TableMetadataV3._make_option_strings(update_options))
        query = "ALTER TABLE {0} WITH {1}".format(model.column_family_name(), options)
        execute(query, connection=connection)
        return True

    return False


def drop_table(model, keyspaces=None, connections=None):
    """
    Drops the table indicated by the model, if it exists.

    If `keyspaces` is specified, the table will be dropped for all specified keyspaces. Note that the `Model.__keyspace__` is ignored in that case.

    If `connections` is specified, the table will be synched for all specified connections. Note that the `Model.__connection__` is ignored in that case.
    If not specified, it will try to get the connection from the Model.


    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*
    """

    context = _get_context(keyspaces, connections)
    for connection, keyspace in context:
        with query.ContextQuery(model, keyspace=keyspace) as m:
            _drop_table(m, connection=connection)


def _drop_table(model, connection=None):
    if not _allow_schema_modification():
        return

    connection = connection or model._get_connection()

    # don't try to delete non existent tables
    meta = get_cluster(connection).metadata

    ks_name = model._get_keyspace()
    raw_cf_name = model._raw_column_family_name()

    try:
        meta.keyspaces[ks_name].tables[raw_cf_name]
        execute('DROP TABLE {0};'.format(model.column_family_name()), connection=connection)
    except KeyError:
        pass


def _allow_schema_modification():
    if not os.getenv(CQLENG_ALLOW_SCHEMA_MANAGEMENT):
        msg = CQLENG_ALLOW_SCHEMA_MANAGEMENT + " environment variable is not set. Future versions of this package will require this variable to enable management functions."
        warnings.warn(msg)
        log.warning(msg)

    return True
