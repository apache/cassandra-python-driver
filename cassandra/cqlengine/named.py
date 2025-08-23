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

from cassandra.util import OrderedDict

from cassandra.cqlengine import CQLEngineException
from cassandra.cqlengine.columns import Column
from cassandra.cqlengine.connection import get_cluster
from cassandra.cqlengine.models import UsingDescriptor, BaseModel
from cassandra.cqlengine.query import AbstractQueryableColumn, SimpleQuerySet
from cassandra.cqlengine.query import DoesNotExist as _DoesNotExist
from cassandra.cqlengine.query import MultipleObjectsReturned as _MultipleObjectsReturned


class QuerySetDescriptor(object):
    """
    returns a fresh queryset for the given model
    it's declared on everytime it's accessed
    """

    def __get__(self, obj, model):
        """ :rtype: ModelQuerySet """
        if model.__abstract__:
            raise CQLEngineException('cannot execute queries against abstract models')
        return SimpleQuerySet(obj)

    def __call__(self, *args, **kwargs):
        """
        Just a hint to IDEs that it's ok to call this

        :rtype: ModelQuerySet
        """
        raise NotImplementedError


class NamedColumn(AbstractQueryableColumn):
    """
    A column that is not coupled to a model class, or type
    """

    def __init__(self, name):
        self.name = name

    def __unicode__(self):
        return self.name

    def _get_column(self):
        """ :rtype: NamedColumn """
        return self

    @property
    def db_field_name(self):
        return self.name

    @property
    def cql(self):
        return self.get_cql()

    def get_cql(self):
        return '"{0}"'.format(self.name)

    def to_database(self, val):
        return val


class NamedTable(object):
    """
    A Table that is not coupled to a model class
    """

    __abstract__ = False

    objects = QuerySetDescriptor()

    __partition_keys = None

    _partition_key_index = None

    __connection__ = None
    _connection = None

    using = UsingDescriptor()

    _get_connection = BaseModel._get_connection

    class DoesNotExist(_DoesNotExist):
        pass

    class MultipleObjectsReturned(_MultipleObjectsReturned):
        pass

    def __init__(self, keyspace, name):
        self.keyspace = keyspace
        self.name = name
        self._connection = None

    @property
    def _partition_keys(self):
        if not self.__partition_keys:
            self._get_partition_keys()
        return self.__partition_keys

    def _get_partition_keys(self):
        try:
            table_meta = get_cluster(self._get_connection()).metadata.keyspaces[self.keyspace].tables[self.name]
            self.__partition_keys = OrderedDict((pk.name, Column(primary_key=True, partition_key=True, db_field=pk.name)) for pk in table_meta.partition_key)
        except Exception as e:
            raise CQLEngineException("Failed inspecting partition keys for {0}."
                                     "Ensure cqlengine is connected before attempting this with NamedTable.".format(self.column_family_name()))

    def column(self, name):
        return NamedColumn(name)

    def column_family_name(self, include_keyspace=True):
        """
        Returns the column family name if it's been defined
        otherwise, it creates it from the module and class name
        """
        if include_keyspace:
            return '{0}.{1}'.format(self.keyspace, self.name)
        else:
            return self.name

    def _get_column(self, name):
        """
        Returns the column matching the given name

        :rtype: Column
        """
        return self.column(name)

    # def create(self, **kwargs):
    #     return self.objects.create(**kwargs)

    def all(self):
        return self.objects.all()

    def filter(self, *args, **kwargs):
        return self.objects.filter(*args, **kwargs)

    def get(self, *args, **kwargs):
        return self.objects.get(*args, **kwargs)


class NamedKeyspace(object):
    """
    A keyspace
    """

    def __init__(self, name):
        self.name = name

    def table(self, name):
        """
        returns a table descriptor with the given
        name that belongs to this keyspace
        """
        return NamedTable(self.name, name)
