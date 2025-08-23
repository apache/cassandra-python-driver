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

import logging
import re
from warnings import warn

from cassandra.cqlengine import CQLEngineException, ValidationError
from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from cassandra.cqlengine import query
from cassandra.cqlengine.query import DoesNotExist as _DoesNotExist
from cassandra.cqlengine.query import MultipleObjectsReturned as _MultipleObjectsReturned
from cassandra.metadata import protect_name
from cassandra.util import OrderedDict

log = logging.getLogger(__name__)


def _clone_model_class(model, attrs):
    new_type = type(model.__name__, (model,), attrs)
    try:
        new_type.__abstract__ = model.__abstract__
        new_type.__discriminator_value__ = model.__discriminator_value__
        new_type.__default_ttl__ = model.__default_ttl__
    except AttributeError:
        pass
    return new_type


class ModelException(CQLEngineException):
    pass


class ModelDefinitionException(ModelException):
    pass


class PolymorphicModelException(ModelException):
    pass


class UndefinedKeyspaceWarning(Warning):
    pass

DEFAULT_KEYSPACE = None


class hybrid_classmethod(object):
    """
    Allows a method to behave as both a class method and
    normal instance method depending on how it's called
    """
    def __init__(self, clsmethod, instmethod):
        self.clsmethod = clsmethod
        self.instmethod = instmethod

    def __get__(self, instance, owner):
        if instance is None:
            return self.clsmethod.__get__(owner, owner)
        else:
            return self.instmethod.__get__(instance, owner)

    def __call__(self, *args, **kwargs):
        """
        Just a hint to IDEs that it's ok to call this
        """
        raise NotImplementedError


class QuerySetDescriptor(object):
    """
    returns a fresh queryset for the given model
    it's declared on everytime it's accessed
    """

    def __get__(self, obj, model):
        """ :rtype: ModelQuerySet """
        if model.__abstract__:
            raise CQLEngineException('cannot execute queries against abstract models')
        queryset = model.__queryset__(model)

        # if this is a concrete polymorphic model, and the discriminator
        # key is an indexed column, add a filter clause to only return
        # logical rows of the proper type
        if model._is_polymorphic and not model._is_polymorphic_base:
            name, column = model._discriminator_column_name, model._discriminator_column
            if column.partition_key or column.index:
                # look for existing poly types
                return queryset.filter(**{name: model.__discriminator_value__})

        return queryset

    def __call__(self, *args, **kwargs):
        """
        Just a hint to IDEs that it's ok to call this

        :rtype: ModelQuerySet
        """
        raise NotImplementedError


class ConditionalDescriptor(object):
    """
    returns a query set descriptor
    """
    def __get__(self, instance, model):
        if instance:
            def conditional_setter(*prepared_conditional, **unprepared_conditionals):
                if len(prepared_conditional) > 0:
                    conditionals = prepared_conditional[0]
                else:
                    conditionals = instance.objects.iff(**unprepared_conditionals)._conditional
                instance._conditional = conditionals
                return instance

            return conditional_setter
        qs = model.__queryset__(model)

        def conditional_setter(**unprepared_conditionals):
            conditionals = model.objects.iff(**unprepared_conditionals)._conditional
            qs._conditional = conditionals
            return qs
        return conditional_setter

    def __call__(self, *args, **kwargs):
        raise NotImplementedError


class TTLDescriptor(object):
    """
    returns a query set descriptor
    """
    def __get__(self, instance, model):
        if instance:
            # instance = copy.deepcopy(instance)
            # instance method
            def ttl_setter(ts):
                instance._ttl = ts
                return instance
            return ttl_setter

        qs = model.__queryset__(model)

        def ttl_setter(ts):
            qs._ttl = ts
            return qs

        return ttl_setter

    def __call__(self, *args, **kwargs):
        raise NotImplementedError


class TimestampDescriptor(object):
    """
    returns a query set descriptor with a timestamp specified
    """
    def __get__(self, instance, model):
        if instance:
            # instance method
            def timestamp_setter(ts):
                instance._timestamp = ts
                return instance
            return timestamp_setter

        return model.objects.timestamp

    def __call__(self, *args, **kwargs):
        raise NotImplementedError


class IfNotExistsDescriptor(object):
    """
    return a query set descriptor with an if_not_exists flag specified
    """
    def __get__(self, instance, model):
        if instance:
            # instance method
            def ifnotexists_setter(ife=True):
                instance._if_not_exists = ife
                return instance
            return ifnotexists_setter

        return model.objects.if_not_exists

    def __call__(self, *args, **kwargs):
        raise NotImplementedError


class IfExistsDescriptor(object):
    """
    return a query set descriptor with an if_exists flag specified
    """
    def __get__(self, instance, model):
        if instance:
            # instance method
            def ifexists_setter(ife=True):
                instance._if_exists = ife
                return instance
            return ifexists_setter

        return model.objects.if_exists

    def __call__(self, *args, **kwargs):
        raise NotImplementedError


class ConsistencyDescriptor(object):
    """
    returns a query set descriptor if called on Class, instance if it was an instance call
    """
    def __get__(self, instance, model):
        if instance:
            # instance = copy.deepcopy(instance)
            def consistency_setter(consistency):
                instance.__consistency__ = consistency
                return instance
            return consistency_setter

        qs = model.__queryset__(model)

        def consistency_setter(consistency):
            qs._consistency = consistency
            return qs

        return consistency_setter

    def __call__(self, *args, **kwargs):
        raise NotImplementedError


class UsingDescriptor(object):
    """
    return a query set descriptor with a connection context specified
    """
    def __get__(self, instance, model):
        if instance:
            # instance method
            def using_setter(connection=None):
                if connection:
                    instance._connection = connection
                return instance
            return using_setter

        return model.objects.using

    def __call__(self, *args, **kwargs):
        raise NotImplementedError


class ColumnQueryEvaluator(query.AbstractQueryableColumn):
    """
    Wraps a column and allows it to be used in comparator
    expressions, returning query operators

    ie:
    Model.column == 5
    """

    def __init__(self, column):
        self.column = column

    def __unicode__(self):
        return self.column.db_field_name

    def _get_column(self):
        return self.column


class ColumnDescriptor(object):
    """
    Handles the reading and writing of column values to and from
    a model instance's value manager, as well as creating
    comparator queries
    """

    def __init__(self, column):
        """
        :param column:
        :type column: columns.Column
        :return:
        """
        self.column = column
        self.query_evaluator = ColumnQueryEvaluator(self.column)

    def __get__(self, instance, owner):
        """
        Returns either the value or column, depending
        on if an instance is provided or not

        :param instance: the model instance
        :type instance: Model
        """
        try:
            return instance._values[self.column.column_name].getval()
        except AttributeError:
            return self.query_evaluator

    def __set__(self, instance, value):
        """
        Sets the value on an instance, raises an exception with classes
        TODO: use None instance to create update statements
        """
        if instance:
            return instance._values[self.column.column_name].setval(value)
        else:
            raise AttributeError('cannot reassign column values')

    def __delete__(self, instance):
        """
        Sets the column value to None, if possible
        """
        if instance:
            if self.column.can_delete:
                instance._values[self.column.column_name].delval()
            else:
                raise AttributeError('cannot delete {0} columns'.format(self.column.column_name))


class BaseModel(object):
    """
    The base model class, don't inherit from this, inherit from Model, defined below
    """

    class DoesNotExist(_DoesNotExist):
        pass

    class MultipleObjectsReturned(_MultipleObjectsReturned):
        pass

    objects = QuerySetDescriptor()
    ttl = TTLDescriptor()
    consistency = ConsistencyDescriptor()
    iff = ConditionalDescriptor()

    # custom timestamps, see USING TIMESTAMP X
    timestamp = TimestampDescriptor()

    if_not_exists = IfNotExistsDescriptor()

    if_exists = IfExistsDescriptor()

    using = UsingDescriptor()

    # _len is lazily created by __len__

    __table_name__ = None

    __table_name_case_sensitive__ = False

    __keyspace__ = None

    __connection__ = None

    __discriminator_value__ = None

    __options__ = None

    __compute_routing_key__ = True

    # the queryset class used for this class
    __queryset__ = query.ModelQuerySet
    __dmlquery__ = query.DMLQuery

    __consistency__ = None  # can be set per query

    _timestamp = None  # optional timestamp to include with the operation (USING TIMESTAMP)

    _if_not_exists = False  # optional if_not_exists flag to check existence before insertion

    _if_exists = False  # optional if_exists flag to check existence before update

    _table_name = None  # used internally to cache a derived table name

    _connection = None

    def __init__(self, **values):
        self._ttl = None
        self._timestamp = None
        self._conditional = None
        self._batch = None
        self._timeout = connection.NOT_SET
        self._is_persisted = False
        self._connection = None

        self._values = {}
        for name, column in self._columns.items():
            # Set default values on instantiation. Thanks to this, we don't have
            # to wait any longer for a call to validate() to have CQLengine set
            # default columns values.
            column_default = column.get_default() if column.has_default else None
            value = values.get(name, column_default)
            if value is not None or isinstance(column, columns.BaseContainerColumn):
                value = column.to_python(value)
            value_mngr = column.value_manager(self, column, value)
            value_mngr.explicit = name in values
            self._values[name] = value_mngr

    def __repr__(self):
        return '{0}({1})'.format(self.__class__.__name__,
                               ', '.join('{0}={1!r}'.format(k, getattr(self, k))
                                         for k in self._defined_columns.keys()
                                         if k != self._discriminator_column_name))

    def __str__(self):
        """
        Pretty printing of models by their primary key
        """
        return '{0} <{1}>'.format(self.__class__.__name__,
                                ', '.join('{0}={1}'.format(k, getattr(self, k)) for k in self._primary_keys.keys()))

    @classmethod
    def _routing_key_from_values(cls, pk_values, protocol_version):
        return cls._key_serializer(pk_values, protocol_version)

    @classmethod
    def _discover_polymorphic_submodels(cls):
        if not cls._is_polymorphic_base:
            raise ModelException('_discover_polymorphic_submodels can only be called on polymorphic base classes')

        def _discover(klass):
            if not klass._is_polymorphic_base and klass.__discriminator_value__ is not None:
                cls._discriminator_map[klass.__discriminator_value__] = klass
            for subklass in klass.__subclasses__():
                _discover(subklass)
        _discover(cls)

    @classmethod
    def _get_model_by_discriminator_value(cls, key):
        if not cls._is_polymorphic_base:
            raise ModelException('_get_model_by_discriminator_value can only be called on polymorphic base classes')
        return cls._discriminator_map.get(key)

    @classmethod
    def _construct_instance(cls, values):
        """
        method used to construct instances from query results
        this is where polymorphic deserialization occurs
        """
        # we're going to take the values, which is from the DB as a dict
        # and translate that into our local fields
        # the db_map is a db_field -> model field map
        if cls._db_map:
            values = dict((cls._db_map.get(k, k), v) for k, v in values.items())

        if cls._is_polymorphic:
            disc_key = values.get(cls._discriminator_column_name)

            if disc_key is None:
                raise PolymorphicModelException('discriminator value was not found in values')

            poly_base = cls if cls._is_polymorphic_base else cls._polymorphic_base

            klass = poly_base._get_model_by_discriminator_value(disc_key)
            if klass is None:
                poly_base._discover_polymorphic_submodels()
                klass = poly_base._get_model_by_discriminator_value(disc_key)
                if klass is None:
                    raise PolymorphicModelException(
                        'unrecognized discriminator column {0} for class {1}'.format(disc_key, poly_base.__name__)
                    )

            if not issubclass(klass, cls):
                raise PolymorphicModelException(
                    '{0} is not a subclass of {1}'.format(klass.__name__, cls.__name__)
                )

            values = dict((k, v) for k, v in values.items() if k in klass._columns.keys())

        else:
            klass = cls

        instance = klass(**values)
        instance._set_persisted(force=True)
        return instance

    def _set_persisted(self, force=False):
        # ensure we don't modify to any values not affected by the last save/update
        for v in [v for v in self._values.values() if v.changed or force]:
            v.reset_previous_value()
            v.explicit = False
        self._is_persisted = True

    def _can_update(self):
        """
        Called by the save function to check if this should be
        persisted with update or insert

        :return:
        """
        if not self._is_persisted:
            return False

        return all([not self._values[k].changed for k in self._primary_keys])

    @classmethod
    def _get_keyspace(cls):
        """
        Returns the manual keyspace, if set, otherwise the default keyspace
        """
        return cls.__keyspace__ or DEFAULT_KEYSPACE

    @classmethod
    def _get_column(cls, name):
        """
        Returns the column matching the given name, raising a key error if
        it doesn't exist

        :param name: the name of the column to return
        :rtype: Column
        """
        return cls._columns[name]

    @classmethod
    def _get_column_by_db_name(cls, name):
        """
        Returns the column, mapped by db_field name
        """
        return cls._columns.get(cls._db_map.get(name, name))

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False

        # check attribute keys
        keys = set(self._columns.keys())
        other_keys = set(other._columns.keys())
        if keys != other_keys:
            return False

        return all(getattr(self, key, None) == getattr(other, key, None) for key in other_keys)

    def __ne__(self, other):
        return not self.__eq__(other)

    @classmethod
    def column_family_name(cls, include_keyspace=True):
        """
        Returns the column family name if it's been defined
        otherwise, it creates it from the module and class name
        """
        cf_name = protect_name(cls._raw_column_family_name())
        if include_keyspace:
            keyspace = cls._get_keyspace()
            if not keyspace:
                raise CQLEngineException("Model keyspace is not set and no default is available. Set model keyspace or setup connection before attempting to generate a query.")
            return '{0}.{1}'.format(protect_name(keyspace), cf_name)

        return cf_name


    @classmethod
    def _raw_column_family_name(cls):
        if not cls._table_name:
            if cls.__table_name__:
                if cls.__table_name_case_sensitive__:
                    warn("Model __table_name_case_sensitive__ will be removed in 4.0.", PendingDeprecationWarning)
                    cls._table_name = cls.__table_name__
                else:
                    table_name = cls.__table_name__.lower()
                    if cls.__table_name__ != table_name:
                        warn(("Model __table_name__ will be case sensitive by default in 4.0. "
                        "You should fix the __table_name__ value of the '{0}' model.").format(cls.__name__))
                    cls._table_name = table_name
            else:
                if cls._is_polymorphic and not cls._is_polymorphic_base:
                    cls._table_name = cls._polymorphic_base._raw_column_family_name()
                else:
                    camelcase = re.compile(r'([a-z])([A-Z])')
                    ccase = lambda s: camelcase.sub(lambda v: '{0}_{1}'.format(v.group(1), v.group(2).lower()), s)

                    cf_name = ccase(cls.__name__)
                    # trim to less than 48 characters or cassandra will complain
                    cf_name = cf_name[-48:]
                    cf_name = cf_name.lower()
                    cf_name = re.sub(r'^_+', '', cf_name)
                    cls._table_name = cf_name

        return cls._table_name

    def _set_column_value(self, name, value):
        """Function to change a column value without changing the value manager states"""
        self._values[name].value = value  # internal assignement, skip the main setter

    def validate(self):
        """
        Cleans and validates the field values
        """
        for name, col in self._columns.items():
            v = getattr(self, name)
            if v is None and not self._values[name].explicit and col.has_default:
                v = col.get_default()
            val = col.validate(v)
            self._set_column_value(name, val)

    # Let an instance be used like a dict of its columns keys/values
    def __iter__(self):
        """ Iterate over column ids. """
        for column_id in self._columns.keys():
            yield column_id

    def __getitem__(self, key):
        """ Returns column's value. """
        if not isinstance(key, str):
            raise TypeError
        if key not in self._columns.keys():
            raise KeyError
        return getattr(self, key)

    def __setitem__(self, key, val):
        """ Sets a column's value. """
        if not isinstance(key, str):
            raise TypeError
        if key not in self._columns.keys():
            raise KeyError
        return setattr(self, key, val)

    def __len__(self):
        """
        Returns the number of columns defined on that model.
        """
        try:
            return self._len
        except:
            self._len = len(self._columns.keys())
            return self._len

    def keys(self):
        """ Returns a list of column IDs. """
        return [k for k in self]

    def values(self):
        """ Returns list of column values. """
        return [self[k] for k in self]

    def items(self):
        """ Returns a list of column ID/value tuples. """
        return [(k, self[k]) for k in self]

    def _as_dict(self):
        """ Returns a map of column names to cleaned values """
        values = self._dynamic_columns or {}
        for name, col in self._columns.items():
            values[name] = col.to_database(getattr(self, name, None))
        return values

    @classmethod
    def create(cls, **kwargs):
        """
        Create an instance of this model in the database.

        Takes the model column values as keyword arguments. Setting a value to
        `None` is equivalent to running a CQL `DELETE` on that column.

        Returns the instance.
        """
        extra_columns = set(kwargs.keys()) - set(cls._columns.keys())
        if extra_columns:
            raise ValidationError("Incorrect columns passed: {0}".format(extra_columns))
        return cls.objects.create(**kwargs)

    @classmethod
    def all(cls):
        """
        Returns a queryset representing all stored objects

        This is a pass-through to the model objects().all()
        """
        return cls.objects.all()

    @classmethod
    def filter(cls, *args, **kwargs):
        """
        Returns a queryset based on filter parameters.

        This is a pass-through to the model objects().:method:`~cqlengine.queries.filter`.
        """
        return cls.objects.filter(*args, **kwargs)

    @classmethod
    def get(cls, *args, **kwargs):
        """
        Returns a single object based on the passed filter constraints.

        This is a pass-through to the model objects().:method:`~cqlengine.queries.get`.
        """
        return cls.objects.get(*args, **kwargs)

    def timeout(self, timeout):
        """
        Sets a timeout for use in :meth:`~.save`, :meth:`~.update`, and :meth:`~.delete`
        operations
        """
        assert self._batch is None, 'Setting both timeout and batch is not supported'
        self._timeout = timeout
        return self

    def save(self):
        """
        Saves an object to the database.

        .. code-block:: python

            #create a person instance
            person = Person(first_name='Kimberly', last_name='Eggleston')
            #saves it to Cassandra
            person.save()
        """

        # handle polymorphic models
        if self._is_polymorphic:
            if self._is_polymorphic_base:
                raise PolymorphicModelException('cannot save polymorphic base model')
            else:
                setattr(self, self._discriminator_column_name, self.__discriminator_value__)

        self.validate()
        self.__dmlquery__(self.__class__, self,
                          batch=self._batch,
                          ttl=self._ttl,
                          timestamp=self._timestamp,
                          consistency=self.__consistency__,
                          if_not_exists=self._if_not_exists,
                          conditional=self._conditional,
                          timeout=self._timeout,
                          if_exists=self._if_exists).save()

        self._set_persisted()

        self._timestamp = None

        return self

    def update(self, **values):
        """
        Performs an update on the model instance. You can pass in values to set on the model
        for updating, or you can call without values to execute an update against any modified
        fields. If no fields on the model have been modified since loading, no query will be
        performed. Model validation is performed normally. Setting a value to `None` is
        equivalent to running a CQL `DELETE` on that column.

        It is possible to do a blind update, that is, to update a field without having first selected the object out of the database.
        See :ref:`Blind Updates <blind_updates>`
        """
        for column_id, v in values.items():
            col = self._columns.get(column_id)

            # check for nonexistant columns
            if col is None:
                raise ValidationError(
                    "{0}.{1} has no column named: {2}".format(
                        self.__module__, self.__class__.__name__, column_id))

            # check for primary key update attempts
            if col.is_primary_key:
                current_value = getattr(self, column_id)
                if v != current_value:
                    raise ValidationError(
                        "Cannot apply update to primary key '{0}' for {1}.{2}".format(
                            column_id, self.__module__, self.__class__.__name__))

            setattr(self, column_id, v)

        # handle polymorphic models
        if self._is_polymorphic:
            if self._is_polymorphic_base:
                raise PolymorphicModelException('cannot update polymorphic base model')
            else:
                setattr(self, self._discriminator_column_name, self.__discriminator_value__)

        self.validate()
        self.__dmlquery__(self.__class__, self,
                          batch=self._batch,
                          ttl=self._ttl,
                          timestamp=self._timestamp,
                          consistency=self.__consistency__,
                          conditional=self._conditional,
                          timeout=self._timeout,
                          if_exists=self._if_exists).update()

        self._set_persisted()

        self._timestamp = None

        return self

    def delete(self):
        """
        Deletes the object from the database
        """
        self.__dmlquery__(self.__class__, self,
                          batch=self._batch,
                          timestamp=self._timestamp,
                          consistency=self.__consistency__,
                          timeout=self._timeout,
                          conditional=self._conditional,
                          if_exists=self._if_exists).delete()

    def get_changed_columns(self):
        """
        Returns a list of the columns that have been updated since instantiation or save
        """
        return [k for k, v in self._values.items() if v.changed]

    @classmethod
    def _class_batch(cls, batch):
        return cls.objects.batch(batch)

    def _inst_batch(self, batch):
        assert self._timeout is connection.NOT_SET, 'Setting both timeout and batch is not supported'
        if self._connection:
            raise CQLEngineException("Cannot specify a connection on model in batch mode.")
        self._batch = batch
        return self

    batch = hybrid_classmethod(_class_batch, _inst_batch)

    @classmethod
    def _class_get_connection(cls):
        return cls.__connection__

    def _inst_get_connection(self):
        return self._connection or self.__connection__

    _get_connection = hybrid_classmethod(_class_get_connection, _inst_get_connection)


class ModelMetaClass(type):

    def __new__(cls, name, bases, attrs):
        # move column definitions into columns dict
        # and set default column names
        column_dict = OrderedDict()
        primary_keys = OrderedDict()
        pk_name = None

        # get inherited properties
        inherited_columns = OrderedDict()
        for base in bases:
            for k, v in getattr(base, '_defined_columns', {}).items():
                inherited_columns.setdefault(k, v)

        # short circuit __abstract__ inheritance
        is_abstract = attrs['__abstract__'] = attrs.get('__abstract__', False)

        # short circuit __discriminator_value__ inheritance
        attrs['__discriminator_value__'] = attrs.get('__discriminator_value__')

        # TODO __default__ttl__ should be removed in the next major release
        options = attrs.get('__options__') or {}
        attrs['__default_ttl__'] = options.get('default_time_to_live')

        column_definitions = [(k, v) for k, v in attrs.items() if isinstance(v, columns.Column)]
        column_definitions = sorted(column_definitions, key=lambda x: x[1].position)

        is_polymorphic_base = any([c[1].discriminator_column for c in column_definitions])

        column_definitions = [x for x in inherited_columns.items()] + column_definitions
        discriminator_columns = [c for c in column_definitions if c[1].discriminator_column]
        is_polymorphic = len(discriminator_columns) > 0
        if len(discriminator_columns) > 1:
            raise ModelDefinitionException('only one discriminator_column can be defined in a model, {0} found'.format(len(discriminator_columns)))

        if attrs['__discriminator_value__'] and not is_polymorphic:
            raise ModelDefinitionException('__discriminator_value__ specified, but no base columns defined with discriminator_column=True')

        discriminator_column_name, discriminator_column = discriminator_columns[0] if discriminator_columns else (None, None)

        if isinstance(discriminator_column, (columns.BaseContainerColumn, columns.Counter)):
            raise ModelDefinitionException('counter and container columns cannot be used as discriminator columns')

        # find polymorphic base class
        polymorphic_base = None
        if is_polymorphic and not is_polymorphic_base:
            def _get_polymorphic_base(bases):
                for base in bases:
                    if getattr(base, '_is_polymorphic_base', False):
                        return base
                    klass = _get_polymorphic_base(base.__bases__)
                    if klass:
                        return klass
            polymorphic_base = _get_polymorphic_base(bases)

        defined_columns = OrderedDict(column_definitions)

        # check for primary key
        if not is_abstract and not any([v.primary_key for k, v in column_definitions]):
            raise ModelDefinitionException("At least 1 primary key is required.")

        counter_columns = [c for c in defined_columns.values() if isinstance(c, columns.Counter)]
        data_columns = [c for c in defined_columns.values() if not c.primary_key and not isinstance(c, columns.Counter)]
        if counter_columns and data_columns:
            raise ModelDefinitionException('counter models may not have data columns')

        has_partition_keys = any(v.partition_key for (k, v) in column_definitions)

        def _transform_column(col_name, col_obj):
            column_dict[col_name] = col_obj
            if col_obj.primary_key:
                primary_keys[col_name] = col_obj
            col_obj.set_column_name(col_name)
            # set properties
            attrs[col_name] = ColumnDescriptor(col_obj)

        partition_key_index = 0
        # transform column definitions
        for k, v in column_definitions:
            # don't allow a column with the same name as a built-in attribute or method
            if k in BaseModel.__dict__:
                raise ModelDefinitionException("column '{0}' conflicts with built-in attribute/method".format(k))

            # counter column primary keys are not allowed
            if (v.primary_key or v.partition_key) and isinstance(v, columns.Counter):
                raise ModelDefinitionException('counter columns cannot be used as primary keys')

            # this will mark the first primary key column as a partition
            # key, if one hasn't been set already
            if not has_partition_keys and v.primary_key:
                v.partition_key = True
                has_partition_keys = True
            if v.partition_key:
                v._partition_key_index = partition_key_index
                partition_key_index += 1

            overriding = column_dict.get(k)
            if overriding:
                v.position = overriding.position
                v.partition_key = overriding.partition_key
                v._partition_key_index = overriding._partition_key_index
            _transform_column(k, v)

        partition_keys = OrderedDict(k for k in primary_keys.items() if k[1].partition_key)
        clustering_keys = OrderedDict(k for k in primary_keys.items() if not k[1].partition_key)

        if attrs.get('__compute_routing_key__', True):
            key_cols = [c for c in partition_keys.values()]
            partition_key_index = dict((col.db_field_name, col._partition_key_index) for col in key_cols)
            key_cql_types = [c.cql_type for c in key_cols]
            key_serializer = staticmethod(lambda parts, proto_version: [t.to_binary(p, proto_version) for t, p in zip(key_cql_types, parts)])
        else:
            partition_key_index = {}
            key_serializer = staticmethod(lambda parts, proto_version: None)

        # setup partition key shortcut
        if len(partition_keys) == 0:
            if not is_abstract:
                raise ModelException("at least one partition key must be defined")
        if len(partition_keys) == 1:
            pk_name = [x for x in partition_keys.keys()][0]
            attrs['pk'] = attrs[pk_name]
        else:
            # composite partition key case, get/set a tuple of values
            _get = lambda self: tuple(self._values[c].getval() for c in partition_keys.keys())
            _set = lambda self, val: tuple(self._values[c].setval(v) for (c, v) in zip(partition_keys.keys(), val))
            attrs['pk'] = property(_get, _set)

        # some validation
        col_names = set()
        for v in column_dict.values():
            # check for duplicate column names
            if v.db_field_name in col_names:
                raise ModelException("{0} defines the column '{1}' more than once".format(name, v.db_field_name))
            if v.clustering_order and not (v.primary_key and not v.partition_key):
                raise ModelException("clustering_order may be specified only for clustering primary keys")
            if v.clustering_order and v.clustering_order.lower() not in ('asc', 'desc'):
                raise ModelException("invalid clustering order '{0}' for column '{1}'".format(repr(v.clustering_order), v.db_field_name))
            col_names.add(v.db_field_name)

        # create db_name -> model name map for loading
        db_map = {}
        for col_name, field in column_dict.items():
            db_field = field.db_field_name
            if db_field != col_name:
                db_map[db_field] = col_name

        # add management members to the class
        attrs['_columns'] = column_dict
        attrs['_primary_keys'] = primary_keys
        attrs['_defined_columns'] = defined_columns

        # maps the database field to the models key
        attrs['_db_map'] = db_map
        attrs['_pk_name'] = pk_name
        attrs['_dynamic_columns'] = {}

        attrs['_partition_keys'] = partition_keys
        attrs['_partition_key_index'] = partition_key_index
        attrs['_key_serializer'] = key_serializer
        attrs['_clustering_keys'] = clustering_keys
        attrs['_has_counter'] = len(counter_columns) > 0

        # add polymorphic management attributes
        attrs['_is_polymorphic_base'] = is_polymorphic_base
        attrs['_is_polymorphic'] = is_polymorphic
        attrs['_polymorphic_base'] = polymorphic_base
        attrs['_discriminator_column'] = discriminator_column
        attrs['_discriminator_column_name'] = discriminator_column_name
        attrs['_discriminator_map'] = {} if is_polymorphic_base else None

        # setup class exceptions
        DoesNotExistBase = None
        for base in bases:
            DoesNotExistBase = getattr(base, 'DoesNotExist', None)
            if DoesNotExistBase is not None:
                break

        DoesNotExistBase = DoesNotExistBase or attrs.pop('DoesNotExist', BaseModel.DoesNotExist)
        attrs['DoesNotExist'] = type('DoesNotExist', (DoesNotExistBase,), {})

        MultipleObjectsReturnedBase = None
        for base in bases:
            MultipleObjectsReturnedBase = getattr(base, 'MultipleObjectsReturned', None)
            if MultipleObjectsReturnedBase is not None:
                break

        MultipleObjectsReturnedBase = MultipleObjectsReturnedBase or attrs.pop('MultipleObjectsReturned', BaseModel.MultipleObjectsReturned)
        attrs['MultipleObjectsReturned'] = type('MultipleObjectsReturned', (MultipleObjectsReturnedBase,), {})

        # create the class and add a QuerySet to it
        klass = super(ModelMetaClass, cls).__new__(cls, name, bases, attrs)

        udts = []
        for col in column_dict.values():
            columns.resolve_udts(col, udts)

        for user_type in set(udts):
            user_type.register_for_keyspace(klass._get_keyspace())

        return klass


class Model(BaseModel, metaclass=ModelMetaClass):
    __abstract__ = True
    """
    *Optional.* Indicates that this model is only intended to be used as a base class for other models.
    You can't create tables for abstract models, but checks around schema validity are skipped during class construction.
    """

    __table_name__ = None
    """
    *Optional.* Sets the name of the CQL table for this model. If left blank, the table name will be the name of the model, with it's module name as it's prefix. Manually defined table names are not inherited.
    """

    __table_name_case_sensitive__ = False
    """
    *Optional.* By default, __table_name__ is case insensitive. Set this to True if you want to preserve the case sensitivity.
    """

    __keyspace__ = None
    """
    Sets the name of the keyspace used by this model.
    """

    __connection__ = None
    """
    Sets the name of the default connection used by this model.
    """

    __options__ = None
    """
    *Optional* Table options applied with this model

    (e.g. compaction, default ttl, cache settings, tec.)
    """

    __discriminator_value__ = None
    """
    *Optional* Specifies a value for the discriminator column when using model inheritance.
    """

    __compute_routing_key__ = True
    """
    *Optional* Setting False disables computing the routing key for TokenAwareRouting
    """
