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

import re

from cassandra.util import OrderedDict
from cassandra.cqlengine import CQLEngineException
from cassandra.cqlengine import columns
from cassandra.cqlengine import connection as conn
from cassandra.cqlengine import models


class UserTypeException(CQLEngineException):
    pass


class UserTypeDefinitionException(UserTypeException):
    pass


class BaseUserType(object):
    """
    The base type class; don't inherit from this, inherit from UserType, defined below
    """
    __type_name__ = None

    _fields = None
    _db_map = None

    def __init__(self, **values):
        self._values = {}
        if self._db_map:
            values = dict((self._db_map.get(k, k), v) for k, v in values.items())

        for name, field in self._fields.items():
            field_default = field.get_default() if field.has_default else None
            value = values.get(name, field_default)
            if value is not None or isinstance(field, columns.BaseContainerColumn):
                value = field.to_python(value)
            value_mngr = field.value_manager(self, field, value)
            value_mngr.explicit = name in values
            self._values[name] = value_mngr

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False

        keys = set(self._fields.keys())
        other_keys = set(other._fields.keys())
        if keys != other_keys:
            return False

        for key in other_keys:
            if getattr(self, key, None) != getattr(other, key, None):
                return False

        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return "{{{0}}}".format(', '.join("'{0}': {1}".format(k, getattr(self, k)) for k, v in self._values.items()))

    def has_changed_fields(self):
        return any(v.changed for v in self._values.values())

    def reset_changed_fields(self):
        for v in self._values.values():
            v.reset_previous_value()

    def __iter__(self):
        for field in self._fields.keys():
            yield field

    def __getattr__(self, attr):
        # provides the mapping from db_field to fields
        try:
            return getattr(self, self._db_map[attr])
        except KeyError:
            raise AttributeError(attr)

    def __getitem__(self, key):
        if not isinstance(key, str):
            raise TypeError
        if key not in self._fields.keys():
            raise KeyError
        return getattr(self, key)

    def __setitem__(self, key, val):
        if not isinstance(key, str):
            raise TypeError
        if key not in self._fields.keys():
            raise KeyError
        return setattr(self, key, val)

    def __len__(self):
        try:
            return self._len
        except:
            self._len = len(self._fields.keys())
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

    @classmethod
    def register_for_keyspace(cls, keyspace, connection=None):
        conn.register_udt(keyspace, cls.type_name(), cls, connection=connection)

    @classmethod
    def type_name(cls):
        """
        Returns the type name if it's been defined
        otherwise, it creates it from the class name
        """
        if cls.__type_name__:
            type_name = cls.__type_name__.lower()
        else:
            camelcase = re.compile(r'([a-z])([A-Z])')
            ccase = lambda s: camelcase.sub(lambda v: '{0}_{1}'.format(v.group(1), v.group(2)), s)

            type_name = ccase(cls.__name__)
            # trim to less than 48 characters or cassandra will complain
            type_name = type_name[-48:]
            type_name = type_name.lower()
            type_name = re.sub(r'^_+', '', type_name)
            cls.__type_name__ = type_name

        return type_name

    def validate(self):
        """
        Cleans and validates the field values
        """
        for name, field in self._fields.items():
            v = getattr(self, name)
            if v is None and not self._values[name].explicit and field.has_default:
                v = field.get_default()
            val = field.validate(v)
            setattr(self, name, val)


class UserTypeMetaClass(type):

    def __new__(cls, name, bases, attrs):
        field_dict = OrderedDict()

        field_defs = [(k, v) for k, v in attrs.items() if isinstance(v, columns.Column)]
        field_defs = sorted(field_defs, key=lambda x: x[1].position)

        def _transform_column(field_name, field_obj):
            field_dict[field_name] = field_obj
            field_obj.set_column_name(field_name)
            attrs[field_name] = models.ColumnDescriptor(field_obj)

        # transform field definitions
        for k, v in field_defs:
            # don't allow a field with the same name as a built-in attribute or method
            if k in BaseUserType.__dict__:
                raise UserTypeDefinitionException("field '{0}' conflicts with built-in attribute/method".format(k))
            _transform_column(k, v)

        attrs['_fields'] = field_dict

        db_map = {}
        for field_name, field in field_dict.items():
            db_field = field.db_field_name
            if db_field != field_name:
                if db_field in field_dict:
                    raise UserTypeDefinitionException("db_field '{0}' for field '{1}' conflicts with another attribute name".format(db_field, field_name))
                db_map[db_field] = field_name
        attrs['_db_map'] = db_map

        klass = super(UserTypeMetaClass, cls).__new__(cls, name, bases, attrs)

        return klass


class UserType(BaseUserType, metaclass=UserTypeMetaClass):
    """
    This class is used to model User Defined Types. To define a type, declare a class inheriting from this,
    and assign field types as class attributes:

    .. code-block:: python

        # connect with default keyspace ...

        from cassandra.cqlengine.columns import Text, Integer
        from cassandra.cqlengine.usertype import UserType

        class address(UserType):
            street = Text()
            zipcode = Integer()

        from cassandra.cqlengine import management
        management.sync_type(address)

    Please see :ref:`user_types` for a complete example and discussion.
    """

    __type_name__ = None
    """
    *Optional.* Sets the name of the CQL type for this type.

    If not specified, the type name will be the name of the class, with it's module name as it's prefix.
    """
