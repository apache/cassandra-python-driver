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
"""
These functions are used to convert Python objects into CQL strings.
When non-prepared statements are executed, these encoder functions are
called on each query parameter.
"""

import logging

from binascii import hexlify
from decimal import Decimal
import calendar
import datetime
import math
import sys
import types
from uuid import UUID
import ipaddress

from cassandra.util import (OrderedDict, OrderedMap, OrderedMapSerializedKey,
                            sortedset, Time, Date, Point, LineString, Polygon)

log = logging.getLogger(__name__)


def cql_quote(term):
    if isinstance(term, str):
        return "'%s'" % str(term).replace("'", "''")
    else:
        return str(term)


class ValueSequence(list):
    pass


class Encoder(object):
    """
    A container for mapping python types to CQL string literals when working
    with non-prepared statements.  The type :attr:`~.Encoder.mapping` can be
    directly customized by users.
    """

    mapping = None
    """
    A list of tuples mapping python types to encoder functions.
    """

    def __init__(self):
        self.mapping = [
            (ValueSequence, self.cql_encode_sequence),
            (Point, self.cql_encode_str_quoted),
            (LineString, self.cql_encode_str_quoted),
            (Polygon, self.cql_encode_str_quoted),
            (Decimal, self.cql_encode_decimal),
            (UUID, self.cql_encode_object),
            (Date, self.cql_encode_date_ext),
            (Time, self.cql_encode_time),
            (ipaddress.IPv4Address, self.cql_encode_ipaddress),
            (ipaddress.IPv6Address, self.cql_encode_ipaddress),
            (float, self.cql_encode_float),
            (bytearray, self.cql_encode_bytes),
            (str, self.cql_encode_str),
            (int, self.cql_encode_object),
            (datetime.datetime, self.cql_encode_datetime),
            (datetime.date, self.cql_encode_date),
            (datetime.time, self.cql_encode_time),
            (dict, self.cql_encode_map_collection),
            (OrderedDict, self.cql_encode_map_collection),
            (OrderedMap, self.cql_encode_map_collection),
            (OrderedMapSerializedKey, self.cql_encode_map_collection),
            (list, self.cql_encode_list_collection),
            (tuple, self.cql_encode_list_collection),  # TODO: change to tuple in next major
            (set, self.cql_encode_set_collection),
            (sortedset, self.cql_encode_set_collection),
            (frozenset, self.cql_encode_set_collection),
            (types.GeneratorType, self.cql_encode_list_collection),
            (memoryview, self.cql_encode_bytes),
            (bytes, self.cql_encode_bytes),
            (type(None), self.cql_encode_none),
         ]

    def get_encode_function(self, need_data_type):
        """
        Returns the appropriate encoder function for a given data type.
        If the data type is a subclass of a registered type, the registered
        type's encoder is returned. This respects the order in :attr:`~.Encoder.mapping`.
        """
        for data_type, encode_function in self.mapping:
            if issubclass(need_data_type, data_type):
                return encode_function
        raise KeyError('No encode function found for type: %s', need_data_type)

    def add_or_override_mapping(self, new_data_type, new_encode_function):
        """
        Adds a new mapping to the encoder or overrides an existing one.
        If the new type subclasses an existing one ensures that it will be
        evaluated before the existing one.
        """
        for i, (data_type, _ ) in enumerate(self.mapping):
            if new_data_type == data_type:
                self.mapping[i] = (new_data_type, new_encode_function)
                return
            if issubclass(new_data_type, data_type):
                self.mapping.insert(i, (new_data_type, new_encode_function))
                return

        self.mapping.append((new_data_type, new_encode_function))

    def cql_encode_none(self, val):
        """
        Converts :const:`None` to the string 'NULL'.
        """
        return 'NULL'

    def cql_encode_unicode(self, val):
        """
        Converts :class:`unicode` objects to UTF-8 encoded strings with quote escaping.
        """
        return cql_quote(val.encode('utf-8'))

    def cql_encode_str(self, val):
        """
        Escapes quotes in :class:`str` objects.
        """
        return cql_quote(val)

    def cql_encode_str_quoted(self, val):
        return "'%s'" % val

    def cql_encode_bytes(self, val):
        return (b'0x' + hexlify(val)).decode('utf-8')

    def cql_encode_object(self, val):
        """
        Default encoder for all objects that do not have a specific encoder function
        registered. This function simply calls :meth:`str()` on the object.
        """
        return str(val)

    def cql_encode_float(self, val):
        """
        Encode floats using repr to preserve precision
        """
        if math.isinf(val):
            return 'Infinity' if val > 0 else '-Infinity'
        elif math.isnan(val):
            return 'NaN'
        else:
            return repr(val)

    def cql_encode_datetime(self, val):
        """
        Converts a :class:`datetime.datetime` object to a (string) integer timestamp
        with millisecond precision.
        """
        timestamp = calendar.timegm(val.utctimetuple())
        return str(int(timestamp * 1e3 + getattr(val, 'microsecond', 0) / 1e3))

    def cql_encode_date(self, val):
        """
        Converts a :class:`datetime.date` object to a string with format
        ``YYYY-MM-DD``.
        """
        return "'%s'" % val.strftime('%Y-%m-%d')

    def cql_encode_time(self, val):
        """
        Converts a :class:`cassandra.util.Time` object to a string with format
        ``HH:MM:SS.mmmuuunnn``.
        """
        return "'%s'" % val

    def cql_encode_date_ext(self, val):
        """
        Encodes a :class:`cassandra.util.Date` object as an integer
        """
        # using the int form in case the Date exceeds datetime.[MIN|MAX]YEAR
        return str(val.days_from_epoch + 2 ** 31)

    def cql_encode_sequence(self, val):
        """
        Converts a sequence to a string of the form ``(item1, item2, ...)``.  This
        is suitable for ``IN`` value lists.
        """
        return '(%s)' % ', '.join(self.cql_encode_all_types(v) for v in val)

    cql_encode_tuple = cql_encode_sequence
    """
    Converts a sequence to a string of the form ``(item1, item2, ...)``.  This
    is suitable for ``tuple`` type columns.
    """

    def cql_encode_map_collection(self, val):
        """
        Converts a dict into a string of the form ``{key1: val1, key2: val2, ...}``.
        This is suitable for ``map`` type columns.
        """
        return '{%s}' % ', '.join(
            '%s: %s' % (self.cql_encode_all_types(k), self.cql_encode_all_types(v))
            for k, v in val.items()
        )

    def cql_encode_list_collection(self, val):
        """
        Converts a sequence to a string of the form ``[item1, item2, ...]``.  This
        is suitable for ``list`` type columns.
        """
        return '[%s]' % ', '.join(self.cql_encode_all_types(v) for v in val)

    def cql_encode_set_collection(self, val):
        """
        Converts a sequence to a string of the form ``{item1, item2, ...}``.  This
        is suitable for ``set`` type columns.
        """
        return '{%s}' % ', '.join(self.cql_encode_all_types(v) for v in val)

    def cql_encode_all_types(self, val, as_text_type=False):
        """
        Converts any type into a CQL string, defaulting to ``cql_encode_object``
        if :attr:`~.Encoder.mapping` does not contain an entry for the type.
        """
        for data_type, encode_function in self.mapping:
            if isinstance(val, data_type):
                encoded = encode_function(val)
                break
        else:
            encoded = self.cql_encode_object(val)
        if as_text_type and not isinstance(encoded, str):
            return encoded.decode('utf-8')
        return encoded

    def cql_encode_ipaddress(self, val):
        """
        Converts an ipaddress (IPV4Address, IPV6Address) to a CQL string. This
        is suitable for ``inet`` type columns.
        """
        return "'%s'" % val.compressed

    def cql_encode_decimal(self, val):
        return self.cql_encode_float(float(val))