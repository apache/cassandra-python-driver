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

import datetime
import base64
import uuid
import re
import json
from decimal import Decimal
from collections import OrderedDict
import logging
import itertools
from functools import partial

import ipaddress


from cassandra.cqltypes import cql_types_from_string
from cassandra.metadata import UserType
from cassandra.util import Polygon, Point, LineString, Duration
from cassandra.datastax.graph.types import Vertex, VertexProperty, Edge, Path, T

__all__ = ['GraphSON1Serializer', 'GraphSON1Deserializer', 'GraphSON1TypeDeserializer',
           'GraphSON2Serializer', 'GraphSON2Deserializer', 'GraphSON2Reader',
           'GraphSON3Serializer', 'GraphSON3Deserializer', 'GraphSON3Reader',
           'to_bigint', 'to_int', 'to_double', 'to_float', 'to_smallint',
           'BooleanTypeIO', 'Int16TypeIO', 'Int32TypeIO', 'DoubleTypeIO',
           'FloatTypeIO', 'UUIDTypeIO', 'BigDecimalTypeIO', 'DurationTypeIO', 'InetTypeIO',
           'InstantTypeIO', 'LocalDateTypeIO', 'LocalTimeTypeIO', 'Int64TypeIO', 'BigIntegerTypeIO',
           'LocalDateTypeIO', 'PolygonTypeIO', 'PointTypeIO', 'LineStringTypeIO', 'BlobTypeIO',
           'GraphSON3Serializer', 'GraphSON3Deserializer', 'UserTypeIO', 'TypeWrapperTypeIO']

"""
Supported types:

DSE Graph      GraphSON 2.0     GraphSON 3.0   |  Python Driver
------------ | -------------- | -------------- | ------------
text         | string         | string         | str
boolean      |                |                | bool
bigint       | g:Int64        | g:Int64        | long
int          | g:Int32        | g:Int32        | int
double       | g:Double       | g:Double       | float
float        | g:Float        | g:Float        | float
uuid         | g:UUID         | g:UUID         | UUID
bigdecimal   | gx:BigDecimal  | gx:BigDecimal  | Decimal
duration     | gx:Duration    | N/A            | timedelta              (Classic graph only)
DSE Duration | N/A            | dse:Duration   | Duration               (Core graph only)
inet         | gx:InetAddress | gx:InetAddress | str (unicode), IPV4Address/IPV6Address (PY3)
timestamp    | gx:Instant     | gx:Instant     | datetime.datetime
date         | gx:LocalDate   | gx:LocalDate   | datetime.date
time         | gx:LocalTime   | gx:LocalTime   | datetime.time
smallint     | gx:Int16       | gx:Int16       | int
varint       | gx:BigInteger  | gx:BigInteger  | long
date         | gx:LocalDate   | gx:LocalDate   | Date
polygon      | dse:Polygon    | dse:Polygon    | Polygon
point        | dse:Point      | dse:Point      | Point
linestring   | dse:Linestring | dse:LineString | LineString
blob         | dse:Blob       | dse:Blob       | bytearray, buffer (PY2), memoryview (PY3), bytes (PY3)
blob         | gx:ByteBuffer  | gx:ByteBuffer  | bytearray, buffer (PY2), memoryview (PY3), bytes (PY3)
list         | N/A            | g:List         | list                   (Core graph only)
map          | N/A            | g:Map          | dict                   (Core graph only)
set          | N/A            | g:Set          | set or list            (Core graph only)
                                                 Can return a list due to numerical values returned by Java
tuple        | N/A            | dse:Tuple      | tuple                  (Core graph only)
udt          | N/A            | dse:UDT        | class or namedtuple    (Core graph only)
"""

MAX_INT32 = 2 ** 32 - 1
MIN_INT32 = -2 ** 31

log = logging.getLogger(__name__)


class _GraphSONTypeType(type):
    """GraphSONType metaclass, required to create a class property."""

    @property
    def graphson_type(cls):
        return "{0}:{1}".format(cls.prefix, cls.graphson_base_type)


class GraphSONTypeIO(object, metaclass=_GraphSONTypeType):
    """Represent a serializable GraphSON type"""

    prefix = 'g'
    graphson_base_type = None
    cql_type = None

    @classmethod
    def definition(cls, value, writer=None):
        return {'cqlType': cls.cql_type}

    @classmethod
    def serialize(cls, value, writer=None):
        return str(value)

    @classmethod
    def deserialize(cls, value, reader=None):
        return value

    @classmethod
    def get_specialized_serializer(cls, value):
        return cls


class TextTypeIO(GraphSONTypeIO):
    cql_type = 'text'


class BooleanTypeIO(GraphSONTypeIO):
    graphson_base_type = None
    cql_type = 'boolean'

    @classmethod
    def serialize(cls, value, writer=None):
        return bool(value)


class IntegerTypeIO(GraphSONTypeIO):

    @classmethod
    def serialize(cls, value, writer=None):
        return value

    @classmethod
    def get_specialized_serializer(cls, value):
        if type(value) is int and (value > MAX_INT32 or value < MIN_INT32):
            return Int64TypeIO

        return Int32TypeIO


class Int16TypeIO(IntegerTypeIO):
    prefix = 'gx'
    graphson_base_type = 'Int16'
    cql_type = 'smallint'


class Int32TypeIO(IntegerTypeIO):
    graphson_base_type = 'Int32'
    cql_type = 'int'


class Int64TypeIO(IntegerTypeIO):
    graphson_base_type = 'Int64'
    cql_type = 'bigint'

    @classmethod
    def deserialize(cls, value, reader=None):
        return value


class FloatTypeIO(GraphSONTypeIO):
    graphson_base_type = 'Float'
    cql_type = 'float'

    @classmethod
    def serialize(cls, value, writer=None):
        return value

    @classmethod
    def deserialize(cls, value, reader=None):
        return float(value)


class DoubleTypeIO(FloatTypeIO):
    graphson_base_type = 'Double'
    cql_type = 'double'


class BigIntegerTypeIO(IntegerTypeIO):
    prefix = 'gx'
    graphson_base_type = 'BigInteger'


class LocalDateTypeIO(GraphSONTypeIO):
    FORMAT = '%Y-%m-%d'

    prefix = 'gx'
    graphson_base_type = 'LocalDate'
    cql_type = 'date'

    @classmethod
    def serialize(cls, value, writer=None):
        return value.isoformat()

    @classmethod
    def deserialize(cls, value, reader=None):
        try:
            return datetime.datetime.strptime(value, cls.FORMAT).date()
        except ValueError:
            # negative date
            return value


class InstantTypeIO(GraphSONTypeIO):
    prefix = 'gx'
    graphson_base_type = 'Instant'
    cql_type = 'timestamp'

    @classmethod
    def serialize(cls, value, writer=None):
        if isinstance(value, datetime.datetime):
            value = datetime.datetime(*value.utctimetuple()[:6]).replace(microsecond=value.microsecond)
        else:
            value = datetime.datetime.combine(value, datetime.datetime.min.time())

        return "{0}Z".format(value.isoformat())

    @classmethod
    def deserialize(cls, value, reader=None):
        try:
            d = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            d = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
        return d


class LocalTimeTypeIO(GraphSONTypeIO):
    FORMATS = [
        '%H:%M',
        '%H:%M:%S',
        '%H:%M:%S.%f'
    ]

    prefix = 'gx'
    graphson_base_type = 'LocalTime'
    cql_type = 'time'

    @classmethod
    def serialize(cls, value, writer=None):
        return value.strftime(cls.FORMATS[2])

    @classmethod
    def deserialize(cls, value, reader=None):
        dt = None
        for f in cls.FORMATS:
            try:
                dt = datetime.datetime.strptime(value, f)
                break
            except ValueError:
                continue

        if dt is None:
            raise ValueError('Unable to decode LocalTime: {0}'.format(value))

        return dt.time()


class BlobTypeIO(GraphSONTypeIO):
    prefix = 'dse'
    graphson_base_type = 'Blob'
    cql_type = 'blob'

    @classmethod
    def serialize(cls, value, writer=None):
        value = base64.b64encode(value)
        value = value.decode('utf-8')
        return value

    @classmethod
    def deserialize(cls, value, reader=None):
        return bytearray(base64.b64decode(value))


class ByteBufferTypeIO(BlobTypeIO):
    prefix = 'gx'
    graphson_base_type = 'ByteBuffer'


class UUIDTypeIO(GraphSONTypeIO):
    graphson_base_type = 'UUID'
    cql_type = 'uuid'

    @classmethod
    def deserialize(cls, value, reader=None):
        return uuid.UUID(value)


class BigDecimalTypeIO(GraphSONTypeIO):
    prefix = 'gx'
    graphson_base_type = 'BigDecimal'
    cql_type = 'bigdecimal'

    @classmethod
    def deserialize(cls, value, reader=None):
        return Decimal(value)


class DurationTypeIO(GraphSONTypeIO):
    prefix = 'gx'
    graphson_base_type = 'Duration'
    cql_type = 'duration'

    _duration_regex = re.compile(r"""
        ^P((?P<days>\d+)D)?
        T((?P<hours>\d+)H)?
        ((?P<minutes>\d+)M)?
        ((?P<seconds>[0-9.]+)S)?$
    """, re.VERBOSE)
    _duration_format = "P{days}DT{hours}H{minutes}M{seconds}S"

    _seconds_in_minute = 60
    _seconds_in_hour = 60 * _seconds_in_minute
    _seconds_in_day = 24 * _seconds_in_hour

    @classmethod
    def serialize(cls, value, writer=None):
        total_seconds = int(value.total_seconds())
        days, total_seconds = divmod(total_seconds, cls._seconds_in_day)
        hours, total_seconds = divmod(total_seconds, cls._seconds_in_hour)
        minutes, total_seconds = divmod(total_seconds, cls._seconds_in_minute)
        total_seconds += value.microseconds / 1e6

        return cls._duration_format.format(
            days=int(days), hours=int(hours), minutes=int(minutes), seconds=total_seconds
        )

    @classmethod
    def deserialize(cls, value, reader=None):
        duration = cls._duration_regex.match(value)
        if duration is None:
            raise ValueError('Invalid duration: {0}'.format(value))

        duration = {k: float(v) if v is not None else 0
                    for k, v in duration.groupdict().items()}
        return datetime.timedelta(days=duration['days'], hours=duration['hours'],
                                  minutes=duration['minutes'], seconds=duration['seconds'])


class DseDurationTypeIO(GraphSONTypeIO):
    prefix = 'dse'
    graphson_base_type = 'Duration'
    cql_type = 'duration'

    @classmethod
    def serialize(cls, value, writer=None):
        return {
            'months': value.months,
            'days': value.days,
            'nanos': value.nanoseconds
        }

    @classmethod
    def deserialize(cls, value, reader=None):
        return Duration(
            reader.deserialize(value['months']),
            reader.deserialize(value['days']),
            reader.deserialize(value['nanos'])
        )


class TypeWrapperTypeIO(GraphSONTypeIO):

    @classmethod
    def definition(cls, value, writer=None):
        return {'cqlType': value.type_io.cql_type}

    @classmethod
    def serialize(cls, value, writer=None):
        return value.type_io.serialize(value.value)

    @classmethod
    def deserialize(cls, value, reader=None):
        return value.type_io.deserialize(value.value)


class PointTypeIO(GraphSONTypeIO):
    prefix = 'dse'
    graphson_base_type = 'Point'
    cql_type = "org.apache.cassandra.db.marshal.PointType"

    @classmethod
    def deserialize(cls, value, reader=None):
        return Point.from_wkt(value)


class LineStringTypeIO(GraphSONTypeIO):
    prefix = 'dse'
    graphson_base_type = 'LineString'
    cql_type = "org.apache.cassandra.db.marshal.LineStringType"

    @classmethod
    def deserialize(cls, value, reader=None):
        return LineString.from_wkt(value)


class PolygonTypeIO(GraphSONTypeIO):
    prefix = 'dse'
    graphson_base_type = 'Polygon'
    cql_type = "org.apache.cassandra.db.marshal.PolygonType"

    @classmethod
    def deserialize(cls, value, reader=None):
        return Polygon.from_wkt(value)


class InetTypeIO(GraphSONTypeIO):
    prefix = 'gx'
    graphson_base_type = 'InetAddress'
    cql_type = 'inet'


class VertexTypeIO(GraphSONTypeIO):
    graphson_base_type = 'Vertex'

    @classmethod
    def deserialize(cls, value, reader=None):
        vertex = Vertex(id=reader.deserialize(value["id"]),
                        label=value["label"] if "label" in value else "vertex",
                        type='vertex',
                        properties={})
        # avoid the properties processing in Vertex.__init__
        vertex.properties = reader.deserialize(value.get('properties', {}))
        return vertex


class VertexPropertyTypeIO(GraphSONTypeIO):
    graphson_base_type = 'VertexProperty'

    @classmethod
    def deserialize(cls, value, reader=None):
        return VertexProperty(label=value['label'],
                              value=reader.deserialize(value["value"]),
                              properties=reader.deserialize(value.get('properties', {})))


class EdgeTypeIO(GraphSONTypeIO):
    graphson_base_type = 'Edge'

    @classmethod
    def deserialize(cls, value, reader=None):
        in_vertex = Vertex(id=reader.deserialize(value["inV"]),
                           label=value['inVLabel'],
                           type='vertex',
                           properties={})
        out_vertex = Vertex(id=reader.deserialize(value["outV"]),
                            label=value['outVLabel'],
                            type='vertex',
                            properties={})
        return Edge(
            id=reader.deserialize(value["id"]),
            label=value["label"] if "label" in value else "vertex",
            type='edge',
            properties=reader.deserialize(value.get("properties", {})),
            inV=in_vertex,
            inVLabel=value['inVLabel'],
            outV=out_vertex,
            outVLabel=value['outVLabel']
        )


class PropertyTypeIO(GraphSONTypeIO):
    graphson_base_type = 'Property'

    @classmethod
    def deserialize(cls, value, reader=None):
        return {value["key"]: reader.deserialize(value["value"])}


class PathTypeIO(GraphSONTypeIO):
    graphson_base_type = 'Path'

    @classmethod
    def deserialize(cls, value, reader=None):
        labels = [set(label) for label in reader.deserialize(value['labels'])]
        objects = [obj for obj in reader.deserialize(value['objects'])]
        p = Path(labels, [])
        p.objects = objects  # avoid the object processing in Path.__init__
        return p


class TraversalMetricsTypeIO(GraphSONTypeIO):
    graphson_base_type = 'TraversalMetrics'

    @classmethod
    def deserialize(cls, value, reader=None):
        return reader.deserialize(value)


class MetricsTypeIO(GraphSONTypeIO):
    graphson_base_type = 'Metrics'

    @classmethod
    def deserialize(cls, value, reader=None):
        return reader.deserialize(value)


class JsonMapTypeIO(GraphSONTypeIO):
    """In GraphSON2, dict are simply serialized as json map"""

    @classmethod
    def serialize(cls, value, writer=None):
        out = {}
        for k, v in value.items():
            out[k] = writer.serialize(v, writer)

        return out


class MapTypeIO(GraphSONTypeIO):
    """In GraphSON3, dict has its own type"""

    graphson_base_type = 'Map'
    cql_type = 'map'

    @classmethod
    def definition(cls, value, writer=None):
        out = OrderedDict([('cqlType', cls.cql_type)])
        out['definition'] = []
        for k, v in value.items():
            # we just need the first pair to write the def
            out['definition'].append(writer.definition(k))
            out['definition'].append(writer.definition(v))
            break
        return out

    @classmethod
    def serialize(cls, value, writer=None):
        out = []
        for k, v in value.items():
            out.append(writer.serialize(k, writer))
            out.append(writer.serialize(v, writer))

        return out

    @classmethod
    def deserialize(cls, value, reader=None):
        out = {}
        a, b = itertools.tee(value)
        for key, val in zip(
            itertools.islice(a, 0, None, 2),
            itertools.islice(b, 1, None, 2)
        ):
            out[reader.deserialize(key)] = reader.deserialize(val)
        return out


class ListTypeIO(GraphSONTypeIO):
    """In GraphSON3, list has its own type"""

    graphson_base_type = 'List'
    cql_type = 'list'

    @classmethod
    def definition(cls, value, writer=None):
        out = OrderedDict([('cqlType', cls.cql_type)])
        out['definition'] = []
        if value:
            out['definition'].append(writer.definition(value[0]))
        return out

    @classmethod
    def serialize(cls, value, writer=None):
        return [writer.serialize(v, writer) for v in value]

    @classmethod
    def deserialize(cls, value, reader=None):
        return [reader.deserialize(obj) for obj in value]


class SetTypeIO(GraphSONTypeIO):
    """In GraphSON3, set has its own type"""

    graphson_base_type = 'Set'
    cql_type = 'set'

    @classmethod
    def definition(cls, value, writer=None):
        out = OrderedDict([('cqlType', cls.cql_type)])
        out['definition'] = []
        for v in value:
            # we only take into account the first value for the definition
            out['definition'].append(writer.definition(v))
            break
        return out

    @classmethod
    def serialize(cls, value, writer=None):
        return [writer.serialize(v, writer) for v in value]

    @classmethod
    def deserialize(cls, value, reader=None):
        lst = [reader.deserialize(obj) for obj in value]

        s = set(lst)
        if len(s) != len(lst):
            log.warning("Coercing g:Set to list due to numerical values returned by Java. "
                        "See TINKERPOP-1844 for details.")
            return lst

        return s


class BulkSetTypeIO(GraphSONTypeIO):
    graphson_base_type = "BulkSet"

    @classmethod
    def deserialize(cls, value, reader=None):
        out = []

        a, b = itertools.tee(value)
        for val, bulk in zip(
            itertools.islice(a, 0, None, 2),
            itertools.islice(b, 1, None, 2)
        ):
            val = reader.deserialize(val)
            bulk = reader.deserialize(bulk)
            for n in range(bulk):
                out.append(val)

        return out


class TupleTypeIO(GraphSONTypeIO):
    prefix = 'dse'
    graphson_base_type = 'Tuple'
    cql_type = 'tuple'

    @classmethod
    def definition(cls, value, writer=None):
        out = OrderedDict()
        out['cqlType'] = cls.cql_type
        serializers = [writer.get_serializer(s) for s in value]
        out['definition'] = [s.definition(v, writer) for v, s in zip(value, serializers)]
        return out

    @classmethod
    def serialize(cls, value, writer=None):
        out = cls.definition(value, writer)
        out['value'] = [writer.serialize(v, writer) for v in value]
        return out

    @classmethod
    def deserialize(cls, value, reader=None):
        return tuple(reader.deserialize(obj) for obj in value['value'])


class UserTypeIO(GraphSONTypeIO):
    prefix = 'dse'
    graphson_base_type = 'UDT'
    cql_type = 'udt'

    FROZEN_REMOVAL_REGEX = re.compile(r'frozen<"*([^"]+)"*>')

    @classmethod
    def cql_types_from_string(cls, typ):
        # sanitizing: remove frozen references and double quotes...
        return cql_types_from_string(
            re.sub(cls.FROZEN_REMOVAL_REGEX, r'\1', typ)
        )

    @classmethod
    def get_udt_definition(cls, value, writer):
        user_type_name = writer.user_types[type(value)]
        keyspace = writer.context['graph_name']
        return writer.context['cluster'].metadata.keyspaces[keyspace].user_types[user_type_name]

    @classmethod
    def is_collection(cls, typ):
        return typ in ['list', 'tuple', 'map', 'set']

    @classmethod
    def is_udt(cls, typ, writer):
        keyspace = writer.context['graph_name']
        if keyspace in writer.context['cluster'].metadata.keyspaces:
            return typ in writer.context['cluster'].metadata.keyspaces[keyspace].user_types
        return False

    @classmethod
    def field_definition(cls, types, writer, name=None):
        """
        Build the udt field definition. This is required when we have a complex udt type.
        """
        index = -1
        out = [OrderedDict() if name is None else OrderedDict([('fieldName', name)])]

        while types:
            index += 1
            typ = types.pop(0)
            if index > 0:
                out.append(OrderedDict())

            if cls.is_udt(typ, writer):
                keyspace = writer.context['graph_name']
                udt = writer.context['cluster'].metadata.keyspaces[keyspace].user_types[typ]
                out[index].update(cls.definition(udt, writer))
            elif cls.is_collection(typ):
                out[index]['cqlType'] = typ
                definition = cls.field_definition(types, writer)
                out[index]['definition'] = definition if isinstance(definition, list) else [definition]
            else:
                out[index]['cqlType'] = typ

        return out if len(out) > 1 else out[0]

    @classmethod
    def definition(cls, value, writer=None):
        udt = value if isinstance(value, UserType) else cls.get_udt_definition(value, writer)
        return OrderedDict([
            ('cqlType', cls.cql_type),
            ('keyspace', udt.keyspace),
            ('name', udt.name),
            ('definition', [
                cls.field_definition(cls.cql_types_from_string(typ), writer, name=name)
                for name, typ in zip(udt.field_names, udt.field_types)])
        ])

    @classmethod
    def serialize(cls, value, writer=None):
        udt = cls.get_udt_definition(value, writer)
        out = cls.definition(value, writer)
        out['value'] = []
        for name, typ in zip(udt.field_names, udt.field_types):
            out['value'].append(writer.serialize(getattr(value, name), writer))
        return out

    @classmethod
    def deserialize(cls, value, reader=None):
        udt_class = reader.context['cluster']._user_types[value['keyspace']][value['name']]
        kwargs = zip(
            list(map(lambda v: v['fieldName'], value['definition'])),
            [reader.deserialize(v) for v in value['value']]
        )
        return udt_class(**dict(kwargs))


class TTypeIO(GraphSONTypeIO):
    prefix = 'g'
    graphson_base_type = 'T'

    @classmethod
    def deserialize(cls, value, reader=None):
        return T.name_to_value[value]


class _BaseGraphSONSerializer(object):

    _serializers = OrderedDict()

    @classmethod
    def register(cls, type, serializer):
        cls._serializers[type] = serializer

    @classmethod
    def get_type_definitions(cls):
        return cls._serializers.copy()

    @classmethod
    def get_serializer(cls, value):
        """
        Get the serializer for a python object.

        :param value: The python object.
        """

        # The serializer matching logic is as follow:
        # 1. Try to find the python type by direct access.
        # 2. Try to find the first serializer by class inheritance.
        # 3. If no serializer found, return the raw value.

        # Note that when trying to find the serializer by class inheritance,
        # the order that serializers are registered is important. The use of
        # an OrderedDict is to avoid the difference between executions.
        serializer = None
        try:
            serializer = cls._serializers[type(value)]
        except KeyError:
            for key, serializer_ in cls._serializers.items():
                if isinstance(value, key):
                    serializer = serializer_
                    break

        if serializer:
            # A serializer can have specialized serializers (e.g for Int32 and Int64, so value dependant)
            serializer = serializer.get_specialized_serializer(value)

        return serializer

    @classmethod
    def serialize(cls, value, writer=None):
        """
        Serialize a python object to GraphSON.

        e.g 'P42DT10H5M37S'
        e.g. {'key': value}

        :param value: The python object to serialize.
        :param writer: A graphson serializer for recursive types (Optional)
        """
        serializer = cls.get_serializer(value)
        if serializer:
            return serializer.serialize(value, writer or cls)

        return value


class GraphSON1Serializer(_BaseGraphSONSerializer):
    """
    Serialize python objects to graphson types.
    """

    # When we fall back to a superclass's serializer, we iterate over this map.
    # We want that iteration order to be consistent, so we use an OrderedDict,
    # not a dict.
    _serializers = OrderedDict([
        (str, TextTypeIO),
        (bool, BooleanTypeIO),
        (bytearray, ByteBufferTypeIO),
        (Decimal, BigDecimalTypeIO),
        (datetime.date, LocalDateTypeIO),
        (datetime.time, LocalTimeTypeIO),
        (datetime.timedelta, DurationTypeIO),
        (datetime.datetime, InstantTypeIO),
        (uuid.UUID, UUIDTypeIO),
        (Polygon, PolygonTypeIO),
        (Point, PointTypeIO),
        (LineString, LineStringTypeIO),
        (dict, JsonMapTypeIO),
        (float, FloatTypeIO)
    ])


GraphSON1Serializer.register(ipaddress.IPv4Address, InetTypeIO)
GraphSON1Serializer.register(ipaddress.IPv6Address, InetTypeIO)
GraphSON1Serializer.register(memoryview, ByteBufferTypeIO)
GraphSON1Serializer.register(bytes, ByteBufferTypeIO)


class _BaseGraphSONDeserializer(object):

    _deserializers = {}

    @classmethod
    def get_type_definitions(cls):
        return cls._deserializers.copy()

    @classmethod
    def register(cls, graphson_type, serializer):
        cls._deserializers[graphson_type] = serializer

    @classmethod
    def get_deserializer(cls, graphson_type):
        try:
            return cls._deserializers[graphson_type]
        except KeyError:
            raise ValueError('Invalid `graphson_type` specified: {}'.format(graphson_type))

    @classmethod
    def deserialize(cls, graphson_type, value):
        """
        Deserialize a `graphson_type` value to a python object.

        :param graphson_base_type: The graphson graphson_type. e.g. 'gx:Instant'
        :param value: The graphson value to deserialize.
        """
        return cls.get_deserializer(graphson_type).deserialize(value)


class GraphSON1Deserializer(_BaseGraphSONDeserializer):
    """
    Deserialize graphson1 types to python objects.
    """
    _TYPES = [UUIDTypeIO, BigDecimalTypeIO, InstantTypeIO, BlobTypeIO, ByteBufferTypeIO,
              PointTypeIO, LineStringTypeIO, PolygonTypeIO, LocalDateTypeIO,
              LocalTimeTypeIO, DurationTypeIO, InetTypeIO]

    _deserializers = {
        t.graphson_type: t
        for t in _TYPES
    }

    @classmethod
    def deserialize_date(cls, value):
        return cls._deserializers[LocalDateTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_time(cls, value):
        return cls._deserializers[LocalTimeTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_timestamp(cls, value):
        return cls._deserializers[InstantTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_duration(cls, value):
        return cls._deserializers[DurationTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_int(cls, value):
        return int(value)

    deserialize_smallint = deserialize_int

    deserialize_varint = deserialize_int

    @classmethod
    def deserialize_bigint(cls, value):
        return cls.deserialize_int(value)

    @classmethod
    def deserialize_double(cls, value):
        return float(value)

    deserialize_float = deserialize_double

    @classmethod
    def deserialize_uuid(cls, value):
        return cls._deserializers[UUIDTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_decimal(cls, value):
        return cls._deserializers[BigDecimalTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_blob(cls, value):
        return cls._deserializers[ByteBufferTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_point(cls, value):
        return cls._deserializers[PointTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_linestring(cls, value):
        return cls._deserializers[LineStringTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_polygon(cls, value):
        return cls._deserializers[PolygonTypeIO.graphson_type].deserialize(value)

    @classmethod
    def deserialize_inet(cls, value):
        return value

    @classmethod
    def deserialize_boolean(cls, value):
        return value


# TODO Remove in the next major
GraphSON1TypeDeserializer = GraphSON1Deserializer
GraphSON1TypeSerializer = GraphSON1Serializer


class GraphSON2Serializer(_BaseGraphSONSerializer):
    TYPE_KEY = "@type"
    VALUE_KEY = "@value"

    _serializers = GraphSON1Serializer.get_type_definitions()

    def serialize(self, value, writer=None):
        """
        Serialize a type to GraphSON2.

        e.g {'@type': 'gx:Duration', '@value': 'P2DT4H'}

        :param value: The python object to serialize.
        """
        serializer = self.get_serializer(value)
        if not serializer:
            raise ValueError("Unable to find a serializer for value of type: ".format(type(value)))

        val = serializer.serialize(value, writer or self)
        if serializer is TypeWrapperTypeIO:
            graphson_base_type = value.type_io.graphson_base_type
            graphson_type = value.type_io.graphson_type
        else:
            graphson_base_type = serializer.graphson_base_type
            graphson_type = serializer.graphson_type

        if graphson_base_type is None:
            out = val
        else:
            out = {self.TYPE_KEY: graphson_type}
            if val is not None:
                out[self.VALUE_KEY] = val

        return out


GraphSON2Serializer.register(int, IntegerTypeIO)


class GraphSON2Deserializer(_BaseGraphSONDeserializer):

    _TYPES = GraphSON1Deserializer._TYPES + [
        Int16TypeIO, Int32TypeIO, Int64TypeIO, DoubleTypeIO, FloatTypeIO,
        BigIntegerTypeIO, VertexTypeIO, VertexPropertyTypeIO, EdgeTypeIO,
        PathTypeIO, PropertyTypeIO, TraversalMetricsTypeIO, MetricsTypeIO]

    _deserializers = {
        t.graphson_type: t
        for t in _TYPES
    }


class GraphSON2Reader(object):
    """
    GraphSON2 Reader that parse json and deserialize to python objects.
    """

    def __init__(self, context, extra_deserializer_map=None):
        """
        :param extra_deserializer_map: map from GraphSON type tag to deserializer instance implementing `deserialize`
        """
        self.context = context
        self.deserializers = GraphSON2Deserializer.get_type_definitions()
        if extra_deserializer_map:
            self.deserializers.update(extra_deserializer_map)

    def read(self, json_data):
        """
        Read and deserialize ``json_data``.
        """
        return self.deserialize(json.loads(json_data))

    def deserialize(self, obj):
        """
        Deserialize GraphSON type-tagged dict values into objects mapped in self.deserializers
        """
        if isinstance(obj, dict):
            try:
                des = self.deserializers[obj[GraphSON2Serializer.TYPE_KEY]]
                return des.deserialize(obj[GraphSON2Serializer.VALUE_KEY], self)
            except KeyError:
                pass
            # list and map are treated as normal json objs (could be isolated deserializers)
            return {self.deserialize(k): self.deserialize(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.deserialize(o) for o in obj]
        else:
            return obj


class TypeIOWrapper(object):
    """Used to force a graphson type during serialization"""

    type_io = None
    value = None

    def __init__(self, type_io, value):
        self.type_io = type_io
        self.value = value


def _wrap_value(type_io, value):
    return TypeIOWrapper(type_io, value)


to_bigint = partial(_wrap_value, Int64TypeIO)
to_int = partial(_wrap_value, Int32TypeIO)
to_smallint = partial(_wrap_value, Int16TypeIO)
to_double = partial(_wrap_value, DoubleTypeIO)
to_float = partial(_wrap_value, FloatTypeIO)


class GraphSON3Serializer(GraphSON2Serializer):

    _serializers = GraphSON2Serializer.get_type_definitions()

    context = None
    """A dict of the serialization context"""

    def __init__(self, context):
        self.context = context
        self.user_types = None

    def definition(self, value):
        serializer = self.get_serializer(value)
        return serializer.definition(value, self)

    def get_serializer(self, value):
        """Custom get_serializer to support UDT/Tuple"""

        serializer = super(GraphSON3Serializer, self).get_serializer(value)
        is_namedtuple_udt = serializer is TupleTypeIO and hasattr(value, '_fields')
        if not serializer or is_namedtuple_udt:
            # Check if UDT
            if self.user_types is None:
                try:
                    user_types = self.context['cluster']._user_types[self.context['graph_name']]
                    self.user_types = dict(map(reversed, user_types.items()))
                except KeyError:
                    self.user_types = {}

            serializer = UserTypeIO if (is_namedtuple_udt or (type(value) in self.user_types)) else serializer

        return serializer


GraphSON3Serializer.register(dict, MapTypeIO)
GraphSON3Serializer.register(list, ListTypeIO)
GraphSON3Serializer.register(set, SetTypeIO)
GraphSON3Serializer.register(tuple, TupleTypeIO)
GraphSON3Serializer.register(Duration, DseDurationTypeIO)
GraphSON3Serializer.register(TypeIOWrapper, TypeWrapperTypeIO)


class GraphSON3Deserializer(GraphSON2Deserializer):
    _TYPES = GraphSON2Deserializer._TYPES + [MapTypeIO, ListTypeIO,
                                             SetTypeIO, TupleTypeIO,
                                             UserTypeIO, DseDurationTypeIO,
                                             TTypeIO, BulkSetTypeIO]

    _deserializers = {t.graphson_type: t for t in _TYPES}


class GraphSON3Reader(GraphSON2Reader):
    """
    GraphSON3 Reader that parse json and deserialize to python objects.
    """

    def __init__(self, context, extra_deserializer_map=None):
        """
        :param context: A dict of the context, mostly used as context for udt deserialization.
        :param extra_deserializer_map: map from GraphSON type tag to deserializer instance implementing `deserialize`
        """
        self.context = context
        self.deserializers = GraphSON3Deserializer.get_type_definitions()
        if extra_deserializer_map:
            self.deserializers.update(extra_deserializer_map)
