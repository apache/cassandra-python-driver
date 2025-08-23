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

from collections import OrderedDict

from gremlin_python.structure.io.graphsonV2d0 import (
    GraphSONReader as GraphSONReaderV2,
    GraphSONUtil as GraphSONUtil,  # no difference between v2 and v3
    VertexDeserializer as VertexDeserializerV2,
    VertexPropertyDeserializer as VertexPropertyDeserializerV2,
    PropertyDeserializer as PropertyDeserializerV2,
    EdgeDeserializer as EdgeDeserializerV2,
    PathDeserializer as PathDeserializerV2
)

from gremlin_python.structure.io.graphsonV3d0 import (
    GraphSONReader as GraphSONReaderV3,
    VertexDeserializer as VertexDeserializerV3,
    VertexPropertyDeserializer as VertexPropertyDeserializerV3,
    PropertyDeserializer as PropertyDeserializerV3,
    EdgeDeserializer as EdgeDeserializerV3,
    PathDeserializer as PathDeserializerV3
)

try:
    from gremlin_python.structure.io.graphsonV2d0 import (
        TraversalMetricsDeserializer as TraversalMetricsDeserializerV2,
        MetricsDeserializer as MetricsDeserializerV2
    )
    from gremlin_python.structure.io.graphsonV3d0 import (
        TraversalMetricsDeserializer as TraversalMetricsDeserializerV3,
        MetricsDeserializer as MetricsDeserializerV3
    )
except ImportError:
    TraversalMetricsDeserializerV2 = MetricsDeserializerV2 = None
    TraversalMetricsDeserializerV3 = MetricsDeserializerV3 = None

from cassandra.graph import (
    GraphSON2Serializer,
    GraphSON2Deserializer,
    GraphSON3Serializer,
    GraphSON3Deserializer
)
from cassandra.graph.graphson import UserTypeIO, TypeWrapperTypeIO
from cassandra.datastax.graph.fluent.predicates import GeoP, TextDistanceP
from cassandra.util import Distance


__all__ = ['GremlinGraphSONReader', 'GeoPSerializer', 'TextDistancePSerializer',
           'DistanceIO', 'gremlin_deserializers', 'deserializers', 'serializers',
           'GremlinGraphSONReaderV2', 'GremlinGraphSONReaderV3', 'dse_graphson2_serializers',
           'dse_graphson2_deserializers', 'dse_graphson3_serializers', 'dse_graphson3_deserializers',
           'gremlin_graphson2_deserializers', 'gremlin_graphson3_deserializers', 'GremlinUserTypeIO']


class _GremlinGraphSONTypeSerializer(object):
    TYPE_KEY = "@type"
    VALUE_KEY = "@value"
    serializer = None

    def __init__(self, serializer):
        self.serializer = serializer

    def dictify(self, v, writer):
        value = self.serializer.serialize(v, writer)
        if self.serializer is TypeWrapperTypeIO:
            graphson_base_type = v.type_io.graphson_base_type
            graphson_type = v.type_io.graphson_type
        else:
            graphson_base_type = self.serializer.graphson_base_type
            graphson_type = self.serializer.graphson_type

        if graphson_base_type is None:
            out = value
        else:
            out = {self.TYPE_KEY: graphson_type}
            if value is not None:
                out[self.VALUE_KEY] = value

        return out

    def definition(self, value, writer=None):
        return self.serializer.definition(value, writer)

    def get_specialized_serializer(self, value):
        ser = self.serializer.get_specialized_serializer(value)
        if ser is not self.serializer:
            return _GremlinGraphSONTypeSerializer(ser)
        return self


class _GremlinGraphSONTypeDeserializer(object):

    deserializer = None

    def __init__(self, deserializer):
        self.deserializer = deserializer

    def objectify(self, v, reader):
        return self.deserializer.deserialize(v, reader)


def _make_gremlin_graphson2_deserializer(graphson_type):
    return _GremlinGraphSONTypeDeserializer(
        GraphSON2Deserializer.get_deserializer(graphson_type.graphson_type)
    )


def _make_gremlin_graphson3_deserializer(graphson_type):
    return _GremlinGraphSONTypeDeserializer(
        GraphSON3Deserializer.get_deserializer(graphson_type.graphson_type)
    )


class _GremlinGraphSONReader(object):
    """Gremlin GraphSONReader Adapter, required to use gremlin types"""

    context = None

    def __init__(self, context, deserializer_map=None):
        self.context = context
        super(_GremlinGraphSONReader, self).__init__(deserializer_map)

    def deserialize(self, obj):
        return self.toObject(obj)


class GremlinGraphSONReaderV2(_GremlinGraphSONReader, GraphSONReaderV2):
    pass

# TODO remove next major
GremlinGraphSONReader = GremlinGraphSONReaderV2

class GremlinGraphSONReaderV3(_GremlinGraphSONReader, GraphSONReaderV3):
    pass


class GeoPSerializer(object):
    @classmethod
    def dictify(cls, p, writer):
        out = {
            "predicateType": "Geo",
            "predicate": p.operator,
            "value": [writer.toDict(p.value), writer.toDict(p.other)] if p.other is not None else writer.toDict(p.value)
        }
        return GraphSONUtil.typedValue("P", out, prefix='dse')


class TextDistancePSerializer(object):
    @classmethod
    def dictify(cls, p, writer):
        out = {
            "predicate": p.operator,
            "value": {
                'query': writer.toDict(p.value),
                'distance': writer.toDict(p.distance)
            }
        }
        return GraphSONUtil.typedValue("P", out)


class DistanceIO(object):
    @classmethod
    def dictify(cls, v, _):
        return GraphSONUtil.typedValue('Distance', str(v), prefix='dse')


GremlinUserTypeIO = _GremlinGraphSONTypeSerializer(UserTypeIO)

# GraphSON2
dse_graphson2_serializers = OrderedDict([
    (t, _GremlinGraphSONTypeSerializer(s))
    for t, s in GraphSON2Serializer.get_type_definitions().items()
])

dse_graphson2_serializers.update(OrderedDict([
    (Distance, DistanceIO),
    (GeoP, GeoPSerializer),
    (TextDistanceP, TextDistancePSerializer)
]))

# TODO remove next major, this is just in case someone was using it
serializers = dse_graphson2_serializers

dse_graphson2_deserializers = {
    k: _make_gremlin_graphson2_deserializer(v)
    for k, v in GraphSON2Deserializer.get_type_definitions().items()
}

dse_graphson2_deserializers.update({
    "dse:Distance": DistanceIO,
})

# TODO remove next major, this is just in case someone was using it
deserializers = dse_graphson2_deserializers

gremlin_graphson2_deserializers = dse_graphson2_deserializers.copy()
gremlin_graphson2_deserializers.update({
    'g:Vertex': VertexDeserializerV2,
    'g:VertexProperty': VertexPropertyDeserializerV2,
    'g:Edge': EdgeDeserializerV2,
    'g:Property': PropertyDeserializerV2,
    'g:Path': PathDeserializerV2
})

if TraversalMetricsDeserializerV2:
    gremlin_graphson2_deserializers.update({
        'g:TraversalMetrics': TraversalMetricsDeserializerV2,
        'g:lMetrics': MetricsDeserializerV2
    })

# TODO remove next major, this is just in case someone was using it
gremlin_deserializers = gremlin_graphson2_deserializers

# GraphSON3
dse_graphson3_serializers = OrderedDict([
    (t, _GremlinGraphSONTypeSerializer(s))
    for t, s in GraphSON3Serializer.get_type_definitions().items()
])

dse_graphson3_serializers.update(OrderedDict([
    (Distance, DistanceIO),
    (GeoP, GeoPSerializer),
    (TextDistanceP, TextDistancePSerializer)
]))

dse_graphson3_deserializers = {
    k: _make_gremlin_graphson3_deserializer(v)
    for k, v in GraphSON3Deserializer.get_type_definitions().items()
}

dse_graphson3_deserializers.update({
    "dse:Distance": DistanceIO
})

gremlin_graphson3_deserializers = dse_graphson3_deserializers.copy()
gremlin_graphson3_deserializers.update({
    'g:Vertex': VertexDeserializerV3,
    'g:VertexProperty': VertexPropertyDeserializerV3,
    'g:Edge': EdgeDeserializerV3,
    'g:Property': PropertyDeserializerV3,
    'g:Path': PathDeserializerV3
})

if TraversalMetricsDeserializerV3:
    gremlin_graphson3_deserializers.update({
        'g:TraversalMetrics': TraversalMetricsDeserializerV3,
        'g:Metrics': MetricsDeserializerV3
    })
