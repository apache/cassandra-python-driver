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

from cassandra.graph import SimpleGraphStatement, GraphProtocol
from cassandra.cluster import EXEC_PROFILE_GRAPH_DEFAULT

from gremlin_python.process.graph_traversal import GraphTraversal
from gremlin_python.structure.io.graphsonV2d0 import GraphSONWriter as GraphSONWriterV2
from gremlin_python.structure.io.graphsonV3d0 import GraphSONWriter as GraphSONWriterV3

from cassandra.datastax.graph.fluent.serializers import GremlinUserTypeIO, \
    dse_graphson2_serializers, dse_graphson3_serializers

log = logging.getLogger(__name__)


__all__ = ['TraversalBatch', '_query_from_traversal', '_DefaultTraversalBatch']


class _GremlinGraphSONWriterAdapter(object):

    def __init__(self, context, **kwargs):
        super(_GremlinGraphSONWriterAdapter, self).__init__(**kwargs)
        self.context = context
        self.user_types = None

    def serialize(self, value, _):
        return self.toDict(value)

    def get_serializer(self, value):
        serializer = None
        try:
            serializer = self.serializers[type(value)]
        except KeyError:
            for key, ser in self.serializers.items():
                if isinstance(value, key):
                    serializer = ser

        if self.context:
            # Check if UDT
            if self.user_types is None:
                try:
                    user_types = self.context['cluster']._user_types[self.context['graph_name']]
                    self.user_types = dict(map(reversed, user_types.items()))
                except KeyError:
                    self.user_types = {}

            # Custom detection to map a namedtuple to udt
            if (tuple in self.serializers and serializer is self.serializers[tuple] and hasattr(value, '_fields') or
                (not serializer and type(value) in self.user_types)):
                serializer = GremlinUserTypeIO

        if serializer:
            try:
                # A serializer can have specialized serializers (e.g for Int32 and Int64, so value dependant)
                serializer = serializer.get_specialized_serializer(value)
            except AttributeError:
                pass

        return serializer

    def toDict(self, obj):
        serializer = self.get_serializer(obj)
        return serializer.dictify(obj, self) if serializer else obj

    def definition(self, value):
        serializer = self.get_serializer(value)
        return serializer.definition(value, self)


class GremlinGraphSON2Writer(_GremlinGraphSONWriterAdapter, GraphSONWriterV2):
    pass


class GremlinGraphSON3Writer(_GremlinGraphSONWriterAdapter, GraphSONWriterV3):
    pass


graphson2_writer = GremlinGraphSON2Writer
graphson3_writer = GremlinGraphSON3Writer


def _query_from_traversal(traversal, graph_protocol, context=None):
    """
    From a GraphTraversal, return a query string.

    :param traversal: The GraphTraversal object
    :param graphson_protocol: The graph protocol to determine the output format.
    """
    if graph_protocol == GraphProtocol.GRAPHSON_2_0:
        graphson_writer = graphson2_writer(context, serializer_map=dse_graphson2_serializers)
    elif graph_protocol == GraphProtocol.GRAPHSON_3_0:
        if context is None:
            raise ValueError('Missing context for GraphSON3 serialization requires.')
        graphson_writer = graphson3_writer(context, serializer_map=dse_graphson3_serializers)
    else:
        raise ValueError('Unknown graph protocol: {}'.format(graph_protocol))

    try:
        query = graphson_writer.writeObject(traversal)
    except Exception:
        log.exception("Error preparing graphson traversal query:")
        raise

    return query


class TraversalBatch(object):
    """
    A `TraversalBatch` is used to execute multiple graph traversals in a
    single transaction. If any traversal in the batch fails, the entire
    batch will fail to apply.

    If a TraversalBatch is bounded to a DSE session, it can be executed using
    `traversal_batch.execute()`.
    """

    _session = None
    _execution_profile = None

    def __init__(self, session=None, execution_profile=None):
        """
        :param session: (Optional) A DSE session
        :param execution_profile: (Optional) The execution profile to use for the batch execution
        """
        self._session = session
        self._execution_profile = execution_profile

    def add(self, traversal):
        """
        Add a traversal to the batch.

        :param traversal: A gremlin GraphTraversal
        """
        raise NotImplementedError()

    def add_all(self, traversals):
        """
        Adds a sequence of traversals to the batch.

        :param traversals: A sequence of gremlin GraphTraversal
        """
        raise NotImplementedError()

    def execute(self):
        """
        Execute the traversal batch if bounded to a `DSE Session`.
        """
        raise NotImplementedError()

    def as_graph_statement(self, graph_protocol=GraphProtocol.GRAPHSON_2_0):
        """
        Return the traversal batch as GraphStatement.

        :param graph_protocol: The graph protocol for the GraphSONWriter. Default is GraphProtocol.GRAPHSON_2_0.
        """
        raise NotImplementedError()

    def clear(self):
        """
        Clear a traversal batch for reuse.
        """
        raise NotImplementedError()

    def __len__(self):
        raise NotImplementedError()

    def __str__(self):
        return u'<TraversalBatch traversals={0}>'.format(len(self))
    __repr__ = __str__


class _DefaultTraversalBatch(TraversalBatch):

    _traversals = None

    def __init__(self, *args, **kwargs):
        super(_DefaultTraversalBatch, self).__init__(*args, **kwargs)
        self._traversals = []

    def add(self, traversal):
        if not isinstance(traversal, GraphTraversal):
            raise ValueError('traversal should be a gremlin GraphTraversal')

        self._traversals.append(traversal)
        return self

    def add_all(self, traversals):
        for traversal in traversals:
            self.add(traversal)

    def as_graph_statement(self, graph_protocol=GraphProtocol.GRAPHSON_2_0, context=None):
        statements = [_query_from_traversal(t, graph_protocol, context) for t in self._traversals]
        query = u"[{0}]".format(','.join(statements))
        return SimpleGraphStatement(query)

    def execute(self):
        if self._session is None:
            raise ValueError('A DSE Session must be provided to execute the traversal batch.')

        execution_profile = self._execution_profile if self._execution_profile else EXEC_PROFILE_GRAPH_DEFAULT
        graph_options = self._session.get_execution_profile(execution_profile).graph_options
        context = {
            'cluster': self._session.cluster,
            'graph_name': graph_options.graph_name
        }
        statement = self.as_graph_statement(graph_options.graph_protocol, context=context) \
            if graph_options.graph_protocol else self.as_graph_statement(context=context)
        return self._session.execute_graph(statement, execution_profile=execution_profile)

    def clear(self):
        del self._traversals[:]

    def __len__(self):
        return len(self._traversals)
