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

import json
from warnings import warn

from cassandra import ConsistencyLevel
from cassandra.query import Statement, SimpleStatement
from cassandra.datastax.graph.types import Vertex, Edge, Path, VertexProperty
from cassandra.datastax.graph.graphson import GraphSON2Reader, GraphSON3Reader


__all__ = [
    'GraphProtocol', 'GraphOptions', 'GraphStatement', 'SimpleGraphStatement',
    'single_object_row_factory', 'graph_result_row_factory', 'graph_object_row_factory',
    'graph_graphson2_row_factory', 'Result', 'graph_graphson3_row_factory'
]

# (attr, description, server option)
_graph_options = (
    ('graph_name', 'name of the targeted graph.', 'graph-name'),
    ('graph_source', 'choose the graph traversal source, configured on the server side.', 'graph-source'),
    ('graph_language', 'the language used in the queries (default "gremlin-groovy")', 'graph-language'),
    ('graph_protocol', 'the graph protocol that the server should use for query results (default "graphson-1-0")', 'graph-results'),
    ('graph_read_consistency_level', '''read `cassandra.ConsistencyLevel <http://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/#cassandra.ConsistencyLevel>`_ for graph queries (if distinct from session default).
Setting this overrides the native `Statement.consistency_level <http://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/query/#cassandra.query.Statement.consistency_level>`_ for read operations from Cassandra persistence''', 'graph-read-consistency'),
    ('graph_write_consistency_level', '''write `cassandra.ConsistencyLevel <http://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/#cassandra.ConsistencyLevel>`_ for graph queries (if distinct from session default).
Setting this overrides the native `Statement.consistency_level <http://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/query/#cassandra.query.Statement.consistency_level>`_ for write operations to Cassandra persistence.''', 'graph-write-consistency')
)
_graph_option_names = tuple(option[0] for option in _graph_options)

# this is defined by the execution profile attribute, not in graph options
_request_timeout_key = 'request-timeout'


class GraphProtocol(object):

    GRAPHSON_1_0 = b'graphson-1.0'
    """
    GraphSON1
    """

    GRAPHSON_2_0 = b'graphson-2.0'
    """
    GraphSON2
    """

    GRAPHSON_3_0 = b'graphson-3.0'
    """
    GraphSON3
    """


class GraphOptions(object):
    """
    Options for DSE Graph Query handler.
    """
    # See _graph_options map above for notes on valid options

    DEFAULT_GRAPH_PROTOCOL = GraphProtocol.GRAPHSON_1_0
    DEFAULT_GRAPH_LANGUAGE = b'gremlin-groovy'

    def __init__(self, **kwargs):
        self._graph_options = {}
        kwargs.setdefault('graph_source', 'g')
        kwargs.setdefault('graph_language', GraphOptions.DEFAULT_GRAPH_LANGUAGE)
        for attr, value in kwargs.items():
            if attr not in _graph_option_names:
                warn("Unknown keyword argument received for GraphOptions: {0}".format(attr))
            setattr(self, attr, value)

    def copy(self):
        new_options = GraphOptions()
        new_options._graph_options = self._graph_options.copy()
        return new_options

    def update(self, options):
        self._graph_options.update(options._graph_options)

    def get_options_map(self, other_options=None):
        """
        Returns a map for these options updated with other options,
        and mapped to graph payload types.
        """
        options = self._graph_options.copy()
        if other_options:
            options.update(other_options._graph_options)

        # cls are special-cased so they can be enums in the API, and names in the protocol
        for cl in ('graph-write-consistency', 'graph-read-consistency'):
            cl_enum = options.get(cl)
            if cl_enum is not None:
                options[cl] = ConsistencyLevel.value_to_name[cl_enum].encode()
        return options

    def set_source_default(self):
        """
        Sets ``graph_source`` to the server-defined default traversal source ('default')
        """
        self.graph_source = 'default'

    def set_source_analytics(self):
        """
        Sets ``graph_source`` to the server-defined analytic traversal source ('a')
        """
        self.graph_source = 'a'

    def set_source_graph(self):
        """
        Sets ``graph_source`` to the server-defined graph traversal source ('g')
        """
        self.graph_source = 'g'

    def set_graph_protocol(self, protocol):
        """
        Sets ``graph_protocol`` as server graph results format (See :class:`cassandra.datastax.graph.GraphProtocol`)
        """
        self.graph_protocol = protocol

    @property
    def is_default_source(self):
        return self.graph_source in (b'default', None)

    @property
    def is_analytics_source(self):
        """
        True if ``graph_source`` is set to the server-defined analytics traversal source ('a')
        """
        return self.graph_source == b'a'

    @property
    def is_graph_source(self):
        """
        True if ``graph_source`` is set to the server-defined graph traversal source ('g')
        """
        return self.graph_source == b'g'


for opt in _graph_options:

    def get(self, key=opt[2]):
        return self._graph_options.get(key)

    def set(self, value, key=opt[2]):
        if value is not None:
            # normalize text here so it doesn't have to be done every time we get options map
            if isinstance(value, str):
                value = value.encode()
            self._graph_options[key] = value
        else:
            self._graph_options.pop(key, None)

    def delete(self, key=opt[2]):
        self._graph_options.pop(key, None)

    setattr(GraphOptions, opt[0], property(get, set, delete, opt[1]))


class GraphStatement(Statement):
    """ An abstract class representing a graph query."""

    @property
    def query(self):
        raise NotImplementedError()

    def __str__(self):
        return u'<GraphStatement query="{0}">'.format(self.query)
    __repr__ = __str__


class SimpleGraphStatement(GraphStatement, SimpleStatement):
    """
    Simple graph statement for :meth:`.Session.execute_graph`.
    Takes the same parameters as :class:`.SimpleStatement`.
    """
    @property
    def query(self):
        return self._query_string


def single_object_row_factory(column_names, rows):
    """
    returns the JSON string value of graph results
    """
    return [row[0] for row in rows]


def graph_result_row_factory(column_names, rows):
    """
    Returns a :class:`Result <cassandra.datastax.graph.Result>` object that can load graph results and produce specific types.
    The Result JSON is deserialized and unpacked from the top-level 'result' dict.
    """
    return [Result(json.loads(row[0])['result']) for row in rows]


def graph_object_row_factory(column_names, rows):
    """
    Like :func:`~.graph_result_row_factory`, except known element types (:class:`~.Vertex`, :class:`~.Edge`) are
    converted to their simplified objects. Some low-level metadata is shed in this conversion. Unknown result types are
    still returned as :class:`Result <cassandra.datastax.graph.Result>`.
    """
    return _graph_object_sequence(json.loads(row[0])['result'] for row in rows)


def _graph_object_sequence(objects):
    for o in objects:
        res = Result(o)
        if isinstance(o, dict):
            typ = res.value.get('type')
            if typ == 'vertex':
                res = res.as_vertex()
            elif typ == 'edge':
                res = res.as_edge()
        yield res


class _GraphSONContextRowFactory(object):
    graphson_reader_class = None
    graphson_reader_kwargs = None

    def __init__(self, cluster):
        context = {'cluster': cluster}
        kwargs = self.graphson_reader_kwargs or {}
        self.graphson_reader = self.graphson_reader_class(context, **kwargs)

    def __call__(self, column_names, rows):
        return [self.graphson_reader.read(row[0])['result'] for row in rows]


class _GraphSON2RowFactory(_GraphSONContextRowFactory):
    """Row factory to deserialize GraphSON2 results."""
    graphson_reader_class = GraphSON2Reader


class _GraphSON3RowFactory(_GraphSONContextRowFactory):
    """Row factory to deserialize GraphSON3 results."""
    graphson_reader_class = GraphSON3Reader


graph_graphson2_row_factory = _GraphSON2RowFactory
graph_graphson3_row_factory = _GraphSON3RowFactory


class Result(object):
    """
    Represents deserialized graph results.
    Property and item getters are provided for convenience.
    """

    value = None
    """
    Deserialized value from the result
    """

    def __init__(self, value):
        self.value = value

    def __getattr__(self, attr):
        if not isinstance(self.value, dict):
            raise ValueError("Value cannot be accessed as a dict")

        if attr in self.value:
            return self.value[attr]

        raise AttributeError("Result has no top-level attribute %r" % (attr,))

    def __getitem__(self, item):
        if isinstance(self.value, dict) and isinstance(item, str):
            return self.value[item]
        elif isinstance(self.value, list) and isinstance(item, int):
            return self.value[item]
        else:
            raise ValueError("Result cannot be indexed by %r" % (item,))

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "%s(%r)" % (Result.__name__, self.value)

    def __eq__(self, other):
        return self.value == other.value

    def as_vertex(self):
        """
        Return a :class:`Vertex` parsed from this result

        Raises TypeError if parsing fails (i.e. the result structure is not valid).
        """
        try:
            return Vertex(self.id, self.label, self.type, self.value.get('properties', {}))
        except (AttributeError, ValueError, TypeError):
            raise TypeError("Could not create Vertex from %r" % (self,))

    def as_edge(self):
        """
        Return a :class:`Edge` parsed from this result

        Raises TypeError if parsing fails (i.e. the result structure is not valid).
        """
        try:
            return Edge(self.id, self.label, self.type, self.value.get('properties', {}),
                        self.inV, self.inVLabel, self.outV, self.outVLabel)
        except (AttributeError, ValueError, TypeError):
            raise TypeError("Could not create Edge from %r" % (self,))

    def as_path(self):
        """
        Return a :class:`Path` parsed from this result

        Raises TypeError if parsing fails (i.e. the result structure is not valid).
        """
        try:
            return Path(self.labels, self.objects)
        except (AttributeError, ValueError, TypeError):
            raise TypeError("Could not create Path from %r" % (self,))

    def as_vertex_property(self):
        return VertexProperty(self.value.get('label'), self.value.get('value'), self.value.get('properties', {}))
