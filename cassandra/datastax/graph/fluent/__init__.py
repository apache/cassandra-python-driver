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
import copy

from concurrent.futures import Future

HAVE_GREMLIN = False
try:
    import gremlin_python
    HAVE_GREMLIN = True
except ImportError:
    # gremlinpython is not installed.
    pass

if HAVE_GREMLIN:
    from gremlin_python.structure.graph import Graph
    from gremlin_python.driver.remote_connection import RemoteConnection, RemoteTraversal
    from gremlin_python.process.traversal import Traverser, TraversalSideEffects
    from gremlin_python.process.graph_traversal import GraphTraversal

    from cassandra.cluster import Session, GraphExecutionProfile, EXEC_PROFILE_GRAPH_DEFAULT
    from cassandra.datastax.graph import GraphOptions, GraphProtocol
    from cassandra.datastax.graph.query import _GraphSONContextRowFactory

    from cassandra.datastax.graph.fluent.serializers import (
        GremlinGraphSONReaderV2,
        GremlinGraphSONReaderV3,
        dse_graphson2_deserializers,
        gremlin_graphson2_deserializers,
        dse_graphson3_deserializers,
        gremlin_graphson3_deserializers
    )
    from cassandra.datastax.graph.fluent.query import _DefaultTraversalBatch, _query_from_traversal

    log = logging.getLogger(__name__)

    __all__ = ['BaseGraphRowFactory', 'graph_traversal_row_factory',
               'graph_traversal_dse_object_row_factory', 'DSESessionRemoteGraphConnection', 'DseGraph']

    # Traversal result keys
    _bulk_key = 'bulk'
    _result_key = 'result'


    class BaseGraphRowFactory(_GraphSONContextRowFactory):
        """
        Base row factory for graph traversal. This class basically wraps a
        graphson reader function to handle additional features of Gremlin/DSE
        and is callable as a normal row factory.

        Currently supported:
          - bulk results
        """

        def __call__(self, column_names, rows):
            for row in rows:
                parsed_row = self.graphson_reader.readObject(row[0])
                yield parsed_row[_result_key]
                bulk = parsed_row.get(_bulk_key, 1)
                for _ in range(bulk - 1):
                    yield copy.deepcopy(parsed_row[_result_key])


    class _GremlinGraphSON2RowFactory(BaseGraphRowFactory):
        """Row Factory that returns the decoded graphson2."""
        graphson_reader_class = GremlinGraphSONReaderV2
        graphson_reader_kwargs = {'deserializer_map': gremlin_graphson2_deserializers}


    class _DseGraphSON2RowFactory(BaseGraphRowFactory):
        """Row Factory that returns the decoded graphson2 as DSE types."""
        graphson_reader_class = GremlinGraphSONReaderV2
        graphson_reader_kwargs = {'deserializer_map': dse_graphson2_deserializers}

    gremlin_graphson2_traversal_row_factory = _GremlinGraphSON2RowFactory
    # TODO remove in next major
    graph_traversal_row_factory = gremlin_graphson2_traversal_row_factory

    dse_graphson2_traversal_row_factory = _DseGraphSON2RowFactory
    # TODO remove in next major
    graph_traversal_dse_object_row_factory = dse_graphson2_traversal_row_factory


    class _GremlinGraphSON3RowFactory(BaseGraphRowFactory):
        """Row Factory that returns the decoded graphson2."""
        graphson_reader_class = GremlinGraphSONReaderV3
        graphson_reader_kwargs = {'deserializer_map': gremlin_graphson3_deserializers}


    class _DseGraphSON3RowFactory(BaseGraphRowFactory):
        """Row Factory that returns the decoded graphson3 as DSE types."""
        graphson_reader_class = GremlinGraphSONReaderV3
        graphson_reader_kwargs = {'deserializer_map': dse_graphson3_deserializers}


    gremlin_graphson3_traversal_row_factory = _GremlinGraphSON3RowFactory
    dse_graphson3_traversal_row_factory = _DseGraphSON3RowFactory


    class DSESessionRemoteGraphConnection(RemoteConnection):
        """
        A Tinkerpop RemoteConnection to execute traversal queries on DSE.

        :param session: A DSE session
        :param graph_name: (Optional) DSE Graph name.
        :param execution_profile: (Optional) Execution profile for traversal queries. Default is set to `EXEC_PROFILE_GRAPH_DEFAULT`.
        """

        session = None
        graph_name = None
        execution_profile = None

        def __init__(self, session, graph_name=None, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
            super(DSESessionRemoteGraphConnection, self).__init__(None, None)

            if not isinstance(session, Session):
                raise ValueError('A DSE Session must be provided to execute graph traversal queries.')

            self.session = session
            self.graph_name = graph_name
            self.execution_profile = execution_profile

        @staticmethod
        def _traversers_generator(traversers):
            for t in traversers:
                yield Traverser(t)

        def _prepare_query(self, bytecode):
            ep = self.session.execution_profile_clone_update(self.execution_profile)
            graph_options = ep.graph_options
            graph_options.graph_name = self.graph_name or graph_options.graph_name
            graph_options.graph_language = DseGraph.DSE_GRAPH_QUERY_LANGUAGE
            # We resolve the execution profile options here , to know how what gremlin factory to set
            self.session._resolve_execution_profile_options(ep)

            context = None
            if graph_options.graph_protocol == GraphProtocol.GRAPHSON_2_0:
                row_factory = gremlin_graphson2_traversal_row_factory
            elif graph_options.graph_protocol == GraphProtocol.GRAPHSON_3_0:
                row_factory = gremlin_graphson3_traversal_row_factory
                context = {
                    'cluster': self.session.cluster,
                    'graph_name': graph_options.graph_name.decode('utf-8')
                }
            else:
                raise ValueError('Unknown graph protocol: {}'.format(graph_options.graph_protocol))

            ep.row_factory = row_factory
            query = DseGraph.query_from_traversal(bytecode, graph_options.graph_protocol, context)

            return query, ep

        @staticmethod
        def _handle_query_results(result_set, gremlin_future):
            try:
                gremlin_future.set_result(
                    RemoteTraversal(DSESessionRemoteGraphConnection._traversers_generator(result_set), TraversalSideEffects())
                )
            except Exception as e:
                gremlin_future.set_exception(e)

        @staticmethod
        def _handle_query_error(response, gremlin_future):
            gremlin_future.set_exception(response)

        def submit(self, bytecode):
            # the only reason I don't use submitAsync here
            # is to avoid an unuseful future wrap
            query, ep = self._prepare_query(bytecode)

            traversers = self.session.execute_graph(query, execution_profile=ep)
            return RemoteTraversal(self._traversers_generator(traversers), TraversalSideEffects())

        def submitAsync(self, bytecode):
            query, ep = self._prepare_query(bytecode)

            # to be compatible with gremlinpython, we need to return a concurrent.futures.Future
            gremlin_future = Future()
            response_future = self.session.execute_graph_async(query, execution_profile=ep)
            response_future.add_callback(self._handle_query_results, gremlin_future)
            response_future.add_errback(self._handle_query_error, gremlin_future)

            return gremlin_future

        def __str__(self):
            return "<DSESessionRemoteGraphConnection: graph_name='{0}'>".format(self.graph_name)

        __repr__ = __str__


    class DseGraph(object):
        """
        Dse Graph utility class for GraphTraversal construction and execution.
        """

        DSE_GRAPH_QUERY_LANGUAGE = 'bytecode-json'
        """
        Graph query language, Default is 'bytecode-json' (GraphSON).
        """

        DSE_GRAPH_QUERY_PROTOCOL = GraphProtocol.GRAPHSON_2_0
        """
        Graph query language, Default is GraphProtocol.GRAPHSON_2_0.
        """

        @staticmethod
        def query_from_traversal(traversal, graph_protocol=DSE_GRAPH_QUERY_PROTOCOL, context=None):
            """
            From a GraphTraversal, return a query string based on the language specified in `DseGraph.DSE_GRAPH_QUERY_LANGUAGE`.

            :param traversal: The GraphTraversal object
            :param graph_protocol: The graph protocol. Default is `DseGraph.DSE_GRAPH_QUERY_PROTOCOL`.
            :param context: The dict of the serialization context, needed for GraphSON3 (tuple, udt).
                            e.g: {'cluster': cluster, 'graph_name': name}
            """

            if isinstance(traversal, GraphTraversal):
                for strategy in traversal.traversal_strategies.traversal_strategies:
                    rc = strategy.remote_connection
                    if (isinstance(rc, DSESessionRemoteGraphConnection) and
                            rc.session or rc.graph_name or rc.execution_profile):
                        log.warning("GraphTraversal session, graph_name and execution_profile are "
                                    "only taken into account when executed with TinkerPop.")

            return _query_from_traversal(traversal, graph_protocol, context)

        @staticmethod
        def traversal_source(session=None, graph_name=None, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT,
                             traversal_class=None):
            """
            Returns a TinkerPop GraphTraversalSource binded to the session and graph_name if provided.

            :param session: (Optional) A DSE session
            :param graph_name: (Optional) DSE Graph name
            :param execution_profile: (Optional) Execution profile for traversal queries. Default is set to `EXEC_PROFILE_GRAPH_DEFAULT`.
            :param traversal_class: (Optional) The GraphTraversalSource class to use (DSL).

            .. code-block:: python

                from cassandra.cluster import Cluster
                from cassandra.datastax.graph.fluent import DseGraph

                c = Cluster()
                session = c.connect()

                g = DseGraph.traversal_source(session, 'my_graph')
                print(g.V().valueMap().toList())

            """

            graph = Graph()
            traversal_source = graph.traversal(traversal_class)

            if session:
                traversal_source = traversal_source.withRemote(
                    DSESessionRemoteGraphConnection(session, graph_name, execution_profile))

            return traversal_source

        @staticmethod
        def create_execution_profile(graph_name, graph_protocol=DSE_GRAPH_QUERY_PROTOCOL, **kwargs):
            """
            Creates an ExecutionProfile for GraphTraversal execution. You need to register that execution profile to the
            cluster by using `cluster.add_execution_profile`.

            :param graph_name: The graph name
            :param graph_protocol: (Optional) The graph protocol, default is `DSE_GRAPH_QUERY_PROTOCOL`.
            """

            if graph_protocol == GraphProtocol.GRAPHSON_2_0:
                row_factory = dse_graphson2_traversal_row_factory
            elif graph_protocol == GraphProtocol.GRAPHSON_3_0:
                row_factory = dse_graphson3_traversal_row_factory
            else:
                raise ValueError('Unknown graph protocol: {}'.format(graph_protocol))

            ep = GraphExecutionProfile(row_factory=row_factory,
                                       graph_options=GraphOptions(graph_name=graph_name,
                                                                  graph_language=DseGraph.DSE_GRAPH_QUERY_LANGUAGE,
                                                                  graph_protocol=graph_protocol),
                                       **kwargs)
            return ep

        @staticmethod
        def batch(*args, **kwargs):
            """
            Returns the :class:`cassandra.datastax.graph.fluent.query.TraversalBatch` object allowing to
            execute multiple traversals in the same transaction.
            """
            return _DefaultTraversalBatch(*args, **kwargs)
