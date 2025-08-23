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

import sys
import logging
import inspect
from packaging.version import Version
import ipaddress
from uuid import UUID
from decimal import Decimal
import datetime

from cassandra.util import Point, LineString, Polygon, Duration

from cassandra.cluster import EXEC_PROFILE_GRAPH_DEFAULT, EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT
from cassandra.cluster import GraphAnalyticsExecutionProfile, GraphExecutionProfile, EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT, \
    default_lbp_factory
from cassandra.policies import DSELoadBalancingPolicy

from cassandra.graph import GraphSON1Deserializer
from cassandra.graph.graphson import InetTypeIO, GraphSON2Deserializer, GraphSON3Deserializer
from cassandra.graph import Edge, Vertex, Path
from cassandra.graph.query import GraphOptions, GraphProtocol, graph_graphson2_row_factory, \
    graph_graphson3_row_factory

from tests.integration import DSE_VERSION
from tests.integration.advanced import *


def setup_module():
    if DSE_VERSION:
        dse_options = {'graph': {'realtime_evaluation_timeout_in_seconds': 60}}
        use_single_node_with_graph(dse_options=dse_options)


log = logging.getLogger(__name__)

MAX_LONG = 9223372036854775807
MIN_LONG = -9223372036854775808
ZERO_LONG = 0

MAKE_STRICT = "schema.config().option('graph.schema_mode').set('production')"
MAKE_NON_STRICT = "schema.config().option('graph.schema_mode').set('development')"
ALLOW_SCANS = "schema.config().option('graph.allow_scan').set('true')"

deserializer_plus_to_ipaddressv4 = lambda x: ipaddress.IPv4Address(GraphSON1Deserializer.deserialize_inet(x))
deserializer_plus_to_ipaddressv6 = lambda x: ipaddress.IPv6Address(GraphSON1Deserializer.deserialize_inet(x))


def generic_ip_deserializer(string_ip_address):
    if ":" in string_ip_address:
        return deserializer_plus_to_ipaddressv6(string_ip_address)
    return deserializer_plus_to_ipaddressv4(string_ip_address)


class GenericIpAddressIO(InetTypeIO):
    @classmethod
    def deserialize(cls, value, reader=None):
        return generic_ip_deserializer(value)

GraphSON2Deserializer._deserializers[GenericIpAddressIO.graphson_type] = GenericIpAddressIO
GraphSON3Deserializer._deserializers[GenericIpAddressIO.graphson_type] = GenericIpAddressIO

if DSE_VERSION:
    if DSE_VERSION >= Version('6.8.0'):
        CREATE_CLASSIC_GRAPH = "system.graph(name).engine(Classic).create()"
    else:
        CREATE_CLASSIC_GRAPH = "system.graph(name).create()"


def reset_graph(session, graph_name):
    ks = list(session.execute(
        "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{}';".format(graph_name)))
    if ks:
        try:
            session.execute_graph('system.graph(name).drop()', {'name': graph_name},
                                  execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)
        except:
            pass

    session.execute_graph(CREATE_CLASSIC_GRAPH, {'name': graph_name},
                          execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)
    wait_for_graph_inserted(session, graph_name)


def wait_for_graph_inserted(session, graph_name):
    count = 0
    exists = session.execute_graph('system.graph(name).exists()', {'name': graph_name},
                                   execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)[0].value
    while not exists and count < 50:
        time.sleep(1)
        exists = session.execute_graph('system.graph(name).exists()', {'name': graph_name},
                                       execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)[0].value
    return exists


class BasicGraphUnitTestCase(BasicKeyspaceUnitTestCase):
    """
    This is basic graph unit test case that provides various utility methods that can be leveraged for testcase setup and tear
    down
    """

    @property
    def graph_name(self):
        return self._testMethodName.lower()

    def session_setup(self):
        lbp = DSELoadBalancingPolicy(default_lbp_factory())

        ep_graphson2 = GraphExecutionProfile(
            request_timeout=60,
            load_balancing_policy=lbp,
            graph_options=GraphOptions(
                graph_name=self.graph_name,
                graph_protocol=GraphProtocol.GRAPHSON_2_0
            ),
            row_factory=graph_graphson2_row_factory)

        ep_graphson3 = GraphExecutionProfile(
            request_timeout=60,
            load_balancing_policy=lbp,
            graph_options=GraphOptions(
                graph_name=self.graph_name,
                graph_protocol=GraphProtocol.GRAPHSON_3_0
            ),
            row_factory=graph_graphson3_row_factory)

        ep_graphson1 = GraphExecutionProfile(
            request_timeout=60,
            load_balancing_policy=lbp,
            graph_options=GraphOptions(
                graph_name=self.graph_name
            )
        )

        ep_analytics = GraphAnalyticsExecutionProfile(
            request_timeout=60,
            load_balancing_policy=lbp,
            graph_options=GraphOptions(
                graph_source=b'a',
                graph_language=b'gremlin-groovy',
                graph_name=self.graph_name
            )
        )

        self.cluster = TestCluster(execution_profiles={
            EXEC_PROFILE_GRAPH_DEFAULT: ep_graphson1,
            EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT: ep_analytics,
            "graphson1": ep_graphson1,
            "graphson2": ep_graphson2,
            "graphson3": ep_graphson3
        })

        self.session = self.cluster.connect()
        self.ks_name = self._testMethodName.lower()
        self.cass_version, self.cql_version = get_server_versions()

    def setUp(self):
        self.session_setup()
        self.reset_graph()
        self.clear_schema()
        # enable dev and scan modes
        self.session.execute_graph(MAKE_NON_STRICT)
        self.session.execute_graph(ALLOW_SCANS)

    def tearDown(self):
        self.cluster.shutdown()

    def clear_schema(self):
        self.session.execute_graph("""
            schema.clear();
        """)

    def reset_graph(self):
        reset_graph(self.session, self.graph_name)

    def wait_for_graph_inserted(self):
        wait_for_graph_inserted(self.session, self.graph_name)

    def _execute(self, query, graphson, params=None, execution_profile_options=None, **kwargs):
        queries = query if isinstance(query, list) else [query]
        ep = self.get_execution_profile(graphson)
        if execution_profile_options:
            ep = self.session.execution_profile_clone_update(ep, **execution_profile_options)

        results = []
        for query in queries:
            log.debug(query)
            rf = self.session.execute_graph_async(query, parameters=params, execution_profile=ep, **kwargs)
            results.append(rf.result())
            self.assertEqual(rf.message.custom_payload['graph-results'], graphson)

        return results[0] if len(results) == 1 else results

    def get_execution_profile(self, graphson, traversal=False):
        ep = 'graphson1'
        if graphson == GraphProtocol.GRAPHSON_2_0:
            ep = 'graphson2'
        elif graphson == GraphProtocol.GRAPHSON_3_0:
            ep = 'graphson3'

        return ep if traversal is False else 'traversal_' + ep

    def resultset_to_list(self, rs):
        results_list = []
        for result in rs:
            try:
                results_list.append(result.value)
            except:
                results_list.append(result)

        return results_list


class GraphUnitTestCase(BasicKeyspaceUnitTestCase):

    @property
    def graph_name(self):
        return self._testMethodName.lower()

    def session_setup(self):
        lbp = DSELoadBalancingPolicy(default_lbp_factory())

        ep_graphson2 = GraphExecutionProfile(
            request_timeout=60,
            load_balancing_policy=lbp,
            graph_options=GraphOptions(
                graph_name=self.graph_name,
                graph_protocol=GraphProtocol.GRAPHSON_2_0
            ),
            row_factory=graph_graphson2_row_factory)

        ep_graphson3 = GraphExecutionProfile(
            request_timeout=60,
            load_balancing_policy=lbp,
            graph_options=GraphOptions(
                graph_name=self.graph_name,
                graph_protocol=GraphProtocol.GRAPHSON_3_0
            ),
            row_factory=graph_graphson3_row_factory)

        ep_graphson1 = GraphExecutionProfile(
            request_timeout=60,
            load_balancing_policy=lbp,
            graph_options=GraphOptions(
                graph_name=self.graph_name,
                graph_language='gremlin-groovy'
            )
        )

        ep_analytics = GraphAnalyticsExecutionProfile(
            request_timeout=60,
            load_balancing_policy=lbp,
            graph_options=GraphOptions(
                graph_source=b'a',
                graph_language=b'gremlin-groovy',
                graph_name=self.graph_name
            )
        )

        self.cluster = TestCluster(execution_profiles={
            EXEC_PROFILE_GRAPH_DEFAULT: ep_graphson1,
            EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT: ep_analytics,
            "graphson1": ep_graphson1,
            "graphson2": ep_graphson2,
            "graphson3": ep_graphson3
        })

        self.session = self.cluster.connect()
        self.ks_name = self._testMethodName.lower()
        self.cass_version, self.cql_version = get_server_versions()

    def setUp(self):
        """basic setup only"""
        self.session_setup()

    def setup_graph(self, schema):
        """Config dependant setup"""
        schema.drop_graph(self.session, self.graph_name)
        schema.create_graph(self.session, self.graph_name)
        schema.clear(self.session)
        if schema is ClassicGraphSchema:
            # enable dev and scan modes
            self.session.execute_graph(MAKE_NON_STRICT)
            self.session.execute_graph(ALLOW_SCANS)

    def teardown_graph(self, schema):
        schema.drop_graph(self.session, self.graph_name)

    def tearDown(self):
        self.cluster.shutdown()

    def execute_graph_queries(self, queries, params=None, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT,
                              verify_graphson=False, **kwargs):
        results = []
        for query in queries:
            log.debug(query)
            rf = self.session.execute_graph_async(query, parameters=params,
                                                  execution_profile=execution_profile, **kwargs)
            if verify_graphson:
                self.assertEqual(rf.message.custom_payload['graph-results'], verify_graphson)
            results.append(rf.result())

        return results

    def execute_graph(self, query, graphson, params=None, execution_profile_options=None, traversal=False, **kwargs):
        queries = query if isinstance(query, list) else [query]
        ep = self.get_execution_profile(graphson)
        if traversal:
            ep = 'traversal_' + ep
        if execution_profile_options:
            ep = self.session.execution_profile_clone_update(ep, **execution_profile_options)

        results = self.execute_graph_queries(queries, params, ep, verify_graphson=graphson, **kwargs)

        return results[0] if len(results) == 1 else results

    def get_execution_profile(self, graphson, traversal=False):
        ep = 'graphson1'
        if graphson == GraphProtocol.GRAPHSON_2_0:
            ep = 'graphson2'
        elif graphson == GraphProtocol.GRAPHSON_3_0:
            ep = 'graphson3'

        return ep if traversal is False else 'traversal_' + ep

    def resultset_to_list(self, rs):
        results_list = []
        for result in rs:
            try:
                results_list.append(result.value)
            except:
                results_list.append(result)

        return results_list


class BasicSharedGraphUnitTestCase(BasicKeyspaceUnitTestCase):
    """
    This is basic graph unit test case that provides various utility methods that can be leveraged for testcase setup and tear
    down
    """

    @classmethod
    def session_setup(cls):
        cls.cluster = TestCluster()
        cls.session = cls.cluster.connect()
        cls.ks_name = cls.__name__.lower()
        cls.cass_version, cls.cql_version = get_server_versions()
        cls.graph_name = cls.__name__.lower()

    @classmethod
    def setUpClass(cls):
        if DSE_VERSION:
            cls.session_setup()
            cls.reset_graph()
            profiles = cls.cluster.profile_manager.profiles
            profiles[EXEC_PROFILE_GRAPH_DEFAULT].request_timeout = 60
            profiles[EXEC_PROFILE_GRAPH_DEFAULT].graph_options.graph_name = cls.graph_name
            profiles[EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT].request_timeout = 60
            profiles[EXEC_PROFILE_GRAPH_ANALYTICS_DEFAULT].graph_options.graph_name = cls.graph_name

    @classmethod
    def tearDownClass(cls):
        if DSE_VERSION:
            cls.cluster.shutdown()

    @classmethod
    def clear_schema(self):
        self.session.execute_graph('schema.clear()')

    @classmethod
    def reset_graph(self):
        reset_graph(self.session, self.graph_name)

    def wait_for_graph_inserted(self):
        wait_for_graph_inserted(self.session, self.graph_name)


class GraphFixtures(object):

    @staticmethod
    def line(length, single_script=True):
        raise NotImplementedError()

    @staticmethod
    def classic():
        raise NotImplementedError()

    @staticmethod
    def multiple_fields():
        raise NotImplementedError()

    @staticmethod
    def large():
        raise NotImplementedError()


class ClassicGraphFixtures(GraphFixtures):

    @staticmethod
    def datatypes():
        data = {
            "boolean1": ["Boolean()", True, None],
            "boolean2": ["Boolean()", False, None],
            "point1": ["Point()", Point(.5, .13), GraphSON1Deserializer.deserialize_point],
             "point2": ["Point()", Point(-5, .0), GraphSON1Deserializer.deserialize_point],

            "linestring1": ["Linestring()", LineString(((1.0, 2.0), (3.0, 4.0), (-89.0, 90.0))),
                                GraphSON1Deserializer.deserialize_linestring],
            "polygon1": ["Polygon()", Polygon([(10.0, 10.0), (80.0, 10.0), (80., 88.0), (10., 89.0), (10., 10.0)],
                                              [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)],
                                               [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]]),
                                GraphSON1Deserializer.deserialize_polygon],
            "int1": ["Int()", 2, GraphSON1Deserializer.deserialize_int],
            "smallint1": ["Smallint()", 1, GraphSON1Deserializer.deserialize_smallint],
            "bigint1": ["Bigint()", MAX_LONG, GraphSON1Deserializer.deserialize_bigint],
            "bigint2": ["Bigint()", MIN_LONG, GraphSON1Deserializer.deserialize_bigint],
            "bigint3": ["Bigint()", ZERO_LONG, GraphSON1Deserializer.deserialize_bigint],
            "varint1": ["Varint()", 2147483647, GraphSON1Deserializer.deserialize_varint],
            "int1": ["Int()", 100, GraphSON1Deserializer.deserialize_int],
            "float1": ["Float()", 0.3415681, GraphSON1Deserializer.deserialize_float],
            "double1": ["Double()", 0.34156811237335205, GraphSON1Deserializer.deserialize_double],
            "uuid1": ["Uuid()", UUID('12345678123456781234567812345678'), GraphSON1Deserializer.deserialize_uuid],
            "decimal1": ["Decimal()", Decimal(10), GraphSON1Deserializer.deserialize_decimal],
            "blob1": ["Blob()",  bytearray(b"Hello World"), GraphSON1Deserializer.deserialize_blob],

            "timestamp1": ["Timestamp()", datetime.datetime.utcnow().replace(microsecond=0),
                                GraphSON1Deserializer.deserialize_timestamp],
            "timestamp2": ["Timestamp()", datetime.datetime.max.replace(microsecond=0),
                                GraphSON1Deserializer.deserialize_timestamp],
            # These are valid values but are pending for DSP-14093 to be fixed
            #"timestamp3": ["Timestamp()", datetime.datetime(159, 1, 1, 23, 59, 59),
            #                                 GraphSON1TypeDeserializer.deserialize_timestamp],
            #"timestamp4": ["Timestamp()", datetime.datetime.min,
            #                                            GraphSON1TypeDeserializer.deserialize_timestamp],
            "inet1": ["Inet()", ipaddress.IPv4Address(u"127.0.0.1"), deserializer_plus_to_ipaddressv4],
            "inet2": ["Inet()", ipaddress.IPv6Address(u"2001:db8:85a3:8d3:1319:8a2e:370:7348"),
                      deserializer_plus_to_ipaddressv6],
            "duration1": ["Duration()", datetime.timedelta(1, 16, 0),
                          GraphSON1Deserializer.deserialize_duration],
            "duration2": ["Duration()", datetime.timedelta(days=1, seconds=16, milliseconds=15),
                          GraphSON1Deserializer.deserialize_duration],
            "blob3": ["Blob()", bytes(b"Hello World Again"), GraphSON1Deserializer.deserialize_blob],
            "blob4": ["Blob()", memoryview(b"And Again Hello World"), GraphSON1Deserializer.deserialize_blob]
            }

        if DSE_VERSION >= Version("5.1"):
            data["time1"] = ["Time()", datetime.time(12, 6, 12, 444), GraphSON1Deserializer.deserialize_time]
            data["time2"] = ["Time()", datetime.time(12, 6, 12), GraphSON1Deserializer.deserialize_time]
            data["time3"] = ["Time()", datetime.time(12, 6), GraphSON1Deserializer.deserialize_time]
            data["time4"] = ["Time()", datetime.time.min, GraphSON1Deserializer.deserialize_time]
            data["time5"] = ["Time()", datetime.time.max, GraphSON1Deserializer.deserialize_time]
            data["blob5"] = ["Blob()", bytearray(b"AKDLIElksadlaswqA" * 10000), GraphSON1Deserializer.deserialize_blob]
            data["datetime1"] = ["Date()", datetime.date.today(), GraphSON1Deserializer.deserialize_date]
            data["datetime2"] = ["Date()", datetime.date(159, 1, 3), GraphSON1Deserializer.deserialize_date]
            data["datetime3"] = ["Date()", datetime.date.min, GraphSON1Deserializer.deserialize_date]
            data["datetime4"] = ["Date()", datetime.date.max, GraphSON1Deserializer.deserialize_date]
            data["time1"] = ["Time()", datetime.time(12, 6, 12, 444), GraphSON1Deserializer.deserialize_time]
            data["time2"] = ["Time()", datetime.time(12, 6, 12), GraphSON1Deserializer.deserialize_time]
            data["time3"] = ["Time()", datetime.time(12, 6), GraphSON1Deserializer.deserialize_time]
            data["time4"] = ["Time()", datetime.time.min, GraphSON1Deserializer.deserialize_time]
            data["time5"] = ["Time()", datetime.time.max, GraphSON1Deserializer.deserialize_time]

        return data

    @staticmethod
    def line(length, single_script=False):
        queries = [ALLOW_SCANS + ';',
                   """schema.propertyKey('index').Int().ifNotExists().create();
                   schema.propertyKey('distance').Int().ifNotExists().create();
                   schema.vertexLabel('lp').properties('index').ifNotExists().create();
                   schema.edgeLabel('goesTo').properties('distance').connection('lp', 'lp').ifNotExists().create();"""]

        vertex_script = ["Vertex vertex0 = graph.addVertex(label, 'lp', 'index', 0);"]
        for index in range(1, length):
            if not single_script and len(vertex_script) > 25:
                queries.append("\n".join(vertex_script))
                vertex_script = [
                    "Vertex vertex{pindex} = g.V().hasLabel('lp').has('index', {pindex}).next()".format(
                        pindex=index-1)]

            vertex_script.append('''
                Vertex vertex{vindex} = graph.addVertex(label, 'lp', 'index', {vindex});
                vertex{pindex}.addEdge('goesTo', vertex{vindex}, 'distance', 5); '''.format(
                vindex=index, pindex=index - 1))

        queries.append("\n".join(vertex_script))
        return queries

    @staticmethod
    def classic():
        queries = [ALLOW_SCANS,
            '''schema.propertyKey('name').Text().ifNotExists().create();
            schema.propertyKey('age').Int().ifNotExists().create();
            schema.propertyKey('lang').Text().ifNotExists().create();
            schema.propertyKey('weight').Float().ifNotExists().create();
            schema.vertexLabel('person').properties('name', 'age').ifNotExists().create();
            schema.vertexLabel('software').properties('name', 'lang').ifNotExists().create();
            schema.edgeLabel('created').properties('weight').connection('person', 'software').ifNotExists().create();
            schema.edgeLabel('created').connection('software', 'software').add();
            schema.edgeLabel('knows').properties('weight').connection('person', 'person').ifNotExists().create();''',

            '''Vertex marko = graph.addVertex(label, 'person', 'name', 'marko', 'age', 29);
            Vertex vadas = graph.addVertex(label, 'person', 'name', 'vadas', 'age', 27);
            Vertex lop = graph.addVertex(label, 'software', 'name', 'lop', 'lang', 'java');
            Vertex josh = graph.addVertex(label, 'person', 'name', 'josh', 'age', 32);
            Vertex ripple = graph.addVertex(label, 'software', 'name', 'ripple', 'lang', 'java');
            Vertex peter = graph.addVertex(label, 'person', 'name', 'peter', 'age', 35);
            Vertex carl = graph.addVertex(label, 'person', 'name', 'carl', 'age', 35);
            marko.addEdge('knows', vadas, 'weight', 0.5f);
            marko.addEdge('knows', josh, 'weight', 1.0f);
            marko.addEdge('created', lop, 'weight', 0.4f);
            josh.addEdge('created', ripple, 'weight', 1.0f);
            josh.addEdge('created', lop, 'weight', 0.4f);
            peter.addEdge('created', lop, 'weight', 0.2f);''']

        return "\n".join(queries)

    @staticmethod
    def multiple_fields():
        query_params = {}
        queries= [ALLOW_SCANS,
                  '''schema.propertyKey('shortvalue').Smallint().ifNotExists().create();
                  schema.vertexLabel('shortvertex').properties('shortvalue').ifNotExists().create();
                  short s1 = 5000; graph.addVertex(label, "shortvertex", "shortvalue", s1);
                  schema.propertyKey('intvalue').Int().ifNotExists().create();
                  schema.vertexLabel('intvertex').properties('intvalue').ifNotExists().create();
                  int i1 = 1000000000; graph.addVertex(label, "intvertex", "intvalue", i1);
                  schema.propertyKey('intvalue2').Int().ifNotExists().create();
                  schema.vertexLabel('intvertex2').properties('intvalue2').ifNotExists().create();
                  Integer i2 = 100000000; graph.addVertex(label, "intvertex2", "intvalue2", i2);
                  schema.propertyKey('longvalue').Bigint().ifNotExists().create();
                  schema.vertexLabel('longvertex').properties('longvalue').ifNotExists().create();
                  long l1 = 9223372036854775807; graph.addVertex(label, "longvertex", "longvalue", l1);
                  schema.propertyKey('longvalue2').Bigint().ifNotExists().create();
                  schema.vertexLabel('longvertex2').properties('longvalue2').ifNotExists().create();
                  Long l2 = 100000000000000000L; graph.addVertex(label, "longvertex2", "longvalue2", l2);
                  schema.propertyKey('floatvalue').Float().ifNotExists().create();
                  schema.vertexLabel('floatvertex').properties('floatvalue').ifNotExists().create();
                  float f1 = 3.5f; graph.addVertex(label, "floatvertex", "floatvalue", f1);
                  schema.propertyKey('doublevalue').Double().ifNotExists().create();
                  schema.vertexLabel('doublevertex').properties('doublevalue').ifNotExists().create();
                  double d1 = 3.5e40; graph.addVertex(label, "doublevertex", "doublevalue", d1);
                  schema.propertyKey('doublevalue2').Double().ifNotExists().create();
                  schema.vertexLabel('doublevertex2').properties('doublevalue2').ifNotExists().create();
                  Double d2 = 3.5e40d; graph.addVertex(label, "doublevertex2", "doublevalue2", d2);''']

        if DSE_VERSION >= Version('5.1'):
            queries.append('''schema.propertyKey('datevalue1').Date().ifNotExists().create();
                         schema.vertexLabel('datevertex1').properties('datevalue1').ifNotExists().create();
                         schema.propertyKey('negdatevalue2').Date().ifNotExists().create();
                        schema.vertexLabel('negdatevertex2').properties('negdatevalue2').ifNotExists().create();''')

            for i in range(1, 4):
                queries.append('''schema.propertyKey('timevalue{0}').Time().ifNotExists().create();
                         schema.vertexLabel('timevertex{0}').properties('timevalue{0}').ifNotExists().create();'''.format(
                    i))

                queries.append('graph.addVertex(label, "datevertex1", "datevalue1", date1);')
            query_params['date1'] = '1999-07-29'

            queries.append('graph.addVertex(label, "negdatevertex2", "negdatevalue2", date2);')
            query_params['date2'] = '-1999-07-28'

            queries.append('graph.addVertex(label, "timevertex1", "timevalue1", time1);')
            query_params['time1'] = '14:02'
            queries.append('graph.addVertex(label, "timevertex2", "timevalue2", time2);')
            query_params['time2'] = '14:02:20'
            queries.append('graph.addVertex(label, "timevertex3", "timevalue3", time3);')
            query_params['time3'] = '14:02:20.222'

        return queries, query_params

    @staticmethod
    def large():
        query_parts = ['''
                int size = 2000;
                List ids = new ArrayList();
                schema.propertyKey('ts').Int().single().ifNotExists().create();
                schema.propertyKey('sin').Int().single().ifNotExists().create();
                schema.propertyKey('cos').Int().single().ifNotExists().create();
                schema.propertyKey('ii').Int().single().ifNotExists().create();
                schema.vertexLabel('lcg').properties('ts', 'sin', 'cos', 'ii').ifNotExists().create();
                schema.edgeLabel('linked').connection('lcg', 'lcg').ifNotExists().create();
                Vertex v = graph.addVertex(label, 'lcg');
                v.property("ts", 100001);
                v.property("sin", 0);
                v.property("cos", 1);
                v.property("ii", 0);
                ids.add(v.id());
                Random rand = new Random();
                for (int ii = 1; ii < size; ii++) {
                    v = graph.addVertex(label, 'lcg');
                    v.property("ii", ii);
                    v.property("ts", 100001 + ii);
                    v.property("sin", Math.sin(ii/5.0));
                    v.property("cos", Math.cos(ii/5.0));
                    Vertex u = g.V(ids.get(rand.nextInt(ids.size()))).next();
                    v.addEdge("linked", u);
                    ids.add(v.id());
                }
                g.V().count();''']

        return "\n".join(query_parts)

    @staticmethod
    def address_book():
        p1 = "Point()"
        p2 = "Point()"
        if DSE_VERSION >= Version('5.1'):
            p1 = "Point().withBounds(-100, -100, 100, 100)"
            p2 = "Point().withGeoBounds()"

        queries = [
            ALLOW_SCANS,
            "schema.propertyKey('name').Text().ifNotExists().create()",
            "schema.propertyKey('pointPropWithBoundsWithSearchIndex').{}.ifNotExists().create()".format(p1),
            "schema.propertyKey('pointPropWithBounds').{}.ifNotExists().create()".format(p1),
            "schema.propertyKey('pointPropWithGeoBoundsWithSearchIndex').{}.ifNotExists().create()".format(p2),
            "schema.propertyKey('pointPropWithGeoBounds').{}.ifNotExists().create()".format(p2),
            "schema.propertyKey('city').Text().ifNotExists().create()",
            "schema.propertyKey('state').Text().ifNotExists().create()",
            "schema.propertyKey('description').Text().ifNotExists().create()",
            "schema.vertexLabel('person').properties('name', 'city', 'state', 'description', 'pointPropWithBoundsWithSearchIndex', 'pointPropWithBounds', 'pointPropWithGeoBoundsWithSearchIndex', 'pointPropWithGeoBounds').ifNotExists().create()",
            "schema.vertexLabel('person').index('searchPointWithBounds').secondary().by('pointPropWithBounds').ifNotExists().add()",
            "schema.vertexLabel('person').index('searchPointWithGeoBounds').secondary().by('pointPropWithGeoBounds').ifNotExists().add()",

            "g.addV('person').property('name', 'Paul Thomas Joe').property('city', 'Rochester').property('state', 'MN').property('pointPropWithBoundsWithSearchIndex', Geo.point(-92.46295, 44.0234)).property('pointPropWithBounds', Geo.point(-92.46295, 44.0234)).property('pointPropWithGeoBoundsWithSearchIndex', Geo.point(-92.46295, 44.0234)).property('pointPropWithGeoBounds', Geo.point(-92.46295, 44.0234)).property('description', 'Lives by the hospital').next()",
            "g.addV('person').property('name', 'George Bill Steve').property('city', 'Minneapolis').property('state', 'MN').property('pointPropWithBoundsWithSearchIndex', Geo.point(-93.266667, 44.093333)).property('pointPropWithBounds', Geo.point(-93.266667, 44.093333)).property('pointPropWithGeoBoundsWithSearchIndex', Geo.point(-93.266667, 44.093333)).property('pointPropWithGeoBounds', Geo.point(-93.266667, 44.093333)).property('description', 'A cold dude').next()",
            "g.addV('person').property('name', 'James Paul Smith').property('city', 'Chicago').property('state', 'IL').property('pointPropWithBoundsWithSearchIndex', Geo.point(-87.684722, 41.836944)).property('description', 'Likes to hang out').next()",
            "g.addV('person').property('name', 'Jill Alice').property('city', 'Atlanta').property('state', 'GA').property('pointPropWithBoundsWithSearchIndex', Geo.point(-84.39, 33.755)).property('description', 'Enjoys a nice cold coca cola').next()"
        ]

        if not Version('5.0') <= DSE_VERSION < Version('5.1'):
            queries.append("schema.vertexLabel('person').index('search').search().by('pointPropWithBoundsWithSearchIndex').withError(0.00001, 0.0).by('pointPropWithGeoBoundsWithSearchIndex').withError(0.00001, 0.0).ifNotExists().add()")

        return "\n".join(queries)


class CoreGraphFixtures(GraphFixtures):

    @staticmethod
    def datatypes():
        data = ClassicGraphFixtures.datatypes()
        del data['duration1']
        del data['duration2']

        # Core Graphs only types
        data["map1"] = ["mapOf(Text, Text)", {'test': 'test'}, None]
        data["map2"] = ["mapOf(Text, Point)", {'test': Point(.5, .13)}, None]
        data["map3"] = ["frozen(mapOf(Int, Varchar))", {42: 'test'}, None]

        data["list1"] = ["listOf(Text)", ['test', 'hello', 'world'], None]
        data["list2"] = ["listOf(Int)", [42, 632, 32], None]
        data["list3"] = ["listOf(Point)", [Point(.5, .13), Point(42.5, .13)], None]
        data["list4"] = ["frozen(listOf(Int))", [42, 55, 33], None]

        data["set1"] = ["setOf(Text)", {'test', 'hello', 'world'}, None]
        data["set2"] = ["setOf(Int)", {42, 632, 32}, None]
        data["set3"] = ["setOf(Point)", {Point(.5, .13), Point(42.5, .13)}, None]
        data["set4"] = ["frozen(setOf(Int))", {42, 55, 33}, None]

        data["tuple1"] = ["tupleOf(Int, Text)", (42, "world"), None]
        data["tuple2"] = ["tupleOf(Int, tupleOf(Text, tupleOf(Text, Point)))", (42, ("world", ('this', Point(.5, .13)))), None]
        data["tuple3"] = ["tupleOf(Int, tupleOf(Text, frozen(mapOf(Text, Text))))", (42, ("world", {'test': 'test'})), None]
        data["tuple4"] = ["tupleOf(Int, tupleOf(Text, frozen(listOf(Int))))", (42, ("world", [65, 89])), None]
        data["tuple5"] = ["tupleOf(Int, tupleOf(Text, frozen(setOf(Int))))", (42, ("world", {65, 55})), None]
        data["tuple6"] = ["tupleOf(Int, tupleOf(Text, tupleOf(Text, LineString)))",
                          (42, ("world", ('this', LineString(((1.0, 2.0), (3.0, 4.0), (-89.0, 90.0)))))), None]

        data["tuple7"] = ["tupleOf(Int, tupleOf(Text, tupleOf(Text, Polygon)))",
                          (42, ("world", ('this', Polygon([(10.0, 10.0), (80.0, 10.0), (80., 88.0), (10., 89.0), (10., 10.0)],
                                              [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)],
                                               [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]])))), None]
        data["dse_duration1"] = ["Duration()", Duration(42, 12, 10303312), None]
        data["dse_duration2"] = ["Duration()", Duration(50, 32, 11), None]

        return data

    @staticmethod
    def line(length, single_script=False):
        queries = ["""
        schema.vertexLabel('lp').ifNotExists().partitionBy('index', Int).create();
        schema.edgeLabel('goesTo').ifNotExists().from('lp').to('lp').property('distance', Int).create();
        """]

        vertex_script = ["g.addV('lp').property('index', 0).next();"]
        for index in range(1, length):
            if not single_script and len(vertex_script) > 25:
                queries.append("\n".join(vertex_script))
                vertex_script = []

            vertex_script.append('''
                g.addV('lp').property('index', {index}).next();
                g.V().hasLabel('lp').has('index', {pindex}).as('pp').V().hasLabel('lp').has('index', {index}).as('p').
                   addE('goesTo').from('pp').to('p').property('distance', 5).next();
                '''.format(
                index=index, pindex=index - 1))

        queries.append("\n".join(vertex_script))
        return queries

    @staticmethod
    def classic():
        queries = [
            '''
            schema.vertexLabel('person').ifNotExists().partitionBy('name', Text).property('age', Int).create();
            schema.vertexLabel('software')ifNotExists().partitionBy('name', Text).property('lang', Text).create();
            schema.edgeLabel('created').ifNotExists().from('person').to('software').property('weight', Double).create();
            schema.edgeLabel('knows').ifNotExists().from('person').to('person').property('weight', Double).create();
            ''',

            '''
            Vertex marko = g.addV('person').property('name', 'marko').property('age', 29).next();
            Vertex vadas = g.addV('person').property('name', 'vadas').property('age', 27).next();
            Vertex lop = g.addV('software').property('name', 'lop').property('lang', 'java').next();
            Vertex josh = g.addV('person').property('name', 'josh').property('age', 32).next();
            Vertex peter = g.addV('person').property('name', 'peter').property('age', 35).next();
            Vertex carl = g.addV('person').property('name', 'carl').property('age', 35).next();
            Vertex ripple = g.addV('software').property('name', 'ripple').property('lang', 'java').next();

            // TODO, switch to VertexReference and use v.id()
            g.V().hasLabel('person').has('name', 'vadas').as('v').V().hasLabel('person').has('name', 'marko').as('m').addE('knows').from('m').to('v').property('weight', 0.5d).next();
            g.V().hasLabel('person').has('name', 'josh').as('j').V().hasLabel('person').has('name', 'marko').as('m').addE('knows').from('m').to('j').property('weight', 1.0d).next();
            g.V().hasLabel('software').has('name', 'lop').as('l').V().hasLabel('person').has('name', 'marko').as('m').addE('created').from('m').to('l').property('weight', 0.4d).next();
            g.V().hasLabel('software').has('name', 'ripple').as('r').V().hasLabel('person').has('name', 'josh').as('j').addE('created').from('j').to('r').property('weight', 1.0d).next();
            g.V().hasLabel('software').has('name', 'lop').as('l').V().hasLabel('person').has('name', 'josh').as('j').addE('created').from('j').to('l').property('weight', 0.4d).next();
            g.V().hasLabel('software').has('name', 'lop').as('l').V().hasLabel('person').has('name', 'peter').as('p').addE('created').from('p').to('l').property('weight', 0.2d).next();

            ''']

        return queries

    @staticmethod
    def multiple_fields():
        ## no generic test currently needs this
        raise NotImplementedError()

    @staticmethod
    def large():
        query_parts = [
            '''
            schema.vertexLabel('lcg').ifNotExists().partitionBy('ts', Int).property('sin', Double).
                property('cos', Double).property('ii', Int).create();
            schema.edgeLabel('linked').ifNotExists().from('lcg').to('lcg').create();
            ''',

            '''
            int size = 2000;
            List ids = new ArrayList();
            v = g.addV('lcg').property('ts', 100001).property('sin', 0d).property('cos', 1d).property('ii', 0).next();
            ids.add(v.id());
            Random rand = new Random();
            for (int ii = 1; ii < size; ii++) {
                v = g.addV('lcg').property('ts', 100001 + ii).property('sin', Math.sin(ii/5.0)).property('cos', Math.cos(ii/5.0)).property('ii', ii).next();

                uid = ids.get(rand.nextInt(ids.size()))
                g.V(v.id()).as('v').V(uid).as('u').addE('linked').from('v').to('u').next();
                ids.add(v.id());
            }
            g.V().count();'''
        ]

        return query_parts

    @staticmethod
    def address_book():
        queries = [
            "schema.vertexLabel('person').ifNotExists().partitionBy('name', Text)."
            "property('pointPropWithBoundsWithSearchIndex', Point)."
            "property('pointPropWithBounds', Point)."
            "property('pointPropWithGeoBoundsWithSearchIndex', Point)."
            "property('pointPropWithGeoBounds', Point)."
            "property('city', Text)."
            "property('state', Text)."
            "property('description', Text).create()",
            "schema.vertexLabel('person').searchIndex().by('name').by('pointPropWithBounds').by('pointPropWithGeoBounds').by('description').asText().create()",
            "g.addV('person').property('name', 'Paul Thomas Joe').property('city', 'Rochester').property('state', 'MN').property('pointPropWithBoundsWithSearchIndex', Geo.point(-92.46295, 44.0234)).property('pointPropWithBounds', Geo.point(-92.46295, 44.0234)).property('pointPropWithGeoBoundsWithSearchIndex', Geo.point(-92.46295, 44.0234)).property('pointPropWithGeoBounds', Geo.point(-92.46295, 44.0234)).property('description', 'Lives by the hospital').next()",
            "g.addV('person').property('name', 'George Bill Steve').property('city', 'Minneapolis').property('state', 'MN').property('pointPropWithBoundsWithSearchIndex', Geo.point(-93.266667, 44.093333)).property('pointPropWithBounds', Geo.point(-93.266667, 44.093333)).property('pointPropWithGeoBoundsWithSearchIndex', Geo.point(-93.266667, 44.093333)).property('pointPropWithGeoBounds', Geo.point(-93.266667, 44.093333)).property('description', 'A cold dude').next()",
            "g.addV('person').property('name', 'James Paul Smith').property('city', 'Chicago').property('state', 'IL').property('pointPropWithBoundsWithSearchIndex', Geo.point(-87.684722, 41.836944)).property('description', 'Likes to hang out').next()",
            "g.addV('person').property('name', 'Jill Alice').property('city', 'Atlanta').property('state', 'GA').property('pointPropWithBoundsWithSearchIndex', Geo.point(-84.39, 33.755)).property('description', 'Enjoys a nice cold coca cola').next()"
        ]

        if not Version('5.0') <= DSE_VERSION < Version('5.1'):
            queries.append("schema.vertexLabel('person').searchIndex().by('pointPropWithBoundsWithSearchIndex').by('pointPropWithGeoBounds')"
                           ".by('pointPropWithGeoBoundsWithSearchIndex').create()")

        return queries


def validate_classic_vertex(test, vertex):
    vertex_props = vertex.properties.keys()
    test.assertEqual(len(vertex_props), 2)
    test.assertIn('name', vertex_props)
    test.assertTrue('lang' in vertex_props or 'age' in vertex_props)


def validate_classic_vertex_return_type(test, vertex):
    validate_generic_vertex_result_type(vertex)
    vertex_props = vertex.properties
    test.assertIn('name', vertex_props)
    test.assertTrue('lang' in vertex_props or 'age' in vertex_props)


def validate_generic_vertex_result_type(test, vertex):
    test.assertIsInstance(vertex, Vertex)
    for attr in ('id', 'type', 'label', 'properties'):
        test.assertIsNotNone(getattr(vertex, attr))


def validate_classic_edge_properties(test, edge_properties):
    test.assertEqual(len(edge_properties.keys()), 1)
    test.assertIn('weight', edge_properties)
    test.assertIsInstance(edge_properties, dict)


def validate_classic_edge(test, edge):
    validate_generic_edge_result_type(test, edge)
    validate_classic_edge_properties(test, edge.properties)


def validate_line_edge(test, edge):
    validate_generic_edge_result_type(test, edge)
    edge_props = edge.properties
    test.assertEqual(len(edge_props.keys()), 1)
    test.assertIn('distance', edge_props)


def validate_generic_edge_result_type(test, edge):
    test.assertIsInstance(edge, Edge)
    for attr in ('properties', 'outV', 'outVLabel', 'inV', 'inVLabel', 'label', 'type', 'id'):
        test.assertIsNotNone(getattr(edge, attr))


def validate_path_result_type(test, path):
    test.assertIsInstance(path, Path)
    test.assertIsNotNone(path.labels)
    for obj in path.objects:
        if isinstance(obj, Edge):
            validate_classic_edge(test, obj)
        elif isinstance(obj, Vertex):
            validate_classic_vertex(test, obj)
        else:
            test.fail("Invalid object found in path " + str(object.type))


class GraphTestConfiguration(object):
    """Possible Configurations:
        ClassicGraphSchema:
            graphson1
            graphson2
            graphson3

        CoreGraphSchema
            graphson3
    """

    @classmethod
    def schemas(cls):
        schemas = [ClassicGraphSchema]
        if DSE_VERSION >= Version("6.8"):
            schemas.append(CoreGraphSchema)
        return schemas

    @classmethod
    def graphson_versions(cls):
        graphson_versions = [GraphProtocol.GRAPHSON_1_0]
        if DSE_VERSION >= Version("6.0"):
            graphson_versions.append(GraphProtocol.GRAPHSON_2_0)
        if DSE_VERSION >= Version("6.8"):
            graphson_versions.append(GraphProtocol.GRAPHSON_3_0)
        return graphson_versions

    @classmethod
    def schema_configurations(cls, schema=None):
        schemas = cls.schemas() if schema is None else [schema]
        configurations = []
        for s in schemas:
                configurations.append(s)

        return configurations

    @classmethod
    def configurations(cls, schema=None, graphson=None):
        schemas = cls.schemas() if schema is None else [schema]
        graphson_versions = cls.graphson_versions() if graphson is None else [graphson]

        configurations = []
        for s in schemas:
            for g in graphson_versions:
                if s is CoreGraphSchema and g != GraphProtocol.GRAPHSON_3_0:
                    continue
                configurations.append((s, g))

        return configurations

    @staticmethod
    def _make_graph_schema_test_method(func, schema):
        def test_input(self):
            self.setup_graph(schema)
            try:
                func(self, schema)
            except:
                raise
            finally:
                self.teardown_graph(schema)

        schema_name = 'classic' if schema is ClassicGraphSchema else 'core'
        test_input.__name__ = '{func}_{schema}'.format(
            func=func.__name__.lstrip('_'), schema=schema_name)
        return test_input

    @staticmethod
    def _make_graph_test_method(func, schema, graphson):
        def test_input(self):
            self.setup_graph(schema)
            try:
                func(self, schema, graphson)
            except:
                raise
            finally:
                self.teardown_graph(schema)

        graphson_name = 'graphson1'
        if graphson == GraphProtocol.GRAPHSON_2_0:
            graphson_name = 'graphson2'
        elif graphson == GraphProtocol.GRAPHSON_3_0:
            graphson_name = 'graphson3'

        schema_name = 'classic' if schema is ClassicGraphSchema else 'core'

        # avoid keyspace name too long issue
        if DSE_VERSION < Version('6.7'):
            schema_name = schema_name[0]
            graphson_name = 'g' + graphson_name[-1]

        test_input.__name__ = '{func}_{schema}_{graphson}'.format(
            func=func.__name__.lstrip('_'), schema=schema_name, graphson=graphson_name)
        return test_input

    @classmethod
    def generate_tests(cls, schema=None, graphson=None, traversal=False):
        """Generate tests for a graph configuration"""
        def decorator(klass):
            if DSE_VERSION:
                predicate = inspect.isfunction
                for name, func in inspect.getmembers(klass, predicate=predicate):
                    if not name.startswith('_test'):
                        continue
                    for _schema, _graphson in cls.configurations(schema, graphson):
                        if traversal and _graphson == GraphProtocol.GRAPHSON_1_0:
                            continue
                        test_input = cls._make_graph_test_method(func, _schema, _graphson)
                        log.debug("Generated test '{}.{}'".format(klass.__name__, test_input.__name__))
                        setattr(klass, test_input.__name__, test_input)
                return klass

        return decorator

    @classmethod
    def generate_schema_tests(cls, schema=None):
        """Generate schema tests for a graph configuration"""
        def decorator(klass):
            if DSE_VERSION:
                predicate = inspect.isfunction
                for name, func in inspect.getmembers(klass, predicate=predicate):
                    if not name.startswith('_test'):
                        continue
                    for _schema in cls.schema_configurations(schema):
                        test_input = cls._make_graph_schema_test_method(func, _schema)
                        log.debug("Generated test '{}.{}'".format(klass.__name__, test_input.__name__))
                        setattr(klass, test_input.__name__, test_input)
            return klass

        return decorator


class VertexLabel(object):
    """
    Helper that represents a new VertexLabel:

    VertexLabel(['Int()', 'Float()'])  # a vertex with 2 properties named property1 and property2
    VertexLabel([('int1', 'Int()'), 'Float()'])  # a vertex with 2 properties named int1 and property1
    """

    id = 0
    label = None
    properties = None

    def __init__(self, properties):
        VertexLabel.id += 1
        self.id = VertexLabel.id
        self.label = "vertex{}".format(self.id)
        self.properties = {'pkid': self.id}
        property_count = 0
        for p in properties:
            if isinstance(p, tuple):
                name, typ = p
            else:
                property_count += 1
                name = "property-v{}-{}".format(self.id, property_count)
                typ = p
            self.properties[name] = typ

    @property
    def non_pk_properties(self):
        return {p: v for p, v in self.properties.items() if p != 'pkid'}


class GraphSchema(object):

    has_geo_bounds = DSE_VERSION and DSE_VERSION >= Version('5.1')
    fixtures = GraphFixtures

    @classmethod
    def sanitize_type(cls, typ):
        if typ.lower().startswith("point"):
            return cls.sanitize_point_type()
        elif typ.lower().startswith("line"):
            return cls.sanitize_line_type()
        elif typ.lower().startswith("poly"):
            return cls.sanitize_polygon_type()
        else:
            return typ

    @classmethod
    def sanitize_point_type(cls):
        return "Point().withGeoBounds()" if cls.has_geo_bounds else "Point()"

    @classmethod
    def sanitize_line_type(cls):
        return "Linestring().withGeoBounds()" if cls.has_geo_bounds else "Linestring()"

    @classmethod
    def sanitize_polygon_type(cls):
        return "Polygon().withGeoBounds()" if cls.has_geo_bounds else "Polygon()"

    @staticmethod
    def drop_graph(session, graph_name):
        ks = list(session.execute(
            "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{}';".format(graph_name)))
        if not ks:
            return

        try:
            session.execute_graph('system.graph(name).drop()', {'name': graph_name},
                                  execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)
        except:
            pass

    @staticmethod
    def create_graph(session, graph_name):
        raise NotImplementedError()

    @staticmethod
    def clear(session):
        pass

    @staticmethod
    def create_vertex_label(session, vertex_label, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
        raise NotImplementedError()

    @staticmethod
    def add_vertex(session, vertex_label, name, value, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
        raise NotImplementedError()

    @classmethod
    def ensure_properties(cls, session, obj, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
        if not isinstance(obj, (Vertex, Edge)):
            return

        # This pre-processing is due to a change in TinkerPop
        # properties are not returned automatically anymore
        # with some queries.
        if not obj.properties:
            if isinstance(obj, Edge):
                obj.properties = {}
                for p in cls.get_edge_properties(session, obj, execution_profile=execution_profile):
                    obj.properties.update(p)
            elif isinstance(obj, Vertex):
                obj.properties = {
                    p.label: p
                    for p in cls.get_vertex_properties(session, obj, execution_profile=execution_profile)
                }

    @staticmethod
    def get_vertex_properties(session, vertex, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
        return session.execute_graph("g.V(vertex_id).properties().toList()", {'vertex_id': vertex.id},
                                     execution_profile=execution_profile)

    @staticmethod
    def get_edge_properties(session, edge, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
        v = session.execute_graph("g.E(edge_id).properties().toList()", {'edge_id': edge.id},
                                     execution_profile=execution_profile)
        return v


class ClassicGraphSchema(GraphSchema):

    fixtures = ClassicGraphFixtures

    @staticmethod
    def create_graph(session, graph_name):
        session.execute_graph(CREATE_CLASSIC_GRAPH, {'name': graph_name},
                              execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)
        wait_for_graph_inserted(session, graph_name)

    @staticmethod
    def clear(session):
        session.execute_graph('schema.clear()')

    @classmethod
    def create_vertex_label(cls, session, vertex_label, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
        statements = ["schema.propertyKey('pkid').Int().ifNotExists().create();"]
        for k, v in vertex_label.non_pk_properties.items():
            typ = cls.sanitize_type(v)
            statements.append("schema.propertyKey('{name}').{type}.create();".format(
                name=k, type=typ
            ))

        statements.append("schema.vertexLabel('{label}').partitionKey('pkid').properties(".format(
            label=vertex_label.label))
        property_names = [name for name in vertex_label.non_pk_properties.keys()]
        statements.append(", ".join(["'{}'".format(p) for p in property_names]))
        statements.append(").create();")

        to_run = "\n".join(statements)
        session.execute_graph(to_run, execution_profile=execution_profile)

    @staticmethod
    def add_vertex(session, vertex_label, name, value, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
        statement = "g.addV('{label}').property('pkid', {pkid}).property('{property_name}', val);".format(
            pkid=vertex_label.id, label=vertex_label.label, property_name=name)
        parameters = {'val': value}
        return session.execute_graph(statement, parameters, execution_profile=execution_profile)


class CoreGraphSchema(GraphSchema):

    fixtures = CoreGraphFixtures

    @classmethod
    def sanitize_type(cls, typ):
        typ = super(CoreGraphSchema, cls).sanitize_type(typ)
        return typ.replace('()', '')

    @classmethod
    def sanitize_point_type(cls):
        return "Point"

    @classmethod
    def sanitize_line_type(cls):
        return "LineString"

    @classmethod
    def sanitize_polygon_type(cls):
        return "Polygon"

    @staticmethod
    def create_graph(session, graph_name):
        session.execute_graph('system.graph(name).create()', {'name': graph_name},
                              execution_profile=EXEC_PROFILE_GRAPH_SYSTEM_DEFAULT)
        wait_for_graph_inserted(session, graph_name)

    @classmethod
    def create_vertex_label(cls, session, vertex_label, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
        statements = ["schema.vertexLabel('{label}').partitionBy('pkid', Int)".format(
            label=vertex_label.label)]

        for name, typ in vertex_label.non_pk_properties.items():
            typ = cls.sanitize_type(typ)
            statements.append(".property('{name}', {type})".format(name=name, type=typ))
        statements.append(".create();")

        to_run = "\n".join(statements)
        session.execute_graph(to_run, execution_profile=execution_profile)

    @staticmethod
    def add_vertex(session, vertex_label, name, value, execution_profile=EXEC_PROFILE_GRAPH_DEFAULT):
        statement = "g.addV('{label}').property('pkid', {pkid}).property('{property_name}', val);".format(
            pkid=vertex_label.id, label=vertex_label.label, property_name=name)
        parameters = {'val': value}
        return session.execute_graph(statement, parameters, execution_profile=execution_profile)
