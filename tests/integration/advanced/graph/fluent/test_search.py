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

from cassandra.util import Distance
from cassandra import InvalidRequest
from cassandra.graph import GraphProtocol
from cassandra.datastax.graph.fluent import DseGraph
from cassandra.datastax.graph.fluent.predicates import Search, Geo, GeoUnit, CqlCollection

from tests.integration.advanced import use_single_node_with_graph_and_solr
from tests.integration.advanced.graph import GraphUnitTestCase, CoreGraphSchema, ClassicGraphSchema, GraphTestConfiguration
from tests.integration import greaterthanorequaldse51, DSE_VERSION, requiredse


def setup_module():
    if DSE_VERSION:
        use_single_node_with_graph_and_solr()


class AbstractSearchTest(GraphUnitTestCase):

    def setUp(self):
        super(AbstractSearchTest, self).setUp()
        self.ep_graphson2 = DseGraph().create_execution_profile(self.graph_name,
                                                                graph_protocol=GraphProtocol.GRAPHSON_2_0)
        self.ep_graphson3 = DseGraph().create_execution_profile(self.graph_name,
                                                                graph_protocol=GraphProtocol.GRAPHSON_3_0)

        self.cluster.add_execution_profile('traversal_graphson2', self.ep_graphson2)
        self.cluster.add_execution_profile('traversal_graphson3', self.ep_graphson3)

    def fetch_traversal_source(self, graphson):
        ep = self.get_execution_profile(graphson, traversal=True)
        return DseGraph().traversal_source(self.session, self.graph_name, execution_profile=ep)

    def _test_search_by_prefix(self, schema, graphson):
        """
        Test to validate that solr searches by prefix function.

        @since 1.0.0
        @jira_ticket PYTHON-660
        @expected_result all names starting with Paul should be returned

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.address_book(), graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.V().has("person", "name", Search.prefix("Paul")).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 1)
        self.assertEqual(results_list[0], "Paul Thomas Joe")

    def _test_search_by_regex(self, schema, graphson):
        """
        Test to validate that solr searches by regex function.

        @since 1.0.0
        @jira_ticket PYTHON-660
        @expected_result all names containing Paul should be returned

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.address_book(), graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.V().has("person", "name", Search.regex(".*Paul.*")).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Paul Thomas Joe", results_list)
        self.assertIn("James Paul Smith", results_list)

    def _test_search_by_token(self, schema, graphson):
        """
        Test to validate that solr searches by token.

        @since 1.0.0
        @jira_ticket PYTHON-660
        @expected_result all names with description containing could shoud be returned

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.address_book(), graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.V().has("person", "description", Search.token("cold")).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Jill Alice", results_list)
        self.assertIn("George Bill Steve", results_list)

    def _test_search_by_token_prefix(self, schema, graphson):
        """
        Test to validate that solr searches by token prefix.

        @since 1.0.0
        @jira_ticket PYTHON-660
        @expected_result all names with description containing a token starting with h are returned

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.address_book(), graphson)
        g = self.fetch_traversal_source(graphson)
        traversal =  g.V().has("person", "description", Search.token_prefix("h")).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Paul Thomas Joe", results_list)
        self.assertIn( "James Paul Smith", results_list)

    def _test_search_by_token_regex(self, schema, graphson):
        """
        Test to validate that solr searches by token regex.

        @since 1.0.0
        @jira_ticket PYTHON-660
        @expected_result all names with description containing nice or hospital are returned

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.address_book(), graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.V().has("person", "description", Search.token_regex("(nice|hospital)")).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Paul Thomas Joe", results_list )
        self.assertIn( "Jill Alice", results_list )

    def _assert_in_distance(self, schema, graphson, inside, names):
        """
        Helper function that asserts that an exception is arisen if geodetic predicates are used
        in cartesian geometry. Also asserts that the expected list is equal to the returned from
        the transversal using different search indexes.
        """
        def assert_equal_list(L1, L2):
            return len(L1) == len(L2) and sorted(L1) == sorted(L2)

        self.execute_graph(schema.fixtures.address_book(), graphson)
        g = self.fetch_traversal_source(graphson)

        traversal = g.V().has("person", "pointPropWithBoundsWithSearchIndex", inside).values("name")
        if schema is ClassicGraphSchema:
            # throws an exception because of a SOLR/Search limitation in the indexing process
            # may be resolved in the future
            self.assertRaises(InvalidRequest, self.execute_traversal, traversal, graphson)
        else:
            traversal = g.V().has("person", "pointPropWithBoundsWithSearchIndex", inside).values("name")
            results_list = self.execute_traversal(traversal, graphson)
            assert_equal_list(names, results_list)

        traversal = g.V().has("person", "pointPropWithBounds", inside).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        assert_equal_list(names, results_list)

        traversal = g.V().has("person", "pointPropWithGeoBoundsWithSearchIndex", inside).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        assert_equal_list(names, results_list)

        traversal = g.V().has("person", "pointPropWithGeoBounds", inside).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        assert_equal_list(names, results_list)

    @greaterthanorequaldse51
    def _test_search_by_distance(self, schema, graphson):
        """
        Test to validate that solr searches by distance.

        @since 1.0.0
        @jira_ticket PYTHON-660
        @expected_result all names with a geo location within a 2 degree distance of -92,44 are returned

        @test_category dse graph
        """
        self._assert_in_distance(schema, graphson,
            Geo.inside(Distance(-92, 44, 2)),
            ["Paul Thomas Joe", "George Bill Steve"]
        )

    @greaterthanorequaldse51
    def _test_search_by_distance_meters_units(self, schema, graphson):
        """
        Test to validate that solr searches by distance.

        @since 2.0.0
        @jira_ticket PYTHON-698
        @expected_result all names with a geo location within a 56k-meter radius of -92,44 are returned

        @test_category dse graph
        """
        self._assert_in_distance(schema, graphson,
            Geo.inside(Distance(-92, 44, 56000), GeoUnit.METERS),
            ["Paul Thomas Joe"]
        )

    @greaterthanorequaldse51
    def _test_search_by_distance_miles_units(self, schema, graphson):
        """
        Test to validate that solr searches by distance.

        @since 2.0.0
        @jira_ticket PYTHON-698
        @expected_result all names with a geo location within a 70-mile radius of -92,44 are returned

        @test_category dse graph
        """
        self._assert_in_distance(schema, graphson,
            Geo.inside(Distance(-92, 44, 70), GeoUnit.MILES),
            ["Paul Thomas Joe", "George Bill Steve"]
        )

    @greaterthanorequaldse51
    def _test_search_by_distance_check_limit(self, schema, graphson):
        """
        Test to validate that solr searches by distance using several units. It will also validate
        that and exception is arisen if geodetic predicates are used against cartesian geometry

        @since 2.0.0
        @jira_ticket PYTHON-698
        @expected_result if the search distance is below the real distance only one
        name will be in the list, otherwise, two

        @test_category dse graph
        """
        # Paul Thomas Joe and George Bill Steve are 64.6923761881464 km apart
        self._assert_in_distance(schema, graphson,
            Geo.inside(Distance(-92.46295, 44.0234, 65), GeoUnit.KILOMETERS),
            ["George Bill Steve", "Paul Thomas Joe"]
        )

        self._assert_in_distance(schema, graphson,
            Geo.inside(Distance(-92.46295, 44.0234, 64), GeoUnit.KILOMETERS),
            ["Paul Thomas Joe"]
        )

        # Paul Thomas Joe and George Bill Steve are 40.19797892069464 miles apart
        self._assert_in_distance(schema, graphson,
            Geo.inside(Distance(-92.46295, 44.0234, 41), GeoUnit.MILES),
            ["George Bill Steve", "Paul Thomas Joe"]
        )

        self._assert_in_distance(schema, graphson,
            Geo.inside(Distance(-92.46295, 44.0234, 40), GeoUnit.MILES),
            ["Paul Thomas Joe"]
        )

    @greaterthanorequaldse51
    def _test_search_by_fuzzy(self, schema, graphson):
        """
        Test to validate that solr searches by distance.

        @since 1.0.0
        @jira_ticket PYTHON-664
        @expected_result all names with a geo location within a 2 radius distance of -92,44 are returned

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.address_book(), graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.V().has("person", "name", Search.fuzzy("Paul Thamas Joe", 1)).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 1)
        self.assertIn("Paul Thomas Joe", results_list)

        traversal = g.V().has("person", "name", Search.fuzzy("Paul Thames Joe", 1)).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 0)

    @greaterthanorequaldse51
    def _test_search_by_fuzzy_token(self, schema, graphson):
        """
        Test to validate that fuzzy searches.

        @since 1.0.0
        @jira_ticket PYTHON-664
        @expected_result all names with that differ from the search criteria by one letter should be returned

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.address_book(), graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.V().has("person", "description", Search.token_fuzzy("lives", 1)).values("name")
        # Should match 'Paul Thomas Joe' since description contains 'Lives'
        # Should match 'James Paul Joe' since description contains 'Likes'
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 2)
        self.assertIn("Paul Thomas Joe", results_list)
        self.assertIn("James Paul Smith", results_list)

        traversal = g.V().has("person", "description", Search.token_fuzzy("loues", 1)).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 0)

    @greaterthanorequaldse51
    def _test_search_by_phrase(self, schema, graphson):
        """
        Test to validate that phrase searches.

        @since 1.0.0
        @jira_ticket PYTHON-664
        @expected_result all names with that differ from the search phrase criteria by two letter should be returned

        @test_category dse graph
        """
        self.execute_graph(schema.fixtures.address_book(), graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.V().has("person", "description", Search.phrase("a cold", 2)).values("name")
        #Should match 'George Bill Steve' since 'A cold dude' is at distance of 0 for 'a cold'.
        #Should match 'Jill Alice' since 'Enjoys a very nice cold coca cola' is at distance of 2 for 'a cold'.
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 2)
        self.assertIn('George Bill Steve', results_list)
        self.assertIn('Jill Alice', results_list)

        traversal = g.V().has("person", "description", Search.phrase("a bald", 2)).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 0)


@requiredse
@GraphTestConfiguration.generate_tests(traversal=True)
class ImplicitSearchTest(AbstractSearchTest):
    """
    This test class will execute all tests of the AbstractSearchTest using implicit execution
    All traversals will be run directly using toList()
    """
    def fetch_key_from_prop(self, property):
        return property.key

    def execute_traversal(self, traversal, graphson=None):
        return traversal.toList()


@requiredse
@GraphTestConfiguration.generate_tests(traversal=True)
class ExplicitSearchTest(AbstractSearchTest):
    """
    This test class will execute all tests of the AbstractSearchTest using implicit execution
    All traversals will be converted to byte code then they will be executed explicitly.
    """

    def execute_traversal(self, traversal, graphson):
        ep = self.get_execution_profile(graphson, traversal=True)
        ep = self.session.get_execution_profile(ep)
        context = None
        if graphson == GraphProtocol.GRAPHSON_3_0:
            context = {
                'cluster': self.cluster,
                'graph_name': ep.graph_options.graph_name.decode('utf-8') if ep.graph_options.graph_name else None
            }
        query = DseGraph.query_from_traversal(traversal, graphson, context=context)
        #Use an ep that is configured with the correct row factory, and bytecode-json language flat set
        result_set = self.execute_graph(query, graphson, traversal=True)
        return list(result_set)


@requiredse
class BaseCqlCollectionPredicatesTest(GraphUnitTestCase):

    def setUp(self):
        super(BaseCqlCollectionPredicatesTest, self).setUp()
        self.ep_graphson3 = DseGraph().create_execution_profile(self.graph_name,
                                                                graph_protocol=GraphProtocol.GRAPHSON_3_0)
        self.cluster.add_execution_profile('traversal_graphson3', self.ep_graphson3)

    def fetch_traversal_source(self, graphson):
        ep = self.get_execution_profile(graphson, traversal=True)
        return DseGraph().traversal_source(self.session, self.graph_name, execution_profile=ep)

    def setup_vertex_label(self, graphson):
        ep = self.get_execution_profile(graphson)
        self.session.execute_graph("""
            schema.vertexLabel('cqlcollections').ifNotExists().partitionBy('name', Varchar)
            .property('list', listOf(Text))
            .property('frozen_list', frozen(listOf(Text)))
            .property('set', setOf(Text))
            .property('frozen_set', frozen(setOf(Text)))
            .property('map_keys', mapOf(Int, Text))
            .property('map_values', mapOf(Int, Text))
            .property('map_entries', mapOf(Int, Text))
            .property('frozen_map', frozen(mapOf(Int, Text)))
            .create()
        """, execution_profile=ep)

        self.session.execute_graph("""
            schema.vertexLabel('cqlcollections').secondaryIndex('list').by('list').create();
            schema.vertexLabel('cqlcollections').secondaryIndex('frozen_list').by('frozen_list').indexFull().create();
            schema.vertexLabel('cqlcollections').secondaryIndex('set').by('set').create();
            schema.vertexLabel('cqlcollections').secondaryIndex('frozen_set').by('frozen_set').indexFull().create();
            schema.vertexLabel('cqlcollections').secondaryIndex('map_keys').by('map_keys').indexKeys().create();
            schema.vertexLabel('cqlcollections').secondaryIndex('map_values').by('map_values').indexValues().create();
            schema.vertexLabel('cqlcollections').secondaryIndex('map_entries').by('map_entries').indexEntries().create();
            schema.vertexLabel('cqlcollections').secondaryIndex('frozen_map').by('frozen_map').indexFull().create();
        """, execution_profile=ep)

    def _test_contains_list(self, schema, graphson):
        """
        Test to validate that the cql predicate contains works with list

        @since TODO dse 6.8
        @jira_ticket PYTHON-1039
        @expected_result contains predicate work on a list

        @test_category dse graph
        """
        self.setup_vertex_label(graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.addV("cqlcollections").property("name", "list1").property("list", ['item1', 'item2'])
        self.execute_traversal(traversal, graphson)
        traversal = g.addV("cqlcollections").property("name", "list2").property("list", ['item3', 'item4'])
        self.execute_traversal(traversal, graphson)
        traversal = g.V().has("cqlcollections", "list", CqlCollection.contains("item1")).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 1)
        self.assertIn("list1", results_list)

    def _test_contains_set(self, schema, graphson):
        """
        Test to validate that the cql predicate contains works with set

        @since TODO dse 6.8
        @jira_ticket PYTHON-1039
        @expected_result contains predicate work on a set

        @test_category dse graph
        """
        self.setup_vertex_label(graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.addV("cqlcollections").property("name", "set1").property("set", {'item1', 'item2'})
        self.execute_traversal(traversal, graphson)
        traversal = g.addV("cqlcollections").property("name", "set2").property("set", {'item3', 'item4'})
        self.execute_traversal(traversal, graphson)
        traversal = g.V().has("cqlcollections", "set", CqlCollection.contains("item1")).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 1)
        self.assertIn("set1", results_list)

    def _test_contains_key_map(self, schema, graphson):
        """
        Test to validate that the cql predicate contains_key works with map

        @since TODO dse 6.8
        @jira_ticket PYTHON-1039
        @expected_result contains_key predicate work on a map

        @test_category dse graph
        """
        self.setup_vertex_label(graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.addV("cqlcollections").property("name", "map1").property("map_keys", {0: 'item1', 1: 'item2'})
        self.execute_traversal(traversal, graphson)
        traversal = g.addV("cqlcollections").property("name", "map2").property("map_keys", {2: 'item3', 3: 'item4'})
        self.execute_traversal(traversal, graphson)
        traversal = g.V().has("cqlcollections", "map_keys", CqlCollection.contains_key(0)).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 1)
        self.assertIn("map1", results_list)

    def _test_contains_value_map(self, schema, graphson):
        """
        Test to validate that the cql predicate contains_value works with map

        @since TODO dse 6.8
        @jira_ticket PYTHON-1039
        @expected_result contains_value predicate work on a map

        @test_category dse graph
        """
        self.setup_vertex_label(graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.addV("cqlcollections").property("name", "map1").property("map_values", {0: 'item1', 1: 'item2'})
        self.execute_traversal(traversal, graphson)
        traversal = g.addV("cqlcollections").property("name", "map2").property("map_values", {2: 'item3', 3: 'item4'})
        self.execute_traversal(traversal, graphson)
        traversal = g.V().has("cqlcollections", "map_values", CqlCollection.contains_value('item3')).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 1)
        self.assertIn("map2", results_list)

    def _test_entry_eq_map(self, schema, graphson):
        """
        Test to validate that the cql predicate entry_eq works with map

        @since TODO dse 6.8
        @jira_ticket PYTHON-1039
        @expected_result entry_eq predicate work on a map

        @test_category dse graph
        """
        self.setup_vertex_label(graphson)
        g = self.fetch_traversal_source(graphson)
        traversal = g.addV("cqlcollections").property("name", "map1").property("map_entries", {0: 'item1', 1: 'item2'})
        self.execute_traversal(traversal, graphson)
        traversal = g.addV("cqlcollections").property("name", "map2").property("map_entries", {2: 'item3', 3: 'item4'})
        self.execute_traversal(traversal, graphson)
        traversal = g.V().has("cqlcollections", "map_entries", CqlCollection.entry_eq([2, 'item3'])).values("name")
        results_list = self.execute_traversal(traversal, graphson)
        self.assertEqual(len(results_list), 1)
        self.assertIn("map2", results_list)


@requiredse
@GraphTestConfiguration.generate_tests(traversal=True, schema=CoreGraphSchema)
class ImplicitCqlCollectionPredicatesTest(BaseCqlCollectionPredicatesTest):
    """
    This test class will execute all tests of the BaseCqlCollectionTest using implicit execution
    All traversals will be run directly using toList()
    """

    def execute_traversal(self, traversal, graphson=None):
        return traversal.toList()


@requiredse
@GraphTestConfiguration.generate_tests(traversal=True, schema=CoreGraphSchema)
class ExplicitCqlCollectionPredicatesTest(BaseCqlCollectionPredicatesTest):
    """
    This test class will execute all tests of the AbstractSearchTest using implicit execution
    All traversals will be converted to byte code then they will be executed explicitly.
    """

    def execute_traversal(self, traversal, graphson):
        ep = self.get_execution_profile(graphson, traversal=True)
        ep = self.session.get_execution_profile(ep)
        context = None
        if graphson == GraphProtocol.GRAPHSON_3_0:
            context = {
                'cluster': self.cluster,
                'graph_name': ep.graph_options.graph_name.decode('utf-8') if ep.graph_options.graph_name else None
            }
        query = DseGraph.query_from_traversal(traversal, graphson, context=context)
        result_set = self.execute_graph(query, graphson, traversal=True)
        return list(result_set)
