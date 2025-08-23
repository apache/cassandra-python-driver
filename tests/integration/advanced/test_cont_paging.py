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

from tests.integration import use_singledc, greaterthanorequaldse51, BasicSharedKeyspaceUnitTestCaseRF3WM, \
    DSE_VERSION, ProtocolVersion, greaterthanorequaldse60, requiredse, TestCluster

import logging
log = logging.getLogger(__name__)

import unittest

from itertools import cycle, count
from packaging.version import Version
import time

from cassandra.cluster import ExecutionProfile, ContinuousPagingOptions
from cassandra.concurrent import execute_concurrent
from cassandra.query import SimpleStatement


def setup_module():
    if DSE_VERSION:
        use_singledc()


@requiredse
class BaseContPagingTests():
    @classmethod
    def setUpClass(cls):
        if not DSE_VERSION or DSE_VERSION < cls.required_dse_version:
            return

        cls.execution_profiles = {"CONTDEFAULT": ExecutionProfile(continuous_paging_options=ContinuousPagingOptions()),
                                  "ONEPAGE": ExecutionProfile(
                                      continuous_paging_options=ContinuousPagingOptions(max_pages=1)),
                                  "MANYPAGES": ExecutionProfile(
                                      continuous_paging_options=ContinuousPagingOptions(max_pages=10)),
                                  "BYTES": ExecutionProfile(continuous_paging_options=ContinuousPagingOptions(
                                      page_unit=ContinuousPagingOptions.PagingUnit.BYTES)),
                                  "SLOW": ExecutionProfile(
                                      continuous_paging_options=ContinuousPagingOptions(max_pages_per_second=1)), }
        cls.sane_eps = ["CONTDEFAULT", "BYTES"]

    @classmethod
    def tearDownClass(cls):
        if not DSE_VERSION or DSE_VERSION < cls.required_dse_version:
            return

    @classmethod
    def create_cluster(cls):

        cls.cluster_with_profiles = TestCluster(protocol_version=cls.protocol_version, execution_profiles=cls.execution_profiles)

        cls.session_with_profiles = cls.cluster_with_profiles.connect(wait_for_all_pools=True)
        statements_and_params = zip(
            cycle(["INSERT INTO  " + cls.ks_name + "." + cls.ks_name + " (k, v) VALUES (%s, 0)"]),
            [(i,) for i in range(150)])
        execute_concurrent(cls.session_with_profiles, list(statements_and_params))

        cls.select_all_statement = "SELECT * FROM {0}.{0}".format(cls.ks_name)

    def test_continuous_paging(self):
        """
        Test to ensure that various continuous paging schemes return the full set of results.
        @since 3.20
        @jira_ticket PYTHON-615
        @expected_result various continuous paging options should fetch all the results

        @test_category queries
        """
        for ep in self.execution_profiles.keys():
            results = list(self.session_with_profiles.execute(self.select_all_statement, execution_profile= ep))
            self.assertEqual(len(results), 150)


    def test_page_fetch_size(self):
        """
        Test to ensure that continuous paging works appropriately with fetch size.
        @since 3.20
        @jira_ticket PYTHON-615
        @expected_result continuous paging options should work sensibly with various fetch size

        @test_category queries
        """

        # Since we fetch one page at a time results should match fetch size
        for fetch_size in (2, 3, 7, 10, 99, 100, 101, 150):
            self.session_with_profiles.default_fetch_size = fetch_size
            results = list(self.session_with_profiles.execute(self.select_all_statement, execution_profile= "ONEPAGE"))
            self.assertEqual(len(results), fetch_size)

        # Since we fetch ten pages at a time results should match fetch size * 10
        for fetch_size in (2, 3, 7, 10, 15):
            self.session_with_profiles.default_fetch_size = fetch_size
            results = list(self.session_with_profiles.execute(self.select_all_statement, execution_profile= "MANYPAGES"))
            self.assertEqual(len(results), fetch_size*10)

        # Default settings for continuous paging should be able to fetch all results regardless of fetch size
        # Changing the units should, not affect the number of results, if max_pages is not set
        for profile in self.sane_eps:
            for fetch_size in (2, 3, 7, 10, 15):
                self.session_with_profiles.default_fetch_size = fetch_size
                results = list(self.session_with_profiles.execute(self.select_all_statement, execution_profile= profile))
                self.assertEqual(len(results), 150)

        # This should take around 3 seconds to fetch but should still complete with all results
        self.session_with_profiles.default_fetch_size = 50
        results = list(self.session_with_profiles.execute(self.select_all_statement, execution_profile= "SLOW"))
        self.assertEqual(len(results), 150)

    def test_paging_cancel(self):
        """
        Test to ensure we can cancel a continuous paging session once it's started
        @since 3.20
        @jira_ticket PYTHON-615
        @expected_result This query should be canceled before any sizable amount of results can be returned
        @test_category queries
        """

        self.session_with_profiles.default_fetch_size = 1
        # This combination should fetch one result a second. We should see a very few results
        results = self.session_with_profiles.execute_async(self.select_all_statement, execution_profile= "SLOW")
        result_set = results.result()
        result_set.cancel_continuous_paging()
        result_lst = list(result_set)
        self.assertLess(len(result_lst), 2, "Cancel should have aborted fetch immediately")

    def test_con_paging_verify_writes(self):
        """
        Test to validate results with a few continuous paging options
        @since 3.20
        @jira_ticket PYTHON-615
        @expected_result all results should be returned correctly
        @test_category queries
        """
        prepared = self.session_with_profiles.prepare(self.select_all_statement)


        for ep in self.sane_eps:
            for fetch_size in (2, 3, 7, 10, 99, 100, 101, 10000):
                self.session_with_profiles.default_fetch_size = fetch_size
                results = self.session_with_profiles.execute(self.select_all_statement, execution_profile=ep)
                result_array = set()
                result_set = set()
                for result in results:
                    result_array.add(result.k)
                    result_set.add(result.v)

                self.assertEqual(set(range(150)), result_array)
                self.assertEqual(set([0]), result_set)

                statement = SimpleStatement(self.select_all_statement)
                results = self.session_with_profiles.execute(statement, execution_profile=ep)
                result_array = set()
                result_set = set()
                for result in results:
                    result_array.add(result.k)
                    result_set.add(result.v)

                self.assertEqual(set(range(150)), result_array)
                self.assertEqual(set([0]), result_set)

                results = self.session_with_profiles.execute(prepared, execution_profile=ep)
                result_array = set()
                result_set = set()
                for result in results:
                    result_array.add(result.k)
                    result_set.add(result.v)

                self.assertEqual(set(range(150)), result_array)
                self.assertEqual(set([0]), result_set)

    def test_can_get_results_when_no_more_pages(self):
        """
        Test to validate that the results can be fetched when
        has_more_pages is False
        @since 3.20
        @jira_ticket PYTHON-946
        @expected_result the results can be fetched
        @test_category queries
        """
        generator_expanded = []
        def get_all_rows(generator, future, generator_expanded):
            self.assertFalse(future.has_more_pages)

            generator_expanded.extend(list(generator))
            print("Setting generator_expanded to True")

        future = self.session_with_profiles.execute_async("SELECT * from system.local LIMIT 10",
                                            execution_profile="CONTDEFAULT")
        future.add_callback(get_all_rows, future, generator_expanded)
        time.sleep(5)
        self.assertTrue(generator_expanded)


@requiredse
@greaterthanorequaldse51
class ContPagingTestsDSEV1(BaseContPagingTests, BasicSharedKeyspaceUnitTestCaseRF3WM):
    @classmethod
    def setUpClass(cls):
        cls.required_dse_version = BaseContPagingTests.required_dse_version = Version('5.1')
        if not DSE_VERSION or DSE_VERSION < cls.required_dse_version:
            return

        BasicSharedKeyspaceUnitTestCaseRF3WM.setUpClass()
        BaseContPagingTests.setUpClass()

        cls.protocol_version = ProtocolVersion.DSE_V1
        cls.create_cluster()


@requiredse
@greaterthanorequaldse60
class ContPagingTestsDSEV2(BaseContPagingTests, BasicSharedKeyspaceUnitTestCaseRF3WM):
    @classmethod
    def setUpClass(cls):
        cls.required_dse_version = BaseContPagingTests.required_dse_version = Version('6.0')
        if not DSE_VERSION or DSE_VERSION < cls.required_dse_version:
            return

        BasicSharedKeyspaceUnitTestCaseRF3WM.setUpClass()
        BaseContPagingTests.setUpClass()

        more_profiles = {
            "SMALL_QUEUE": ExecutionProfile(continuous_paging_options=ContinuousPagingOptions(max_queue_size=2)),
            "BIG_QUEUE": ExecutionProfile(continuous_paging_options=ContinuousPagingOptions(max_queue_size=400))
        }
        cls.sane_eps += ["SMALL_QUEUE", "BIG_QUEUE"]
        cls.execution_profiles.update(more_profiles)

        cls.protocol_version = ProtocolVersion.DSE_V2
        cls.create_cluster()
