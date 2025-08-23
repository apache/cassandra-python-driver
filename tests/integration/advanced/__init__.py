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

import unittest

from urllib.request import build_opener, Request, HTTPHandler
import re
import os
import time
from os.path import expanduser

from ccmlib import common

from tests.integration import get_server_versions, BasicKeyspaceUnitTestCase, \
    drop_keyspace_shutdown_cluster, get_node, USE_CASS_EXTERNAL, TestCluster
from tests.integration import use_singledc, use_single_node, wait_for_node_socket, CASSANDRA_IP

home = expanduser('~')

# Home directory of the Embedded Apache Directory Server to use
ADS_HOME = os.getenv('ADS_HOME', home)


def find_spark_master(session):

    # Iterate over the nodes the one with port 7080 open is the spark master
    for host in session.hosts:
        ip = host.address
        port = 7077
        spark_master = (ip, port)
        if common.check_socket_listening(spark_master, timeout=3):
            return spark_master[0]
    return None


def wait_for_spark_workers(num_of_expected_workers, timeout):
    """
    This queries the spark master and checks for the expected number of workers
    """
    start_time = time.time()
    while True:
        opener = build_opener(HTTPHandler)
        request = Request("http://{0}:7080".format(CASSANDRA_IP))
        request.get_method = lambda: 'GET'
        connection = opener.open(request)
        match = re.search('Alive Workers:.*(\d+)</li>', connection.read().decode('utf-8'))
        num_workers = int(match.group(1))
        if num_workers == num_of_expected_workers:
            match = True
            break
        elif time.time() - start_time > timeout:
            match = True
            break
        time.sleep(1)
    return match


def use_single_node_with_graph(start=True, options={}, dse_options={}):
    use_single_node(start=start, workloads=['graph'], configuration_options=options, dse_options=dse_options)


def use_single_node_with_graph_and_spark(start=True, options={}):
    use_single_node(start=start, workloads=['graph', 'spark'], configuration_options=options)


def use_single_node_with_graph_and_solr(start=True, options={}):
    use_single_node(start=start, workloads=['graph', 'solr'], configuration_options=options)


def use_singledc_wth_graph(start=True):
    use_singledc(start=start, workloads=['graph'])


def use_singledc_wth_graph_and_spark(start=True):
    use_cluster_with_graph(3)


def use_cluster_with_graph(num_nodes):
    """
    This is a  workaround to account for the fact that spark nodes will conflict over master assignment
    when started all at once.
    """
    if USE_CASS_EXTERNAL:
        return

    # Create the cluster but don't start it.
    use_singledc(start=False, workloads=['graph', 'spark'])
    # Start first node.
    get_node(1).start(wait_for_binary_proto=True)
    # Wait binary protocol port to open
    wait_for_node_socket(get_node(1), 120)
    # Wait for spark master to start up
    spark_master_http = ("localhost", 7080)
    common.check_socket_listening(spark_master_http, timeout=60)
    tmp_cluster = TestCluster()

    # Start up remaining nodes.
    try:
        session = tmp_cluster.connect()
        statement = "ALTER KEYSPACE dse_leases WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'dc1': '%d'}" % (num_nodes)
        session.execute(statement)
    finally:
        tmp_cluster.shutdown()

    for i in range(1, num_nodes+1):
        if i is not 1:
            node = get_node(i)
            node.start(wait_for_binary_proto=True)
            wait_for_node_socket(node, 120)

    # Wait for workers to show up as Alive on master
    wait_for_spark_workers(3, 120)


class BasicGeometricUnitTestCase(BasicKeyspaceUnitTestCase):
    """
    This base test class is used by all the geometric tests. It contains class level teardown and setup
    methods. It also contains the test fixtures used by those tests
    """

    @classmethod
    def common_dse_setup(cls, rf, keyspace_creation=True):
        cls.cluster = TestCluster()
        cls.session = cls.cluster.connect()
        cls.ks_name = cls.__name__.lower()
        if keyspace_creation:
            cls.create_keyspace(rf)
        cls.cass_version, cls.cql_version = get_server_versions()
        cls.session.set_keyspace(cls.ks_name)

    @classmethod
    def setUpClass(cls):
        cls.common_dse_setup(1)
        cls.initalizeTables()

    @classmethod
    def tearDownClass(cls):
        drop_keyspace_shutdown_cluster(cls.ks_name, cls.session, cls.cluster)

    @classmethod
    def initalizeTables(cls):
        udt_type = "CREATE TYPE udt1 (g {0})".format(cls.cql_type_name)
        large_table = "CREATE TABLE tbl (k uuid PRIMARY KEY, g {0}, l list<{0}>, s set<{0}>, m0 map<{0},int>, m1 map<int,{0}>, t tuple<{0},{0},{0}>, u frozen<udt1>)".format(
            cls.cql_type_name)
        simple_table = "CREATE TABLE tblpk (k {0} primary key, v int)".format(cls.cql_type_name)
        cluster_table = "CREATE TABLE tblclustering (k0 int, k1 {0}, v int, primary key (k0, k1))".format(
            cls.cql_type_name)
        cls.session.execute(udt_type)
        cls.session.execute(large_table)
        cls.session.execute(simple_table)
        cls.session.execute(cluster_table)
