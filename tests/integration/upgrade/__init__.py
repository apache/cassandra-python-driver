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


from tests.integration import CCM_KWARGS, use_cluster, remove_cluster, MockLoggingHandler
from tests.integration import setup_keyspace

from cassandra.cluster import Cluster
from cassandra import cluster

from collections import namedtuple
from functools import wraps
import logging
from threading import Thread, Event
from ccmlib.node import TimeoutError
import time
import logging

import unittest


def setup_module():
    remove_cluster()


UPGRADE_CLUSTER_NAME = "upgrade_cluster"
UpgradePath = namedtuple('UpgradePath', ('name', 'starting_version', 'upgrade_version', 'configuration_options'))

log = logging.getLogger(__name__)


class upgrade_paths(object):
    """
    Decorator used to specify the upgrade paths for a particular method
    """
    def __init__(self, paths):
        self.paths = paths

    def __call__(self, method):
        @wraps(method)
        def wrapper(*args, **kwargs):
            for path in self.paths:
                self_from_decorated = args[0]
                log.debug('setting up {path}'.format(path=path))
                self_from_decorated.UPGRADE_PATH = path
                self_from_decorated._upgrade_step_setup()
                method(*args, **kwargs)
                log.debug('tearing down {path}'.format(path=path))
                self_from_decorated._upgrade_step_teardown()
        return wrapper


class UpgradeBase(unittest.TestCase):
    """
    Base class for the upgrade tests. The _setup method
    will clean the environment and start the appropriate C* version according
    to the upgrade path. The upgrade can be done in a different thread using the
    start_upgrade upgrade_method (this would be the most realistic scenario)
    or node by node, waiting for the upgrade to happen, using _upgrade_one_node method
    """
    UPGRADE_PATH = None
    start_cluster = True
    set_keyspace = True

    @classmethod
    def setUpClass(cls):
        cls.logger_handler = MockLoggingHandler()
        logger = logging.getLogger(cluster.__name__)
        logger.addHandler(cls.logger_handler)

    def _upgrade_step_setup(self):
        """
        This is not the regular _setUp method because it will be called from
        the decorator instead of letting nose handle it.
        This setup method will start a cluster with the right version according
        to the variable UPGRADE_PATH.
        """
        remove_cluster()
        self.cluster = use_cluster(UPGRADE_CLUSTER_NAME + self.UPGRADE_PATH.name, [3],
                                   ccm_options=self.UPGRADE_PATH.starting_version, set_keyspace=self.set_keyspace,
                                   configuration_options=self.UPGRADE_PATH.configuration_options)
        self.nodes = self.cluster.nodelist()
        self.last_node_upgraded = None
        self.upgrade_done = Event()
        self.upgrade_thread = None

        if self.start_cluster:
            setup_keyspace()

            self.cluster_driver = Cluster()
            self.session = self.cluster_driver.connect()
            self.logger_handler.reset()

    def _upgrade_step_teardown(self):
        """
        special tearDown method called by the decorator after the method has ended
        """
        if self.upgrade_thread:
            self.upgrade_thread.join(timeout=5)
            self.upgrade_thread = None

        if self.start_cluster:
            self.cluster_driver.shutdown()

    def start_upgrade(self, time_node_upgrade):
        """
        Starts the upgrade in a different thread
        """
        log.debug('Starting upgrade in new thread')
        self.upgrade_thread = Thread(target=self._upgrade, args=(time_node_upgrade,))
        self.upgrade_thread.start()

    def _upgrade(self, time_node_upgrade):
        """
        Starts the upgrade in the same thread
        """
        start_time = time.time()
        for node in self.nodes:
            self.upgrade_node(node)
            end_time = time.time()
            time_to_upgrade = end_time - start_time
            if time_node_upgrade > time_to_upgrade:
                time.sleep(time_node_upgrade - time_to_upgrade)
        self.upgrade_done.set()

    def is_upgraded(self):
        """
        Returns True if the upgrade has finished and False otherwise
        """
        return self.upgrade_done.is_set()

    def wait_for_upgrade(self, timeout=None):
        """
        Waits until the upgrade has completed
        """
        self.upgrade_done.wait(timeout=timeout)

    def upgrade_node(self, node):
        """
        Upgrades only one node. Return True if the upgrade
        has finished and False otherwise
        """
        node.drain()
        node.stop(gently=True)

        node.set_install_dir(**self.UPGRADE_PATH.upgrade_version)

        # There must be a cleaner way of doing this, but it's necessary here
        # to call the private method from cluster __update_topology_files
        self.cluster._Cluster__update_topology_files()
        try:
            node.start(wait_for_binary_proto=True, wait_other_notice=True)
        except TimeoutError:
            self.fail("Error starting C* node while upgrading")

        return True


class UpgradeBaseAuth(UpgradeBase):
    """
    Base class of authentication test, the authentication parameters for
    C* still have to be specified within the upgrade path variable
    """
    start_cluster = False
    set_keyspace = False


    def _upgrade_step_setup(self):
        """
        We sleep here for the same reason as we do in test_authentication.py:
        there seems to be some race, with some versions of C* taking longer to
        get the auth (and default user) setup. Sleep here to give it a chance
        """
        super(UpgradeBaseAuth, self)._upgrade_step_setup()
        time.sleep(10)
