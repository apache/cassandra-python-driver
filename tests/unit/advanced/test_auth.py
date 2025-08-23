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

import os
from puresasl import QOP

import unittest

from cassandra.auth import DSEGSSAPIAuthProvider

# Cannot import requiredse from tests.integration
# This auth provider requires kerberos and puresals
DSE_VERSION = os.getenv('DSE_VERSION', None)
@unittest.skipUnless(DSE_VERSION, "DSE required")
class TestGSSAPI(unittest.TestCase):

    def test_host_resolution(self):
        # resolves by default
        provider = DSEGSSAPIAuthProvider(service='test', qops=QOP.all)
        authenticator = provider.new_authenticator('127.0.0.1')
        self.assertEqual(authenticator.sasl.host, 'localhost')

        # numeric fallback okay
        authenticator = provider.new_authenticator('192.0.2.1')
        self.assertEqual(authenticator.sasl.host, '192.0.2.1')

        # disable explicitly
        provider = DSEGSSAPIAuthProvider(service='test', qops=QOP.all, resolve_host_name=False)
        authenticator = provider.new_authenticator('127.0.0.1')
        self.assertEqual(authenticator.sasl.host, '127.0.0.1')

