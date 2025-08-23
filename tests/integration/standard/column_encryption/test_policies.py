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
import unittest

from tests.integration import use_singledc, TestCluster

from cassandra.policies import ColDesc

from cassandra.column_encryption.policies import AES256ColumnEncryptionPolicy, \
    AES256_KEY_SIZE_BYTES, AES256_BLOCK_SIZE_BYTES


def setup_module():
    use_singledc()


class ColumnEncryptionPolicyTest(unittest.TestCase):

    def _recreate_keyspace(self, session):
        session.execute("drop keyspace if exists foo")
        session.execute("CREATE KEYSPACE foo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
        session.execute("CREATE TABLE foo.bar(encrypted blob, unencrypted int, primary key(unencrypted))")

    def _create_policy(self, key, iv = None):
        cl_policy = AES256ColumnEncryptionPolicy()
        col_desc = ColDesc('foo','bar','encrypted')
        cl_policy.add_column(col_desc, key, "int")
        return (col_desc, cl_policy)

    def test_end_to_end_prepared(self):

        # We only currently perform testing on a single type/expected value pair since CLE functionality is essentially
        # independent of the underlying type.  We intercept data after it's been encoded when it's going out and before it's
        # encoded when coming back; the actual types of the data involved don't impact us.
        expected = 0

        key = os.urandom(AES256_KEY_SIZE_BYTES)
        (_, cl_policy) = self._create_policy(key)
        cluster = TestCluster(column_encryption_policy=cl_policy)
        session = cluster.connect()
        self._recreate_keyspace(session)

        prepared = session.prepare("insert into foo.bar (encrypted, unencrypted) values (?,?)")
        for i in range(100):
            session.execute(prepared, (i, i))

        # A straight select from the database will now return the decrypted bits.  We select both encrypted and unencrypted
        # values here to confirm that we don't interfere with regular processing of unencrypted vals.
        (encrypted, unencrypted) = session.execute("select encrypted, unencrypted from foo.bar where unencrypted = %s allow filtering", (expected,)).one()
        self.assertEqual(expected, encrypted)
        self.assertEqual(expected, unencrypted)

        # Confirm the same behaviour from a subsequent prepared statement as well
        prepared = session.prepare("select encrypted, unencrypted from foo.bar where unencrypted = ? allow filtering")
        (encrypted, unencrypted) = session.execute(prepared, [expected]).one()
        self.assertEqual(expected, encrypted)
        self.assertEqual(expected, unencrypted)

    def test_end_to_end_simple(self):

        expected = 1

        key = os.urandom(AES256_KEY_SIZE_BYTES)
        (col_desc, cl_policy) = self._create_policy(key)
        cluster = TestCluster(column_encryption_policy=cl_policy)
        session = cluster.connect()
        self._recreate_keyspace(session)

        # Use encode_and_encrypt helper function to populate date
        for i in range(1, 100):
            self.assertIsNotNone(i)
            encrypted = cl_policy.encode_and_encrypt(col_desc, i)
            session.execute("insert into foo.bar (encrypted, unencrypted) values (%s,%s)", (encrypted, i))

        # A straight select from the database will now return the decrypted bits.  We select both encrypted and unencrypted
        # values here to confirm that we don't interfere with regular processing of unencrypted vals.
        (encrypted, unencrypted) = session.execute("select encrypted, unencrypted from foo.bar where unencrypted = %s allow filtering", (expected,)).one()
        self.assertEqual(expected, encrypted)
        self.assertEqual(expected, unencrypted)

        # Confirm the same behaviour from a subsequent prepared statement as well
        prepared = session.prepare("select encrypted, unencrypted from foo.bar where unencrypted = ? allow filtering")
        (encrypted, unencrypted) = session.execute(prepared, [expected]).one()
        self.assertEqual(expected, encrypted)
        self.assertEqual(expected, unencrypted)

    def test_end_to_end_different_cle_contexts_different_ivs(self):
        """
        Test to validate PYTHON-1350.  We should be able to decode the data from two different contexts (with two different IVs)
        since the IV used to decrypt the data is actually now stored with the data.
        """

        expected = 2

        key = os.urandom(AES256_KEY_SIZE_BYTES)

        # Simulate the creation of two AES256 policies at two different times.  Python caches
        # default param args at function definition time so a single value will be used any time
        # the default val is used.  Upshot is that within the same test we'll always have the same
        # IV if we rely on the default args, so manually introduce some variation here to simulate
        # what actually happens if you have two distinct sessions created at two different times.
        iv1 = os.urandom(AES256_BLOCK_SIZE_BYTES)
        (col_desc1, cl_policy1) = self._create_policy(key, iv=iv1)
        cluster1 = TestCluster(column_encryption_policy=cl_policy1)
        session1 = cluster1.connect()
        self._recreate_keyspace(session1)

        # Use encode_and_encrypt helper function to populate date
        for i in range(1, 100):
            self.assertIsNotNone(i)
            encrypted = cl_policy1.encode_and_encrypt(col_desc1, i)
            session1.execute("insert into foo.bar (encrypted, unencrypted) values (%s,%s)", (encrypted, i))
        session1.shutdown()
        cluster1.shutdown()

        # Explicitly clear the class-level cache here; we're trying to simulate a second connection from a completely new process and
        # that would entail not re-using any cached ciphers
        AES256ColumnEncryptionPolicy._build_cipher.cache_clear()
        cache_info = cl_policy1.cache_info()
        self.assertEqual(cache_info.currsize, 0)

        iv2 = os.urandom(AES256_BLOCK_SIZE_BYTES)
        (_, cl_policy2) = self._create_policy(key, iv=iv2)
        cluster2 = TestCluster(column_encryption_policy=cl_policy2)
        session2 = cluster2.connect()
        (encrypted, unencrypted) = session2.execute("select encrypted, unencrypted from foo.bar where unencrypted = %s allow filtering", (expected,)).one()
        self.assertEqual(expected, encrypted)
        self.assertEqual(expected, unencrypted)

    def test_end_to_end_different_cle_contexts_different_policies(self):
        """
        Test to validate PYTHON-1356.  Class variables used to pass CLE policy down to protocol handler shouldn't persist.
        """

        expected = 3

        key = os.urandom(AES256_KEY_SIZE_BYTES)
        (col_desc, cl_policy) = self._create_policy(key)
        cluster = TestCluster(column_encryption_policy=cl_policy)
        session = cluster.connect()
        self._recreate_keyspace(session)

        # Use encode_and_encrypt helper function to populate date
        session.execute("insert into foo.bar (encrypted, unencrypted) values (%s,%s)", (cl_policy.encode_and_encrypt(col_desc, expected), expected))

        # We now open a new session _without_ the CLE policy specified.  We should _not_ be able to read decrypted bits from this session.
        cluster2 = TestCluster()
        session2 = cluster2.connect()

        # A straight select from the database will now return the decrypted bits.  We select both encrypted and unencrypted
        # values here to confirm that we don't interfere with regular processing of unencrypted vals.
        (encrypted, unencrypted) = session2.execute("select encrypted, unencrypted from foo.bar where unencrypted = %s allow filtering", (expected,)).one()
        self.assertEqual(cl_policy.encode_and_encrypt(col_desc, expected), encrypted)
        self.assertEqual(expected, unencrypted)

        # Confirm the same behaviour from a subsequent prepared statement as well
        prepared = session2.prepare("select encrypted, unencrypted from foo.bar where unencrypted = ? allow filtering")
        (encrypted, unencrypted) = session2.execute(prepared, [expected]).one()
        self.assertEqual(cl_policy.encode_and_encrypt(col_desc, expected), encrypted)
