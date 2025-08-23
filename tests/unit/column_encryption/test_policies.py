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

from cassandra.policies import ColDesc
from cassandra.column_encryption.policies import AES256ColumnEncryptionPolicy, \
    AES256_BLOCK_SIZE_BYTES, AES256_KEY_SIZE_BYTES

class AES256ColumnEncryptionPolicyTest(unittest.TestCase):

    def _random_block(self):
        return os.urandom(AES256_BLOCK_SIZE_BYTES)

    def _random_key(self):
        return os.urandom(AES256_KEY_SIZE_BYTES)

    def _test_round_trip(self, bytes):
        coldesc = ColDesc('ks1','table1','col1')
        policy = AES256ColumnEncryptionPolicy()
        policy.add_column(coldesc, self._random_key(), "blob")
        encrypted_bytes = policy.encrypt(coldesc, bytes)
        self.assertEqual(bytes, policy.decrypt(coldesc, encrypted_bytes))

    def test_no_padding_necessary(self):
        self._test_round_trip(self._random_block())

    def test_some_padding_required(self):
        for byte_size in range(1,AES256_BLOCK_SIZE_BYTES - 1):
            bytes = os.urandom(byte_size)
            self._test_round_trip(bytes)
        for byte_size in range(AES256_BLOCK_SIZE_BYTES + 1,(2 * AES256_BLOCK_SIZE_BYTES) - 1):
            bytes = os.urandom(byte_size)
            self._test_round_trip(bytes)

    def test_add_column_invalid_key_size_raises(self):
        coldesc = ColDesc('ks1','table1','col1')
        policy = AES256ColumnEncryptionPolicy()
        for key_size in range(1,AES256_KEY_SIZE_BYTES - 1):
            with self.assertRaises(ValueError):
                policy.add_column(coldesc, os.urandom(key_size), "blob")
        for key_size in range(AES256_KEY_SIZE_BYTES + 1,(2 * AES256_KEY_SIZE_BYTES) - 1):
            with self.assertRaises(ValueError):
                policy.add_column(coldesc, os.urandom(key_size), "blob")

    def test_add_column_invalid_iv_size_raises(self):
        def test_iv_size(iv_size):
                policy = AES256ColumnEncryptionPolicy(iv = os.urandom(iv_size))
                policy.add_column(coldesc, os.urandom(AES256_KEY_SIZE_BYTES), "blob")
                policy.encrypt(coldesc, os.urandom(128))

        coldesc = ColDesc('ks1','table1','col1')
        for iv_size in range(1,AES256_BLOCK_SIZE_BYTES - 1):
            with self.assertRaises(ValueError):
                test_iv_size(iv_size)
        for iv_size in range(AES256_BLOCK_SIZE_BYTES + 1,(2 * AES256_BLOCK_SIZE_BYTES) - 1):
            with self.assertRaises(ValueError):
                test_iv_size(iv_size)

        # Finally, confirm that the expected IV size has no issue
        test_iv_size(AES256_BLOCK_SIZE_BYTES)

    def test_add_column_null_coldesc_raises(self):
        with self.assertRaises(ValueError):
            policy = AES256ColumnEncryptionPolicy()
            policy.add_column(None, self._random_block(), "blob")

    def test_add_column_null_key_raises(self):
        with self.assertRaises(ValueError):
            policy = AES256ColumnEncryptionPolicy()
            coldesc = ColDesc('ks1','table1','col1')
            policy.add_column(coldesc, None, "blob")

    def test_add_column_null_type_raises(self):
        with self.assertRaises(ValueError):
            policy = AES256ColumnEncryptionPolicy()
            coldesc = ColDesc('ks1','table1','col1')
            policy.add_column(coldesc, self._random_block(), None)

    def test_add_column_unknown_type_raises(self):
        with self.assertRaises(ValueError):
            policy = AES256ColumnEncryptionPolicy()
            coldesc = ColDesc('ks1','table1','col1')
            policy.add_column(coldesc, self._random_block(), "foobar")

    def test_encode_and_encrypt_null_coldesc_raises(self):
        with self.assertRaises(ValueError):
            policy = AES256ColumnEncryptionPolicy()
            coldesc = ColDesc('ks1','table1','col1')
            policy.add_column(coldesc, self._random_key(), "blob")
            policy.encode_and_encrypt(None, self._random_block())

    def test_encode_and_encrypt_null_obj_raises(self):
        with self.assertRaises(ValueError):
            policy = AES256ColumnEncryptionPolicy()
            coldesc = ColDesc('ks1','table1','col1')
            policy.add_column(coldesc, self._random_key(), "blob")
            policy.encode_and_encrypt(coldesc, None)

    def test_encode_and_encrypt_unknown_coldesc_raises(self):
        with self.assertRaises(ValueError):
            policy = AES256ColumnEncryptionPolicy()
            coldesc = ColDesc('ks1','table1','col1')
            policy.add_column(coldesc, self._random_key(), "blob")
            policy.encode_and_encrypt(ColDesc('ks2','table2','col2'), self._random_block())

    def test_contains_column(self):
        coldesc = ColDesc('ks1','table1','col1')
        policy = AES256ColumnEncryptionPolicy()
        policy.add_column(coldesc, self._random_key(), "blob")
        self.assertTrue(policy.contains_column(coldesc))
        self.assertFalse(policy.contains_column(ColDesc('ks2','table1','col1')))
        self.assertFalse(policy.contains_column(ColDesc('ks1','table2','col1')))
        self.assertFalse(policy.contains_column(ColDesc('ks1','table1','col2')))
        self.assertFalse(policy.contains_column(ColDesc('ks2','table2','col2')))

    def test_encrypt_unknown_column(self):
        with self.assertRaises(ValueError):
            policy = AES256ColumnEncryptionPolicy()
            coldesc = ColDesc('ks1','table1','col1')
            policy.add_column(coldesc, self._random_key(), "blob")
            policy.encrypt(ColDesc('ks2','table2','col2'), self._random_block())

    def test_decrypt_unknown_column(self):
        policy = AES256ColumnEncryptionPolicy()
        coldesc = ColDesc('ks1','table1','col1')
        policy.add_column(coldesc, self._random_key(), "blob")
        encrypted_bytes = policy.encrypt(coldesc, self._random_block())
        with self.assertRaises(ValueError):
            policy.decrypt(ColDesc('ks2','table2','col2'), encrypted_bytes)

    def test_cache_info(self):
        # Exclude any interference from tests above
        AES256ColumnEncryptionPolicy._build_cipher.cache_clear()

        coldesc1 = ColDesc('ks1','table1','col1')
        coldesc2 = ColDesc('ks2','table2','col2')
        coldesc3 = ColDesc('ks3','table3','col3')
        policy = AES256ColumnEncryptionPolicy()
        for coldesc in [coldesc1, coldesc2, coldesc3]:
            policy.add_column(coldesc, self._random_key(), "blob")

        # First run for this coldesc should be a miss, everything else should be a cache hit
        for _ in range(10):
            policy.encrypt(coldesc1, self._random_block())
        cache_info = policy.cache_info()
        self.assertEqual(cache_info.hits, 9)
        self.assertEqual(cache_info.misses, 1)
        self.assertEqual(cache_info.maxsize, 128)

        # Important note: we're measuring the size of the cache of ciphers, NOT stored
        # keys.  We won't have a cipher here until we actually encrypt something
        self.assertEqual(cache_info.currsize, 1)
        policy.encrypt(coldesc2, self._random_block())
        self.assertEqual(policy.cache_info().currsize, 2)
        policy.encrypt(coldesc3, self._random_block())
        self.assertEqual(policy.cache_info().currsize, 3)
