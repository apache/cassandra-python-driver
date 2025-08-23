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

from collections import namedtuple
from functools import lru_cache

import logging
import os

log = logging.getLogger(__name__)

from cassandra.cqltypes import _cqltypes
from cassandra.policies import ColumnEncryptionPolicy

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

AES256_BLOCK_SIZE = 128
AES256_BLOCK_SIZE_BYTES = int(AES256_BLOCK_SIZE / 8)
AES256_KEY_SIZE = 256
AES256_KEY_SIZE_BYTES = int(AES256_KEY_SIZE / 8)

ColData = namedtuple('ColData', ['key','type'])

class AES256ColumnEncryptionPolicy(ColumnEncryptionPolicy):

    # Fix block cipher mode for now.  IV size is a function of block cipher used
    # so fixing this avoids (possibly unnecessary) validation logic here.
    mode = modes.CBC

    # "iv" param here expects a bytearray that's the same size as the block
    # size for AES-256 (128 bits or 16 bytes).  If none is provided a new one
    # will be randomly generated, but in this case the IV should be recorded and
    # preserved or else you will not be able to decrypt any data encrypted by this
    # policy.
    def __init__(self, iv=None):

        # CBC uses an IV that's the same size as the block size
        #
        # Avoid defining IV with a default arg in order to stay away from
        # any issues around the caching of default args
        self.iv = iv
        if self.iv:
            if not len(self.iv) == AES256_BLOCK_SIZE_BYTES:
                raise ValueError("This policy uses AES-256 with CBC mode and therefore expects a 128-bit initialization vector")
        else:
            self.iv = os.urandom(AES256_BLOCK_SIZE_BYTES)

        # ColData for a given ColDesc is always preserved.  We only create a Cipher
        # when there's an actual need to for a given ColDesc
        self.coldata = {}
        self.ciphers = {}

    def encrypt(self, coldesc, obj_bytes):

        # AES256 has a 128-bit block size so if the input bytes don't align perfectly on
        # those blocks we have to pad them.  There's plenty of room for optimization here:
        #
        # * Instances of the PKCS7 padder should be managed in a bounded pool
        # * It would be nice if we could get a flag from encrypted data to indicate
        #   whether it was padded or not
        #   * Might be able to make this happen with a leading block of flags in encrypted data
        padder = padding.PKCS7(AES256_BLOCK_SIZE).padder()
        padded_bytes = padder.update(obj_bytes) + padder.finalize()

        cipher = self._get_cipher(coldesc)
        encryptor = cipher.encryptor()
        return self.iv + encryptor.update(padded_bytes) + encryptor.finalize()

    def decrypt(self, coldesc, bytes):

        iv = bytes[:AES256_BLOCK_SIZE_BYTES]
        encrypted_bytes = bytes[AES256_BLOCK_SIZE_BYTES:]
        cipher = self._get_cipher(coldesc, iv=iv)
        decryptor = cipher.decryptor()
        padded_bytes = decryptor.update(encrypted_bytes) + decryptor.finalize()

        unpadder = padding.PKCS7(AES256_BLOCK_SIZE).unpadder()
        return unpadder.update(padded_bytes) + unpadder.finalize()

    def add_column(self, coldesc, key, type):

        if not coldesc:
            raise ValueError("ColDesc supplied to add_column cannot be None")
        if not key:
            raise ValueError("Key supplied to add_column cannot be None")
        if not type:
            raise ValueError("Type supplied to add_column cannot be None")
        if type not in _cqltypes.keys():
            raise ValueError("Type %s is not a supported type".format(type))
        if not len(key) == AES256_KEY_SIZE_BYTES:
            raise ValueError("AES256 column encryption policy expects a 256-bit encryption key")
        self.coldata[coldesc] = ColData(key, _cqltypes[type])

    def contains_column(self, coldesc):
        return coldesc in self.coldata

    def encode_and_encrypt(self, coldesc, obj):
        if not coldesc:
            raise ValueError("ColDesc supplied to encode_and_encrypt cannot be None")
        if not obj:
            raise ValueError("Object supplied to encode_and_encrypt cannot be None")
        coldata = self.coldata.get(coldesc)
        if not coldata:
            raise ValueError("Could not find ColData for ColDesc %s".format(coldesc))
        return self.encrypt(coldesc, coldata.type.serialize(obj, None))

    def cache_info(self):
        return AES256ColumnEncryptionPolicy._build_cipher.cache_info()

    def column_type(self, coldesc):
        return self.coldata[coldesc].type

    def _get_cipher(self, coldesc, iv=None):
        """
        Access relevant state from this instance necessary to create a Cipher and then get one,
        hopefully returning a cached instance if we've already done so (and it hasn't been evicted)
        """
        try:
            coldata = self.coldata[coldesc]
            return AES256ColumnEncryptionPolicy._build_cipher(coldata.key, iv or self.iv)
        except KeyError:
            raise ValueError("Could not find column {}".format(coldesc))

    # Explicitly use a class method here to avoid caching self
    @lru_cache(maxsize=128)
    def _build_cipher(key, iv):
        return Cipher(algorithms.AES256(key), AES256ColumnEncryptionPolicy.mode(iv))
