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

from io import BytesIO

from cassandra import DriverException
from cassandra.segment import Segment, CrcException
from cassandra.connection import segment_codec_no_compression, segment_codec_lz4


def to_bits(b):
    return '{:08b}'.format(b)


class SegmentCodecTest(unittest.TestCase):

    small_msg = b'b' * 50
    max_msg = b'b' * Segment.MAX_PAYLOAD_LENGTH
    large_msg = b'b' * (Segment.MAX_PAYLOAD_LENGTH + 1)

    @staticmethod
    def _header_to_bits(data):
        # unpack a header to bits
        # data should be the little endian bytes sequence
        if len(data) > 6:  # compressed
            data = data[:5]
            bits = ''.join([to_bits(b) for b in reversed(data)])
            # return the compressed payload length, the uncompressed payload length,
            # the self-contained flag and the padding as bits
            return bits[23:40] + bits[6:23] + bits[5:6] + bits[:5]
        else:  # uncompressed
            data = data[:3]
            bits = ''.join([to_bits(b) for b in reversed(data)])
            # return the payload length, the self-contained flag and
            # the padding as bits
            return bits[7:24] + bits[6:7] + bits[:6]

    def test_encode_uncompressed_header(self):
        buffer = BytesIO()
        segment_codec_no_compression.encode_header(buffer, len(self.small_msg), -1, True)
        self.assertEqual(buffer.tell(), 6)
        self.assertEqual(
            self._header_to_bits(buffer.getvalue()),
            "00000000000110010" + "1" + "000000")

    @unittest.skipUnless(segment_codec_lz4, ' lz4 not installed')
    def test_encode_compressed_header(self):
        buffer = BytesIO()
        compressed_length = len(segment_codec_lz4.compress(self.small_msg))
        segment_codec_lz4.encode_header(buffer, compressed_length, len(self.small_msg), True)

        self.assertEqual(buffer.tell(), 8)
        self.assertEqual(
            self._header_to_bits(buffer.getvalue()),
            "{:017b}".format(compressed_length) + "00000000000110010" + "1" + "00000")

    def test_encode_uncompressed_header_with_max_payload(self):
        buffer = BytesIO()
        segment_codec_no_compression.encode_header(buffer, len(self.max_msg), -1, True)
        self.assertEqual(buffer.tell(), 6)
        self.assertEqual(
            self._header_to_bits(buffer.getvalue()),
            "11111111111111111" + "1" + "000000")

    def test_encode_header_fails_if_payload_too_big(self):
        buffer = BytesIO()
        for codec in [c for c in [segment_codec_no_compression, segment_codec_lz4] if c is not None]:
            with self.assertRaises(DriverException):
                codec.encode_header(buffer, len(self.large_msg), -1, False)

    def test_encode_uncompressed_header_not_self_contained_msg(self):
        buffer = BytesIO()
        # simulate the first chunk with the max size
        segment_codec_no_compression.encode_header(buffer, len(self.max_msg), -1, False)
        self.assertEqual(buffer.tell(), 6)
        self.assertEqual(
            self._header_to_bits(buffer.getvalue()),
            ("11111111111111111"
             "0"  # not self-contained
             "000000"))

    @unittest.skipUnless(segment_codec_lz4, ' lz4 not installed')
    def test_encode_compressed_header_with_max_payload(self):
        buffer = BytesIO()
        compressed_length = len(segment_codec_lz4.compress(self.max_msg))
        segment_codec_lz4.encode_header(buffer, compressed_length, len(self.max_msg), True)
        self.assertEqual(buffer.tell(), 8)
        self.assertEqual(
            self._header_to_bits(buffer.getvalue()),
            "{:017b}".format(compressed_length) + "11111111111111111" + "1" + "00000")

    @unittest.skipUnless(segment_codec_lz4, ' lz4 not installed')
    def test_encode_compressed_header_not_self_contained_msg(self):
        buffer = BytesIO()
        # simulate the first chunk with the max size
        compressed_length = len(segment_codec_lz4.compress(self.max_msg))
        segment_codec_lz4.encode_header(buffer, compressed_length, len(self.max_msg), False)
        self.assertEqual(buffer.tell(), 8)
        self.assertEqual(
            self._header_to_bits(buffer.getvalue()),
            ("{:017b}".format(compressed_length) +
             "11111111111111111"
             "0"  # not self-contained
             "00000"))

    def test_decode_uncompressed_header(self):
        buffer = BytesIO()
        segment_codec_no_compression.encode_header(buffer, len(self.small_msg), -1, True)
        buffer.seek(0)
        header = segment_codec_no_compression.decode_header(buffer)
        self.assertEqual(header.uncompressed_payload_length, -1)
        self.assertEqual(header.payload_length, len(self.small_msg))
        self.assertEqual(header.is_self_contained, True)

    @unittest.skipUnless(segment_codec_lz4, ' lz4 not installed')
    def test_decode_compressed_header(self):
        buffer = BytesIO()
        compressed_length = len(segment_codec_lz4.compress(self.small_msg))
        segment_codec_lz4.encode_header(buffer, compressed_length, len(self.small_msg), True)
        buffer.seek(0)
        header = segment_codec_lz4.decode_header(buffer)
        self.assertEqual(header.uncompressed_payload_length, len(self.small_msg))
        self.assertEqual(header.payload_length, compressed_length)
        self.assertEqual(header.is_self_contained, True)

    def test_decode_header_fails_if_corrupted(self):
        buffer = BytesIO()
        segment_codec_no_compression.encode_header(buffer, len(self.small_msg), -1, True)
        # corrupt one byte
        buffer.seek(buffer.tell()-1)
        buffer.write(b'0')
        buffer.seek(0)

        with self.assertRaises(CrcException):
            segment_codec_no_compression.decode_header(buffer)

    def test_decode_uncompressed_self_contained_segment(self):
        buffer = BytesIO()
        segment_codec_no_compression.encode(buffer, self.small_msg)

        buffer.seek(0)
        header = segment_codec_no_compression.decode_header(buffer)
        segment = segment_codec_no_compression.decode(buffer, header)

        self.assertEqual(header.is_self_contained, True)
        self.assertEqual(header.uncompressed_payload_length, -1)
        self.assertEqual(header.payload_length, len(self.small_msg))
        self.assertEqual(segment.payload, self.small_msg)

    @unittest.skipUnless(segment_codec_lz4, ' lz4 not installed')
    def test_decode_compressed_self_contained_segment(self):
        buffer = BytesIO()
        segment_codec_lz4.encode(buffer, self.small_msg)

        buffer.seek(0)
        header = segment_codec_lz4.decode_header(buffer)
        segment = segment_codec_lz4.decode(buffer, header)

        self.assertEqual(header.is_self_contained, True)
        self.assertEqual(header.uncompressed_payload_length, len(self.small_msg))
        self.assertGreater(header.uncompressed_payload_length, header.payload_length)
        self.assertEqual(segment.payload, self.small_msg)

    def test_decode_multi_segments(self):
        buffer = BytesIO()
        segment_codec_no_compression.encode(buffer, self.large_msg)

        buffer.seek(0)
        # We should have 2 segments to read
        headers = []
        segments = []
        headers.append(segment_codec_no_compression.decode_header(buffer))
        segments.append(segment_codec_no_compression.decode(buffer, headers[0]))
        headers.append(segment_codec_no_compression.decode_header(buffer))
        segments.append(segment_codec_no_compression.decode(buffer, headers[1]))

        self.assertTrue(all([h.is_self_contained is False for h in headers]))
        decoded_msg = segments[0].payload + segments[1].payload
        self.assertEqual(decoded_msg, self.large_msg)

    @unittest.skipUnless(segment_codec_lz4, ' lz4 not installed')
    def test_decode_fails_if_corrupted(self):
        buffer = BytesIO()
        segment_codec_lz4.encode(buffer, self.small_msg)
        buffer.seek(buffer.tell()-1)
        buffer.write(b'0')
        buffer.seek(0)
        header = segment_codec_lz4.decode_header(buffer)
        with self.assertRaises(CrcException):
            segment_codec_lz4.decode(buffer, header)

    @unittest.skipUnless(segment_codec_lz4, ' lz4 not installed')
    def test_decode_tiny_msg_not_compressed(self):
        buffer = BytesIO()
        segment_codec_lz4.encode(buffer, b'b')
        buffer.seek(0)
        header = segment_codec_lz4.decode_header(buffer)
        segment = segment_codec_lz4.decode(buffer, header)
        self.assertEqual(header.uncompressed_payload_length, 0)
        self.assertEqual(header.payload_length, 1)
        self.assertEqual(segment.payload, b'b')
