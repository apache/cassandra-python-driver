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

import zlib

from cassandra import DriverException
from cassandra.marshal import int32_pack
from cassandra.protocol import write_uint_le, read_uint_le

CRC24_INIT = 0x875060
CRC24_POLY = 0x1974F0B
CRC24_LENGTH = 3
CRC32_LENGTH = 4
CRC32_INITIAL = zlib.crc32(b"\xfa\x2d\x55\xca")


class CrcException(Exception):
    """
    CRC mismatch error.

    TODO: here to avoid import cycles with cassandra.connection. In the next
          major, the exceptions should be declared in a separated exceptions.py
          file.
    """
    pass


def compute_crc24(data, length):
    crc = CRC24_INIT

    for _ in range(length):
        crc ^= (data & 0xff) << 16
        data >>= 8

        for i in range(8):
            crc <<= 1
            if crc & 0x1000000 != 0:
                crc ^= CRC24_POLY

    return crc


def compute_crc32(data, value):
    crc32 = zlib.crc32(data, value)
    return crc32


class SegmentHeader(object):

    payload_length = None
    uncompressed_payload_length = None
    is_self_contained = None

    def __init__(self, payload_length, uncompressed_payload_length, is_self_contained):
        self.payload_length = payload_length
        self.uncompressed_payload_length = uncompressed_payload_length
        self.is_self_contained = is_self_contained

    @property
    def segment_length(self):
        """
        Return the total length of the segment, including the CRC.
        """
        hl = SegmentCodec.UNCOMPRESSED_HEADER_LENGTH if self.uncompressed_payload_length < 1 \
            else SegmentCodec.COMPRESSED_HEADER_LENGTH
        return hl + CRC24_LENGTH + self.payload_length + CRC32_LENGTH


class Segment(object):

    MAX_PAYLOAD_LENGTH = 128 * 1024 - 1

    payload = None
    is_self_contained = None

    def __init__(self, payload, is_self_contained):
        self.payload = payload
        self.is_self_contained = is_self_contained


class SegmentCodec(object):

    COMPRESSED_HEADER_LENGTH = 5
    UNCOMPRESSED_HEADER_LENGTH = 3
    FLAG_OFFSET = 17

    compressor = None
    decompressor = None

    def __init__(self, compressor=None, decompressor=None):
        self.compressor = compressor
        self.decompressor = decompressor

    @property
    def header_length(self):
        return self.COMPRESSED_HEADER_LENGTH if self.compression \
            else self.UNCOMPRESSED_HEADER_LENGTH

    @property
    def header_length_with_crc(self):
        return (self.COMPRESSED_HEADER_LENGTH if self.compression
                else self.UNCOMPRESSED_HEADER_LENGTH) + CRC24_LENGTH

    @property
    def compression(self):
        return self.compressor and self.decompressor

    def compress(self, data):
        # the uncompressed length is already encoded in the header, so
        # we remove it here
        return self.compressor(data)[4:]

    def decompress(self, encoded_data, uncompressed_length):
        return self.decompressor(int32_pack(uncompressed_length) + encoded_data)

    def encode_header(self, buffer, payload_length, uncompressed_length, is_self_contained):
        if payload_length > Segment.MAX_PAYLOAD_LENGTH:
            raise DriverException('Payload length exceed Segment.MAX_PAYLOAD_LENGTH')

        header_data = payload_length

        flag_offset = self.FLAG_OFFSET
        if self.compression:
            header_data |= uncompressed_length << flag_offset
            flag_offset += 17

        if is_self_contained:
            header_data |= 1 << flag_offset

        write_uint_le(buffer, header_data, size=self.header_length)
        header_crc = compute_crc24(header_data, self.header_length)
        write_uint_le(buffer, header_crc, size=CRC24_LENGTH)

    def _encode_segment(self, buffer, payload, is_self_contained):
        """
        Encode a message to a single segment.
        """
        uncompressed_payload = payload
        uncompressed_payload_length = len(payload)

        if self.compression:
            compressed_payload = self.compress(uncompressed_payload)
            if len(compressed_payload) >= uncompressed_payload_length:
                encoded_payload = uncompressed_payload
                uncompressed_payload_length = 0
            else:
                encoded_payload = compressed_payload
        else:
            encoded_payload = uncompressed_payload

        payload_length = len(encoded_payload)
        self.encode_header(buffer, payload_length, uncompressed_payload_length, is_self_contained)
        payload_crc = compute_crc32(encoded_payload, CRC32_INITIAL)
        buffer.write(encoded_payload)
        write_uint_le(buffer, payload_crc)

    def encode(self, buffer, msg):
        """
        Encode a message to one of more segments.
        """
        msg_length = len(msg)

        if msg_length > Segment.MAX_PAYLOAD_LENGTH:
            payloads = []
            for i in range(0, msg_length, Segment.MAX_PAYLOAD_LENGTH):
                payloads.append(msg[i:i + Segment.MAX_PAYLOAD_LENGTH])
        else:
            payloads = [msg]

        is_self_contained = len(payloads) == 1
        for payload in payloads:
            self._encode_segment(buffer, payload, is_self_contained)

    def decode_header(self, buffer):
        header_data = read_uint_le(buffer, self.header_length)

        expected_header_crc = read_uint_le(buffer, CRC24_LENGTH)
        actual_header_crc = compute_crc24(header_data, self.header_length)
        if actual_header_crc != expected_header_crc:
            raise CrcException('CRC mismatch on header {:x}. Received {:x}", computed {:x}.'.format(
                header_data, expected_header_crc, actual_header_crc))

        payload_length = header_data & Segment.MAX_PAYLOAD_LENGTH
        header_data >>= 17

        if self.compression:
            uncompressed_payload_length = header_data & Segment.MAX_PAYLOAD_LENGTH
            header_data >>= 17
        else:
            uncompressed_payload_length = -1

        is_self_contained = (header_data & 1) == 1

        return SegmentHeader(payload_length, uncompressed_payload_length, is_self_contained)

    def decode(self, buffer, header):
        encoded_payload = buffer.read(header.payload_length)
        expected_payload_crc = read_uint_le(buffer)

        actual_payload_crc = compute_crc32(encoded_payload, CRC32_INITIAL)
        if actual_payload_crc != expected_payload_crc:
            raise CrcException('CRC mismatch on payload. Received {:x}", computed {:x}.'.format(
                expected_payload_crc, actual_payload_crc))

        payload = encoded_payload
        if self.compression and header.uncompressed_payload_length > 0:
            payload = self.decompress(encoded_payload, header.uncompressed_payload_length)

        return Segment(payload, header.is_self_contained)
