# Copyright DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms
import unittest

import itertools

from cassandra.endpoint import DefaultEndPoint, SniEndPoint, SniEndPointFactory

from unittest.mock import patch


def socket_getaddrinfo(*args):
    return [
        (0, 0, 0, '', ('127.0.0.1', 30002)),
        (0, 0, 0, '', ('127.0.0.2', 30002)),
        (0, 0, 0, '', ('127.0.0.3', 30002))
    ]


@patch('socket.getaddrinfo', socket_getaddrinfo)
class SniEndPointTest(unittest.TestCase):

    endpoint_factory = SniEndPointFactory("proxy.datastax.com", 30002)

    def test_sni_endpoint_properties(self):

        endpoint = self.endpoint_factory.create_from_sni('test')
        self.assertEqual(endpoint.address, 'proxy.datastax.com')
        self.assertEqual(endpoint.port, 30002)
        self.assertEqual(endpoint._server_name, 'test')
        self.assertEqual(str(endpoint), 'proxy.datastax.com:30002:test')

    def test_endpoint_equality(self):
        self.assertNotEqual(
            DefaultEndPoint('10.0.0.1'),
            self.endpoint_factory.create_from_sni('10.0.0.1')
        )

        self.assertEqual(
            self.endpoint_factory.create_from_sni('10.0.0.1'),
            self.endpoint_factory.create_from_sni('10.0.0.1')
        )

        self.assertNotEqual(
            self.endpoint_factory.create_from_sni('10.0.0.1'),
            self.endpoint_factory.create_from_sni('10.0.0.0')
        )

        self.assertNotEqual(
            self.endpoint_factory.create_from_sni('10.0.0.1'),
            SniEndPointFactory("proxy.datastax.com", 9999).create_from_sni('10.0.0.1')
        )

    def test_endpoint_resolve(self):
        ips = ['127.0.0.1', '127.0.0.2', '127.0.0.3']
        it = itertools.cycle(ips)

        endpoint = self.endpoint_factory.create_from_sni('test')
        for i in range(10):
            (address, _) = endpoint.resolve()
            self.assertEqual(address, next(it))

    def test_sni_resolution_start_index(self):
        factory = SniEndPointFactory("proxy.datastax.com", 9999)
        initial_index = factory._init_index

        endpoint1 = factory.create_from_sni('sni1')
        self.assertEqual(factory._init_index, initial_index + 1)
        self.assertEqual(endpoint1._index, factory._init_index)

        endpoint2 = factory.create_from_sni('sni2')
        self.assertEqual(factory._init_index, initial_index + 2)
        self.assertEqual(endpoint2._index, factory._init_index)
