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

import struct
import math
from cassandra.cqltypes import lookup_casstype
from cassandra.protocol import ProtocolVersion
from cassandra.cqltypes import PointType, LineStringType, PolygonType, WKBGeometryType
from cassandra.util import Point, LineString, Polygon, _LinearRing, Distance, _HAS_GEOMET

wkb_be = 0
wkb_le = 1

protocol_versions = ProtocolVersion.SUPPORTED_VERSIONS


class GeoTypes(unittest.TestCase):

    samples = (Point(1, 2), LineString(((1, 2), (3, 4), (5, 6))), Polygon([(10.1, 10.0), (110.0, 10.0), (110., 110.0), (10., 110.0), (10., 10.0)], [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)], [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]]))

    def test_marshal_platform(self):
        for proto_ver in protocol_versions:
            for geo in self.samples:
                cql_type = lookup_casstype(geo.__class__.__name__ + 'Type')
                self.assertEqual(cql_type.from_binary(cql_type.to_binary(geo, proto_ver), proto_ver), geo)

    def _verify_both_endian(self, typ, body_fmt, params, expected):
        for proto_ver in protocol_versions:
            self.assertEqual(typ.from_binary(struct.pack(">BI" + body_fmt, wkb_be, *params), proto_ver), expected)
            self.assertEqual(typ.from_binary(struct.pack("<BI" + body_fmt, wkb_le, *params), proto_ver), expected)

    def test_both_endian(self):
        self._verify_both_endian(PointType, "dd", (WKBGeometryType.POINT, 1, 2), Point(1, 2))
        self._verify_both_endian(LineStringType, "Idddddd", (WKBGeometryType.LINESTRING, 3, 1, 2, 3, 4, 5, 6), LineString(((1, 2), (3, 4), (5, 6))))
        self._verify_both_endian(PolygonType, "IIdddddd", (WKBGeometryType.POLYGON, 1, 3, 1, 2, 3, 4, 5, 6), Polygon(((1, 2), (3, 4), (5, 6))))

    def test_empty_wkb(self):
        for cls in (LineString, Polygon):
            class_name = cls.__name__
            cql_type = lookup_casstype(class_name + 'Type')
            self.assertEqual(str(cql_type.from_binary(cql_type.to_binary(cls(), 0), 0)), class_name.upper() + " EMPTY")
        self.assertEqual(str(PointType.from_binary(PointType.to_binary(Point(), 0), 0)), "POINT (nan nan)")

    def test_str_wkt(self):
        self.assertEqual(str(Point(1., 2.)), 'POINT (1.0 2.0)')
        self.assertEqual(str(Point()), "POINT (nan nan)")
        self.assertEqual(str(LineString(((1., 2.), (3., 4.), (5., 6.)))), 'LINESTRING (1.0 2.0, 3.0 4.0, 5.0 6.0)')
        self.assertEqual(str(_LinearRing(((1., 2.), (3., 4.), (5., 6.)))), 'LINEARRING (1.0 2.0, 3.0 4.0, 5.0 6.0)')
        self.assertEqual(str(Polygon([(10.1, 10.0), (110.0, 10.0), (110., 110.0), (10., 110.0), (10., 10.0)],
                                     [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)],
                                      [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]])),
                         'POLYGON ((10.1 10.0, 110.0 10.0, 110.0 110.0, 10.0 110.0, 10.0 10.0), (20.0 20.0, 20.0 30.0, 30.0 30.0, 30.0 20.0, 20.0 20.0), (40.0 20.0, 40.0 30.0, 50.0 30.0, 50.0 20.0, 40.0 20.0))')

        class LinearRing(_LinearRing):
            pass
        for cls in (LineString, LinearRing, Polygon):
            self.assertEqual(str(cls()), cls.__name__.upper() + " EMPTY")

    def test_repr(self):
        for geo in (Point(1., 2.),
                    LineString(((1., 2.), (3., 4.), (5., 6.))),
                    _LinearRing(((1., 2.), (3., 4.), (5., 6.))),
                    Polygon([(10.1, 10.0), (110.0, 10.0), (110., 110.0), (10., 110.0), (10., 10.0)],
                            [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)],
                             [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]])):
            self.assertEqual(eval(repr(geo)), geo)

    def test_hash(self):
        for geo in (Point(1., 2.),
                    LineString(((1., 2.), (3., 4.), (5., 6.))),
                    _LinearRing(((1., 2.), (3., 4.), (5., 6.))),
                    Polygon([(10.1, 10.0), (110.0, 10.0), (110., 110.0), (10., 110.0), (10., 10.0)],
                            [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)],
                             [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]])):
            self.assertEqual(len(set((geo, geo))), 1)

    def test_eq(self):
        for geo in (Point(1., 2.),
                    LineString(((1., 2.), (3., 4.), (5., 6.))),
                    _LinearRing(((1., 2.), (3., 4.), (5., 6.))),
                    Polygon([(10.1, 10.0), (110.0, 10.0), (110., 110.0), (10., 110.0), (10., 10.0)],
                            [[(20., 20.0), (20., 30.0), (30., 30.0), (30., 20.0), (20., 20.0)],
                             [(40., 20.0), (40., 30.0), (50., 30.0), (50., 20.0), (40., 20.0)]])):
            # same type
            self.assertEqual(geo, geo)

            # does not blow up on other types
            # specifically use assertFalse(eq) to make sure we're using the geo __eq__ operator
            self.assertFalse(geo == object())

@unittest.skipUnless(_HAS_GEOMET, "Skip wkt geometry tests when geomet is not installed")
class WKTTest(unittest.TestCase):

    def test_line_parse(self):
        """
        This test exercises the parsing logic our LINESTRING WKT object
        @since 1.2
        @jira_ticket PYTHON-641
        @test_category dse geometric
        @expected_result We should be able to form LINESTRINGS objects from properly formatted WKT strings
        """

        # Test simple line string
        ls = "LINESTRING (1.0 2.0, 3.0 4.0, 5.0 6.0)"
        lo = LineString.from_wkt(ls)
        lo_expected_cords = ((1.0, 2.0), (3.0, 4.0), (5.0, 6.0))
        self.assertEqual(lo.coords, lo_expected_cords)

        # Test very long line string
        long_ls = self._construct_line_string(10000)
        long_lo = LineString.from_wkt(long_ls)
        self.assertEqual(len(long_lo.coords), 10000)
        self.assertEqual(long_lo.coords, self._construct_line_string_expected_cords(10000))

        # Test line string with negative numbers
        ls = "LINESTRING (-1.3 1.2, 3.23 -4.54, 1.34 -9.26)"
        lo = LineString.from_wkt(ls)
        lo_expected_cords = ((-1.3, 1.2), (3.23, -4.54), (1.34, -9.26))
        self.assertEqual(lo.coords, lo_expected_cords)

        # Test bad line strings
        bls = "LINESTRIN (1.0 2.0, 3.0 4.0, 5.0 6.0)"
        with self.assertRaises(ValueError):
            blo = LineString.from_wkt(bls)
        bls = "LINESTRING (1.0 2.0 3.0 4.0 5.0"
        with self.assertRaises(ValueError):
            blo = LineString.from_wkt(bls)

        # Test with NAN
        ls = "LINESTRING (NAN NAN, NAN NAN)"
        lo = LineString.from_wkt(ls)
        self.assertEqual(len(lo.coords), 2)
        for cords in lo.coords:
            for cord in cords:
                self.assertTrue(math.isnan(cord))

    def test_distance_parse(self):
        """
        This test exercises the parsing logic our Distance WKT object
        @since 1.2
        @jira_ticket PYTHON-670
        @test_category dse geometric
        @expected_result We should be able to form Distance objects from properly formatted WKT strings
        """

        ds = "DISTANCE ((12, 10) 3)"
        do = Distance(12, 10, 3)
        self.assertEqual(do.x, 12)
        self.assertEqual(do.y, 10)
        self.assertEqual(do.radius, 3)
        # Test bad distance strings

        bds = "DISTANCE ((1.0 2.0))"
        with self.assertRaises(ValueError):
            bdo = Distance.from_wkt(bds)
        bps = "DISTANCE ((1.0 2.0 3.0 4.0 5.0)"
        with self.assertRaises(ValueError):
            bdo = Distance.from_wkt(bds)

        # NAN isn't supported, truncating not supported

    def test_point_parse(self):
        """
        This test exercises the parsing logic our POINT WKT object
        @since 1.2
        @jira_ticket PYTHON-641
        @test_category dse geometric
        @expected_result We should be able to form POINT objects from properly formatted WKT strings
        """

        # Test basic point
        ps = "POINT (1.0 2.0)"
        po = Point.from_wkt(ps)
        self.assertEqual(po.x, 1.0)
        self.assertEqual(po.y, 2.0)

        # Test bad point strings
        bps = "POIN (1.0 2.0)"
        with self.assertRaises(ValueError):
            bpo = Point.from_wkt(bps)
        bps = "POINT (1.0 2.0 3.0 4.0 5.0"
        with self.assertRaises(ValueError):
            bpo = Point.from_wkt(bps)

        # Points get truncated automatically
        tps = "POINT (9.0 2.0 3.0 4.0 5.0)"
        tpo = Point.from_wkt(tps)
        self.assertEqual(tpo.x, 9.0)
        self.assertEqual(tpo.y, 2.0)

        # Test point with NAN
        ps = "POINT (NAN NAN)"
        po = Point.from_wkt(ps)
        self.assertTrue(math.isnan(po.x))
        self.assertTrue(math.isnan(po.y))

    def test_polygon_parse(self):
        """
        This test exercises the parsing logic our POLYGON WKT object
        @since 1.2
        @jira_ticket PYTHON-641
        @test_category dse geometric
        @expected_result We should be able to form POLYGON objects from properly formatted WKT strings
        """

        example_poly_string = 'POLYGON ((10.1 10.0, 110.0 10.0, 110.0 110.0, 10.0 110.0, 10.0 10.0), (20.0 20.0, 20.0 30.0, 30.0 30.0, 30.0 20.0, 20.0 20.0), (40.0 20.0, 40.0 30.0, 50.0 30.0, 50.0 20.0, 40.0 20.0))'
        poly_obj = Polygon.from_wkt(example_poly_string)
        expected_ex_coords = ((10.1, 10.0), (110.0, 10.0), (110.0, 110.0), (10.0, 110.0), (10.0, 10.0))
        expected_in_coords_1 = ((20.0, 20.0), (20.0, 30.0), (30.0, 30.0), (30.0, 20.0), (20.0, 20.0))
        expected_in_coords_2 = ((40.0, 20.0), (40.0, 30.0), (50.0, 30.0), (50.0, 20.0), (40.0, 20.0))
        self.assertEqual(poly_obj.exterior.coords, expected_ex_coords)
        self.assertEqual(len(poly_obj.interiors), 2)
        self.assertEqual(poly_obj.interiors[0].coords, expected_in_coords_1)
        self.assertEqual(poly_obj.interiors[1].coords, expected_in_coords_2)

        # Test with very long polygon
        long_poly_string = self._construct_polygon_string(10000)
        long_poly_obj = Polygon.from_wkt(long_poly_string)
        self.assertEqual(len(long_poly_obj.exterior.coords), 10000)
        #for expected, recieved in zip(self._construct_line_string_expected_cords(10000), long_poly_obj.exterior.coords):
        #    self.assertEqual(expected, recieved)
        self.assertEqual(long_poly_obj.exterior.coords, self._construct_line_string_expected_cords(10000))

        # Test bad polygon strings
        bps = "POLYGONE ((30 10, 40 40, 20 40, 10 20, 30 10))"
        with self.assertRaises(ValueError):
            bpo = Polygon.from_wkt(bps)
        bps = "POLYGON (30 10, 40 40, 20 40, 10 20, 30 10)"
        with self.assertRaises(ValueError):
            bpo = Polygon.from_wkt(bps)

        # Polygons get truncated automatically
        ps = "POLYGON ((30 10, 40 40, 20, 10 20, 30))"
        po = Polygon.from_wkt(ps)
        expected_ex_coords = ((30.0, 10.0), (40.0, 40.0), (20.0,), (10.0, 20.0), (30.0,))
        self.assertEqual(po.exterior.coords, expected_ex_coords)

        # Test Polygon with NAN
        ps = "POLYGON ((NAN NAN, NAN NAN, NAN NAN, NAN, NAN))"
        po = Polygon.from_wkt(ps)
        for cords in po.exterior.coords:
            for cord in cords:
                self.assertTrue(math.isnan(cord))

    def _construct_line_string(self, num_of_points):
        # Constructs a arbitrarily long line string
        ls = "LINESTRING ("
        for i in range(0, num_of_points):
            ls += str(i)+" "+str(i)+', '
        return ls.rstrip(', ')+")"

    def _construct_line_string_expected_cords(self, num_of_points):
        # Constructs the corresponding expected coordinates for a linge string, and polygon string
        coords = []

        for i in range(0, num_of_points):
            coords.append((i, i))
        return tuple(coords)

    def _construct_polygon_string(self, num_of_points):
        # Constructs a arbitrarily long polygpn string
        ls = "POLYGON (("
        for i in range(0, num_of_points):
            ls += str(i)+" "+str(i)+', '
        return ls.rstrip(', ')+"))"
