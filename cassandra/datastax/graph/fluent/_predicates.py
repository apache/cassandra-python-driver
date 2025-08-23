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

import math

from gremlin_python.process.traversal import P

from cassandra.util import Distance

__all__ = ['GeoP', 'TextDistanceP', 'Search', 'GeoUnit', 'Geo', 'CqlCollection']


class GeoP(object):

    def __init__(self, operator, value, other=None):
        self.operator = operator
        self.value = value
        self.other = other

    @staticmethod
    def inside(*args, **kwargs):
        return GeoP("inside", *args, **kwargs)

    def __eq__(self, other):
        return isinstance(other,
                          self.__class__) and self.operator == other.operator and self.value == other.value and self.other == other.other

    def __repr__(self):
        return self.operator + "(" + str(self.value) + ")" if self.other is None else self.operator + "(" + str(
            self.value) + "," + str(self.other) + ")"


class TextDistanceP(object):

    def __init__(self, operator, value, distance):
        self.operator = operator
        self.value = value
        self.distance = distance

    @staticmethod
    def fuzzy(*args):
        return TextDistanceP("fuzzy", *args)

    @staticmethod
    def token_fuzzy(*args):
        return TextDistanceP("tokenFuzzy", *args)

    @staticmethod
    def phrase(*args):
        return TextDistanceP("phrase", *args)

    def __eq__(self, other):
        return isinstance(other,
                          self.__class__) and self.operator == other.operator and self.value == other.value and self.distance == other.distance

    def __repr__(self):
        return self.operator + "(" + str(self.value) + "," + str(self.distance) + ")"


class Search(object):

    @staticmethod
    def token(value):
        """
        Search any instance of a certain token within the text property targeted.
        :param value: the value to look for.
        """
        return P('token', value)

    @staticmethod
    def token_prefix(value):
        """
        Search any instance of a certain token prefix withing the text property targeted.
        :param value: the value to look for.
        """
        return P('tokenPrefix', value)

    @staticmethod
    def token_regex(value):
        """
        Search any instance of the provided regular expression for the targeted property.
        :param value: the value to look for.
        """
        return P('tokenRegex', value)

    @staticmethod
    def prefix(value):
        """
        Search for a specific prefix at the beginning of the text property targeted.
        :param value: the value to look for.
        """
        return P('prefix', value)

    @staticmethod
    def regex(value):
        """
        Search for this regular expression inside the text property targeted.
        :param value: the value to look for.
        """
        return P('regex', value)

    @staticmethod
    def fuzzy(value, distance):
        """
        Search for a fuzzy string inside the text property targeted.
        :param value: the value to look for.
        :param distance: The distance for the fuzzy search. ie. 1, to allow a one-letter misspellings.
        """
        return TextDistanceP.fuzzy(value, distance)

    @staticmethod
    def token_fuzzy(value, distance):
        """
        Search for a token fuzzy inside the text property targeted.
        :param value: the value to look for.
        :param distance: The distance for the token fuzzy search. ie. 1, to allow a one-letter misspellings.
        """
        return TextDistanceP.token_fuzzy(value, distance)

    @staticmethod
    def phrase(value, proximity):
        """
        Search for a phrase inside the text property targeted.
        :param value: the value to look for.
        :param proximity: The proximity for the phrase search. ie. phrase('David Felcey', 2).. to find 'David Felcey' with up to two middle names.
        """
        return TextDistanceP.phrase(value, proximity)


class CqlCollection(object):

    @staticmethod
    def contains(value):
        """
        Search for a value inside a cql list/set column.
        :param value: the value to look for.
        """
        return P('contains', value)

    @staticmethod
    def contains_value(value):
        """
        Search for a map value.
        :param value: the value to look for.
        """
        return P('containsValue', value)

    @staticmethod
    def contains_key(value):
        """
        Search for a map key.
        :param value: the value to look for.
        """
        return P('containsKey', value)

    @staticmethod
    def entry_eq(value):
        """
        Search for a map entry.
        :param value: the value to look for.
        """
        return P('entryEq', value)


class GeoUnit(object):
    _EARTH_MEAN_RADIUS_KM = 6371.0087714
    _DEGREES_TO_RADIANS = math.pi / 180
    _DEG_TO_KM = _DEGREES_TO_RADIANS * _EARTH_MEAN_RADIUS_KM
    _KM_TO_DEG = 1 / _DEG_TO_KM
    _MILES_TO_KM = 1.609344001

    MILES = _MILES_TO_KM * _KM_TO_DEG
    KILOMETERS = _KM_TO_DEG
    METERS = _KM_TO_DEG / 1000.0
    DEGREES = 1


class Geo(object):

    @staticmethod
    def inside(value, units=GeoUnit.DEGREES):
        """
        Search any instance of geometry inside the Distance targeted.
        :param value: A Distance to look for.
        :param units: The units for ``value``. See GeoUnit enum. (Can also
            provide an integer to use as a multiplier to convert ``value`` to
            degrees.)
        """
        return GeoP.inside(
            value=Distance(x=value.x, y=value.y, radius=value.radius * units)
        )
