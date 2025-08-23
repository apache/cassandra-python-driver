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

from cassandra.cqlengine import UnicodeMixin


class QueryOperatorException(Exception):
    pass


class BaseQueryOperator(UnicodeMixin):
    # The symbol that identifies this operator in kwargs
    # ie: colname__<symbol>
    symbol = None

    # The comparator symbol this operator uses in cql
    cql_symbol = None

    def __unicode__(self):
        if self.cql_symbol is None:
            raise QueryOperatorException("cql symbol is None")
        return self.cql_symbol


class OpMapMeta(type):

    def __init__(cls, name, bases, dct):
        if not hasattr(cls, 'opmap'):
            cls.opmap = {}
        else:
            cls.opmap[cls.symbol] = cls
        super(OpMapMeta, cls).__init__(name, bases, dct)


class BaseWhereOperator(BaseQueryOperator, metaclass=OpMapMeta):
    """ base operator used for where clauses """
    @classmethod
    def get_operator(cls, symbol):
        try:
            return cls.opmap[symbol.upper()]
        except KeyError:
            raise QueryOperatorException("{0} doesn't map to a QueryOperator".format(symbol))


class EqualsOperator(BaseWhereOperator):
    symbol = 'EQ'
    cql_symbol = '='


class NotEqualsOperator(BaseWhereOperator):
    symbol = 'NE'
    cql_symbol = '!='


class InOperator(EqualsOperator):
    symbol = 'IN'
    cql_symbol = 'IN'


class GreaterThanOperator(BaseWhereOperator):
    symbol = "GT"
    cql_symbol = '>'


class GreaterThanOrEqualOperator(BaseWhereOperator):
    symbol = "GTE"
    cql_symbol = '>='


class LessThanOperator(BaseWhereOperator):
    symbol = "LT"
    cql_symbol = '<'


class LessThanOrEqualOperator(BaseWhereOperator):
    symbol = "LTE"
    cql_symbol = '<='


class ContainsOperator(EqualsOperator):
    symbol = "CONTAINS"
    cql_symbol = 'CONTAINS'


class LikeOperator(EqualsOperator):
    symbol = "LIKE"
    cql_symbol = 'LIKE'


class IsNotNullOperator(EqualsOperator):
    symbol = "IS NOT NULL"
    cql_symbol = 'IS NOT NULL'
