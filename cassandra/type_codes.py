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

"""
Module with constants for Cassandra type codes.

These constants are useful for

    a) mapping messages to cqltypes                (cassandra/cqltypes.py)
    b) optimized dispatching for (de)serialization (cassandra/encoding.py)

Type codes are repeated here from the Cassandra binary protocol specification:

            0x0000    Custom: the value is a [string], see above.
            0x0001    Ascii
            0x0002    Bigint
            0x0003    Blob
            0x0004    Boolean
            0x0005    Counter
            0x0006    Decimal
            0x0007    Double
            0x0008    Float
            0x0009    Int
            0x000A    Text
            0x000B    Timestamp
            0x000C    Uuid
            0x000D    Varchar
            0x000E    Varint
            0x000F    Timeuuid
            0x0010    Inet
            0x0011    SimpleDateType
            0x0012    TimeType
            0x0013    ShortType
            0x0014    ByteType
            0x0015    DurationType
            0x0020    List: the value is an [option], representing the type
                            of the elements of the list.
            0x0021    Map: the value is two [option], representing the types of the
                           keys and values of the map
            0x0022    Set: the value is an [option], representing the type
                            of the elements of the set
"""

CUSTOM_TYPE = 0x0000
AsciiType = 0x0001
LongType = 0x0002
BytesType = 0x0003
BooleanType = 0x0004
CounterColumnType = 0x0005
DecimalType = 0x0006
DoubleType = 0x0007
FloatType = 0x0008
Int32Type = 0x0009
UTF8Type = 0x000A
DateType = 0x000B
UUIDType = 0x000C
VarcharType = 0x000D
IntegerType = 0x000E
TimeUUIDType = 0x000F
InetAddressType = 0x0010
SimpleDateType = 0x0011
TimeType = 0x0012
ShortType = 0x0013
ByteType = 0x0014
DurationType = 0x0015
ListType = 0x0020
MapType = 0x0021
SetType = 0x0022
UserType = 0x0030
TupleType = 0x0031
