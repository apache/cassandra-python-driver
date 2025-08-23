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

from _weakref import ref
import calendar
from collections import OrderedDict
from collections.abc import Mapping
import datetime
from functools import total_ordering
from itertools import chain
import keyword
import logging
import pickle
import random
import re
import socket
import sys
import time
import uuid

_HAS_GEOMET = True
try:
    from geomet import wkt
except:
    _HAS_GEOMET = False


from cassandra import DriverException

DATETIME_EPOC = datetime.datetime(1970, 1, 1).replace(tzinfo=None)
UTC_DATETIME_EPOC = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc).replace(tzinfo=None)

_nan = float('nan')

log = logging.getLogger(__name__)

assert sys.byteorder in ('little', 'big')
is_little_endian = sys.byteorder == 'little'


def datetime_from_timestamp(timestamp):
    """
    Creates a timezone-agnostic datetime from timestamp (in seconds) in a consistent manner.
    Works around a Windows issue with large negative timestamps (PYTHON-119),
    and rounding differences in Python 3.4 (PYTHON-340).

    :param timestamp: a unix timestamp, in seconds
    """
    dt = DATETIME_EPOC + datetime.timedelta(seconds=timestamp)
    return dt


def utc_datetime_from_ms_timestamp(timestamp):
    """
    Creates a UTC datetime from a timestamp in milliseconds. See
    :meth:`datetime_from_timestamp`.

    Raises an `OverflowError` if the timestamp is out of range for
    :class:`~datetime.datetime`.

    :param timestamp: timestamp, in milliseconds
    """
    return UTC_DATETIME_EPOC + datetime.timedelta(milliseconds=timestamp)


def ms_timestamp_from_datetime(dt):
    """
    Converts a datetime to a timestamp expressed in milliseconds.

    :param dt: a :class:`datetime.datetime`
    """
    return int(round((dt - UTC_DATETIME_EPOC).total_seconds() * 1000))


def unix_time_from_uuid1(uuid_arg):
    """
    Converts a version 1 :class:`uuid.UUID` to a timestamp with the same precision
    as :meth:`time.time()` returns.  This is useful for examining the
    results of queries returning a v1 :class:`~uuid.UUID`.

    :param uuid_arg: a version 1 :class:`~uuid.UUID`
    """
    return (uuid_arg.time - 0x01B21DD213814000) / 1e7


def datetime_from_uuid1(uuid_arg):
    """
    Creates a timezone-agnostic datetime from the timestamp in the
    specified type-1 UUID.

    :param uuid_arg: a version 1 :class:`~uuid.UUID`
    """
    return datetime_from_timestamp(unix_time_from_uuid1(uuid_arg))


def min_uuid_from_time(timestamp):
    """
    Generates the minimum TimeUUID (type 1) for a given timestamp, as compared by Cassandra.

    See :func:`uuid_from_time` for argument and return types.
    """
    return uuid_from_time(timestamp, 0x808080808080, 0x80)  # Cassandra does byte-wise comparison; fill with min signed bytes (0x80 = -128)


def max_uuid_from_time(timestamp):
    """
    Generates the maximum TimeUUID (type 1) for a given timestamp, as compared by Cassandra.

    See :func:`uuid_from_time` for argument and return types.
    """
    return uuid_from_time(timestamp, 0x7f7f7f7f7f7f, 0x3f7f)  # Max signed bytes (0x7f = 127)


def uuid_from_time(time_arg, node=None, clock_seq=None):
    """
    Converts a datetime or timestamp to a type 1 :class:`uuid.UUID`.

    :param time_arg:
      The time to use for the timestamp portion of the UUID.
      This can either be a :class:`datetime` object or a timestamp
      in seconds (as returned from :meth:`time.time()`).
    :type datetime: :class:`datetime` or timestamp

    :param node:
      None integer for the UUID (up to 48 bits). If not specified, this
      field is randomized.
    :type node: long

    :param clock_seq:
      Clock sequence field for the UUID (up to 14 bits). If not specified,
      a random sequence is generated.
    :type clock_seq: int

    :rtype: :class:`uuid.UUID`

    """
    if hasattr(time_arg, 'utctimetuple'):
        seconds = int(calendar.timegm(time_arg.utctimetuple()))
        microseconds = (seconds * 1e6) + time_arg.time().microsecond
    else:
        microseconds = int(time_arg * 1e6)

    # 0x01b21dd213814000 is the number of 100-ns intervals between the
    # UUID epoch 1582-10-15 00:00:00 and the Unix epoch 1970-01-01 00:00:00.
    intervals = int(microseconds * 10) + 0x01b21dd213814000

    time_low = intervals & 0xffffffff
    time_mid = (intervals >> 32) & 0xffff
    time_hi_version = (intervals >> 48) & 0x0fff

    if clock_seq is None:
        clock_seq = random.getrandbits(14)
    else:
        if clock_seq > 0x3fff:
            raise ValueError('clock_seq is out of range (need a 14-bit value)')

    clock_seq_low = clock_seq & 0xff
    clock_seq_hi_variant = 0x80 | ((clock_seq >> 8) & 0x3f)

    if node is None:
        node = random.getrandbits(48)

    return uuid.UUID(fields=(time_low, time_mid, time_hi_version,
                             clock_seq_hi_variant, clock_seq_low, node), version=1)

LOWEST_TIME_UUID = uuid.UUID('00000000-0000-1000-8080-808080808080')
""" The lowest possible TimeUUID, as sorted by Cassandra. """

HIGHEST_TIME_UUID = uuid.UUID('ffffffff-ffff-1fff-bf7f-7f7f7f7f7f7f')
""" The highest possible TimeUUID, as sorted by Cassandra. """


def _addrinfo_or_none(contact_point, port):
    """
    A helper function that wraps socket.getaddrinfo and returns None
    when it fails to, e.g. resolve one of the hostnames. Used to address
    PYTHON-895.
    """
    try:
        value = socket.getaddrinfo(contact_point, port,
                                  socket.AF_UNSPEC, socket.SOCK_STREAM)
        return value
    except socket.gaierror:
        log.debug('Could not resolve hostname "{}" '
                  'with port {}'.format(contact_point, port))
        return None


def _addrinfo_to_ip_strings(addrinfo):
    """
    Helper function that consumes the data output by socket.getaddrinfo and
    extracts the IP address from the sockaddr portion of the result.

    Since this is meant to be used in conjunction with _addrinfo_or_none,
    this will pass None and EndPoint instances through unaffected.
    """
    if addrinfo is None:
        return None
    return [(entry[4][0], entry[4][1]) for entry in addrinfo]


def _resolve_contact_points_to_string_map(contact_points):
    return OrderedDict(
        ('{cp}:{port}'.format(cp=cp, port=port), _addrinfo_to_ip_strings(_addrinfo_or_none(cp, port)))
        for cp, port in contact_points
    )


class _IterationGuard(object):
    # This context manager registers itself in the current iterators of the
    # weak container, such as to delay all removals until the context manager
    # exits.
    # This technique should be relatively thread-safe (since sets are).

    def __init__(self, weakcontainer):
        # Don't create cycles
        self.weakcontainer = ref(weakcontainer)

    def __enter__(self):
        w = self.weakcontainer()
        if w is not None:
            w._iterating.add(self)
        return self

    def __exit__(self, e, t, b):
        w = self.weakcontainer()
        if w is not None:
            s = w._iterating
            s.remove(self)
            if not s:
                w._commit_removals()


class WeakSet(object):
    def __init__(self, data=None):
        self.data = set()

        def _remove(item, selfref=ref(self)):
            self = selfref()
            if self is not None:
                if self._iterating:
                    self._pending_removals.append(item)
                else:
                    self.data.discard(item)

        self._remove = _remove
        # A list of keys to be removed
        self._pending_removals = []
        self._iterating = set()
        if data is not None:
            self.update(data)

    def _commit_removals(self):
        l = self._pending_removals
        discard = self.data.discard
        while l:
            discard(l.pop())

    def __iter__(self):
        with _IterationGuard(self):
            for itemref in self.data:
                item = itemref()
                if item is not None:
                    yield item

    def __len__(self):
        return sum(x() is not None for x in self.data)

    def __contains__(self, item):
        return ref(item) in self.data

    def __reduce__(self):
        return (self.__class__, (list(self),),
                getattr(self, '__dict__', None))

    __hash__ = None

    def add(self, item):
        if self._pending_removals:
            self._commit_removals()
        self.data.add(ref(item, self._remove))

    def clear(self):
        if self._pending_removals:
            self._commit_removals()
        self.data.clear()

    def copy(self):
        return self.__class__(self)

    def pop(self):
        if self._pending_removals:
            self._commit_removals()
        while True:
            try:
                itemref = self.data.pop()
            except KeyError:
                raise KeyError('pop from empty WeakSet')
            item = itemref()
            if item is not None:
                return item

    def remove(self, item):
        if self._pending_removals:
            self._commit_removals()
        self.data.remove(ref(item))

    def discard(self, item):
        if self._pending_removals:
            self._commit_removals()
        self.data.discard(ref(item))

    def update(self, other):
        if self._pending_removals:
            self._commit_removals()
        if isinstance(other, self.__class__):
            self.data.update(other.data)
        else:
            for element in other:
                self.add(element)

    def __ior__(self, other):
        self.update(other)
        return self

    # Helper functions for simple delegating methods.
    def _apply(self, other, method):
        if not isinstance(other, self.__class__):
            other = self.__class__(other)
        newdata = method(other.data)
        newset = self.__class__()
        newset.data = newdata
        return newset

    def difference(self, other):
        return self._apply(other, self.data.difference)
    __sub__ = difference

    def difference_update(self, other):
        if self._pending_removals:
            self._commit_removals()
        if self is other:
            self.data.clear()
        else:
            self.data.difference_update(ref(item) for item in other)

    def __isub__(self, other):
        if self._pending_removals:
            self._commit_removals()
        if self is other:
            self.data.clear()
        else:
            self.data.difference_update(ref(item) for item in other)
        return self

    def intersection(self, other):
        return self._apply(other, self.data.intersection)
    __and__ = intersection

    def intersection_update(self, other):
        if self._pending_removals:
            self._commit_removals()
        self.data.intersection_update(ref(item) for item in other)

    def __iand__(self, other):
        if self._pending_removals:
            self._commit_removals()
        self.data.intersection_update(ref(item) for item in other)
        return self

    def issubset(self, other):
        return self.data.issubset(ref(item) for item in other)
    __lt__ = issubset

    def __le__(self, other):
        return self.data <= set(ref(item) for item in other)

    def issuperset(self, other):
        return self.data.issuperset(ref(item) for item in other)
    __gt__ = issuperset

    def __ge__(self, other):
        return self.data >= set(ref(item) for item in other)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.data == set(ref(item) for item in other)

    def symmetric_difference(self, other):
        return self._apply(other, self.data.symmetric_difference)
    __xor__ = symmetric_difference

    def symmetric_difference_update(self, other):
        if self._pending_removals:
            self._commit_removals()
        if self is other:
            self.data.clear()
        else:
            self.data.symmetric_difference_update(ref(item) for item in other)

    def __ixor__(self, other):
        if self._pending_removals:
            self._commit_removals()
        if self is other:
            self.data.clear()
        else:
            self.data.symmetric_difference_update(ref(item) for item in other)
        return self

    def union(self, other):
        return self._apply(other, self.data.union)
    __or__ = union

    def isdisjoint(self, other):
        return len(self.intersection(other)) == 0


class SortedSet(object):
    '''
    A sorted set based on sorted list

    A sorted set implementation is used in this case because it does not
    require its elements to be immutable/hashable.

    #Not implemented: update functions, inplace operators
    '''

    def __init__(self, iterable=()):
        self._items = []
        self.update(iterable)

    def __len__(self):
        return len(self._items)

    def __getitem__(self, i):
        return self._items[i]

    def __iter__(self):
        return iter(self._items)

    def __reversed__(self):
        return reversed(self._items)

    def __repr__(self):
        return '%s(%r)' % (
            self.__class__.__name__,
            self._items)

    def __reduce__(self):
        return self.__class__, (self._items,)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._items == other._items
        else:
            try:
                return len(other) == len(self._items) and all(item in self for item in other)
            except TypeError:
                return NotImplemented

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return self._items != other._items
        else:
            try:
                return len(other) != len(self._items) or any(item not in self for item in other)
            except TypeError:
                return NotImplemented

    def __le__(self, other):
        return self.issubset(other)

    def __lt__(self, other):
        return len(other) > len(self._items) and self.issubset(other)

    def __ge__(self, other):
        return self.issuperset(other)

    def __gt__(self, other):
        return len(self._items) > len(other) and self.issuperset(other)

    def __and__(self, other):
        return self._intersect(other)
    __rand__ = __and__

    def __iand__(self, other):
        isect = self._intersect(other)
        self._items = isect._items
        return self

    def __or__(self, other):
        return self.union(other)
    __ror__ = __or__

    def __ior__(self, other):
        union = self.union(other)
        self._items = union._items
        return self

    def __sub__(self, other):
        return self._diff(other)

    def __rsub__(self, other):
        return sortedset(other) - self

    def __isub__(self, other):
        diff = self._diff(other)
        self._items = diff._items
        return self

    def __xor__(self, other):
        return self.symmetric_difference(other)
    __rxor__ = __xor__

    def __ixor__(self, other):
        sym_diff = self.symmetric_difference(other)
        self._items = sym_diff._items
        return self

    def __contains__(self, item):
        i = self._find_insertion(item)
        return i < len(self._items) and self._items[i] == item

    def __delitem__(self, i):
        del self._items[i]

    def __delslice__(self, i, j):
        del self._items[i:j]

    def add(self, item):
        i = self._find_insertion(item)
        if i < len(self._items):
            if self._items[i] != item:
                self._items.insert(i, item)
        else:
            self._items.append(item)

    def update(self, iterable):
        for i in iterable:
            self.add(i)

    def clear(self):
        del self._items[:]

    def copy(self):
        new = sortedset()
        new._items = list(self._items)
        return new

    def isdisjoint(self, other):
        return len(self._intersect(other)) == 0

    def issubset(self, other):
        return len(self._intersect(other)) == len(self._items)

    def issuperset(self, other):
        return len(self._intersect(other)) == len(other)

    def pop(self):
        if not self._items:
            raise KeyError("pop from empty set")
        return self._items.pop()

    def remove(self, item):
        i = self._find_insertion(item)
        if i < len(self._items):
            if self._items[i] == item:
                self._items.pop(i)
                return
        raise KeyError('%r' % item)

    def union(self, *others):
        union = sortedset()
        union._items = list(self._items)
        for other in others:
            for item in other:
                union.add(item)
        return union

    def intersection(self, *others):
        isect = self.copy()
        for other in others:
            isect = isect._intersect(other)
            if not isect:
                break
        return isect

    def difference(self, *others):
        diff = self.copy()
        for other in others:
            diff = diff._diff(other)
            if not diff:
                break
        return diff

    def symmetric_difference(self, other):
        diff_self_other = self._diff(other)
        diff_other_self = other.difference(self)
        return diff_self_other.union(diff_other_self)

    def _diff(self, other):
        diff = sortedset()
        for item in self._items:
            if item not in other:
                diff.add(item)
        return diff

    def _intersect(self, other):
        isect = sortedset()
        for item in self._items:
            if item in other:
                isect.add(item)
        return isect

    def _find_insertion(self, x):
        # this uses bisect_left algorithm unless it has elements it can't compare,
        # in which case it defaults to grouping non-comparable items at the beginning or end,
        # and scanning sequentially to find an insertion point
        a = self._items
        lo = 0
        hi = len(a)
        try:
            while lo < hi:
                mid = (lo + hi) // 2
                if a[mid] < x: lo = mid + 1
                else: hi = mid
        except TypeError:
            # could not compare a[mid] with x
            # start scanning to find insertion point while swallowing type errors
            lo = 0
            compared_one = False  # flag is used to determine whether un-comparables are grouped at the front or back
            while lo < hi:
                try:
                    if a[lo] == x or a[lo] >= x: break
                    compared_one = True
                except TypeError:
                    if compared_one: break
                lo += 1
        return lo

sortedset = SortedSet  # backwards-compatibility


class OrderedMap(Mapping):
    '''
    An ordered map that accepts non-hashable types for keys. It also maintains the
    insertion order of items, behaving as OrderedDict in that regard. These maps
    are constructed and read just as normal mapping types, except that they may
    contain arbitrary collections and other non-hashable items as keys::

        >>> od = OrderedMap([({'one': 1, 'two': 2}, 'value'),
        ...                  ({'three': 3, 'four': 4}, 'value2')])
        >>> list(od.keys())
        [{'two': 2, 'one': 1}, {'three': 3, 'four': 4}]
        >>> list(od.values())
        ['value', 'value2']

    These constructs are needed to support nested collections in Cassandra 2.1.3+,
    where frozen collections can be specified as parameters to others::

        CREATE TABLE example (
            ...
            value map<frozen<map<int, int>>, double>
            ...
        )

    This class derives from the (immutable) Mapping API. Objects in these maps
    are not intended be modified.
    '''

    def __init__(self, *args, **kwargs):
        if len(args) > 1:
            raise TypeError('expected at most 1 arguments, got %d' % len(args))

        self._items = []
        self._index = {}
        if args:
            e = args[0]
            if callable(getattr(e, 'keys', None)):
                for k in e.keys():
                    self._insert(k, e[k])
            else:
                for k, v in e:
                    self._insert(k, v)

        for k, v in kwargs.items():
            self._insert(k, v)

    def _insert(self, key, value):
        flat_key = self._serialize_key(key)
        i = self._index.get(flat_key, -1)
        if i >= 0:
            self._items[i] = (key, value)
        else:
            self._items.append((key, value))
            self._index[flat_key] = len(self._items) - 1

    __setitem__ = _insert

    def __getitem__(self, key):
        try:
            index = self._index[self._serialize_key(key)]
            return self._items[index][1]
        except KeyError:
            raise KeyError(str(key))

    def __delitem__(self, key):
        # not efficient -- for convenience only
        try:
            index = self._index.pop(self._serialize_key(key))
            self._index = dict((k, i if i < index else i - 1) for k, i in self._index.items())
            self._items.pop(index)
        except KeyError:
            raise KeyError(str(key))

    def __iter__(self):
        for i in self._items:
            yield i[0]

    def __len__(self):
        return len(self._items)

    def __eq__(self, other):
        if isinstance(other, OrderedMap):
            return self._items == other._items
        try:
            d = dict(other)
            return len(d) == len(self._items) and all(i[1] == d[i[0]] for i in self._items)
        except KeyError:
            return False
        except TypeError:
            pass
        return NotImplemented

    def __repr__(self):
        return '%s([%s])' % (
            self.__class__.__name__,
            ', '.join("(%r, %r)" % (k, v) for k, v in self._items))

    def __str__(self):
        return '{%s}' % ', '.join("%r: %r" % (k, v) for k, v in self._items)

    def popitem(self):
        try:
            kv = self._items.pop()
            del self._index[self._serialize_key(kv[0])]
            return kv
        except IndexError:
            raise KeyError()

    def _serialize_key(self, key):
        return pickle.dumps(key)


class OrderedMapSerializedKey(OrderedMap):

    def __init__(self, cass_type, protocol_version):
        super(OrderedMapSerializedKey, self).__init__()
        self.cass_key_type = cass_type
        self.protocol_version = protocol_version

    def _insert_unchecked(self, key, flat_key, value):
        self._items.append((key, value))
        self._index[flat_key] = len(self._items) - 1

    def _serialize_key(self, key):
        return self.cass_key_type.serialize(key, self.protocol_version)


@total_ordering
class Time(object):
    '''
    Idealized time, independent of day.

    Up to nanosecond resolution
    '''

    MICRO = 1000
    MILLI = 1000 * MICRO
    SECOND = 1000 * MILLI
    MINUTE = 60 * SECOND
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR

    nanosecond_time = 0

    def __init__(self, value):
        """
        Initializer value can be:

        - integer_type: absolute nanoseconds in the day
        - datetime.time: built-in time
        - string_type: a string time of the form "HH:MM:SS[.mmmuuunnn]"
        """
        if isinstance(value, int):
            self._from_timestamp(value)
        elif isinstance(value, datetime.time):
            self._from_time(value)
        elif isinstance(value, str):
            self._from_timestring(value)
        else:
            raise TypeError('Time arguments must be a whole number, datetime.time, or string')

    @property
    def hour(self):
        """
        The hour component of this time (0-23)
        """
        return self.nanosecond_time // Time.HOUR

    @property
    def minute(self):
        """
        The minute component of this time (0-59)
        """
        minutes = self.nanosecond_time // Time.MINUTE
        return minutes % 60

    @property
    def second(self):
        """
        The second component of this time (0-59)
        """
        seconds = self.nanosecond_time // Time.SECOND
        return seconds % 60

    @property
    def nanosecond(self):
        """
        The fractional seconds component of the time, in nanoseconds
        """
        return self.nanosecond_time % Time.SECOND

    def time(self):
        """
        Return a built-in datetime.time (nanosecond precision truncated to micros).
        """
        return datetime.time(hour=self.hour, minute=self.minute, second=self.second,
                             microsecond=self.nanosecond // Time.MICRO)

    def _from_timestamp(self, t):
        if t >= Time.DAY:
            raise ValueError("value must be less than number of nanoseconds in a day (%d)" % Time.DAY)
        self.nanosecond_time = t

    def _from_timestring(self, s):
        try:
            parts = s.split('.')
            base_time = time.strptime(parts[0], "%H:%M:%S")
            self.nanosecond_time = (base_time.tm_hour * Time.HOUR +
                                    base_time.tm_min * Time.MINUTE +
                                    base_time.tm_sec * Time.SECOND)

            if len(parts) > 1:
                # right pad to 9 digits
                nano_time_str = parts[1] + "0" * (9 - len(parts[1]))
                self.nanosecond_time += int(nano_time_str)

        except ValueError:
            raise ValueError("can't interpret %r as a time" % (s,))

    def _from_time(self, t):
        self.nanosecond_time = (t.hour * Time.HOUR +
                                t.minute * Time.MINUTE +
                                t.second * Time.SECOND +
                                t.microsecond * Time.MICRO)

    def __hash__(self):
        return self.nanosecond_time

    def __eq__(self, other):
        if isinstance(other, Time):
            return self.nanosecond_time == other.nanosecond_time

        if isinstance(other, int):
            return self.nanosecond_time == other

        return self.nanosecond_time % Time.MICRO == 0 and \
            datetime.time(hour=self.hour, minute=self.minute, second=self.second,
                          microsecond=self.nanosecond // Time.MICRO) == other

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        if not isinstance(other, Time):
            return NotImplemented
        return self.nanosecond_time < other.nanosecond_time

    def __repr__(self):
        return "Time(%s)" % self.nanosecond_time

    def __str__(self):
        return "%02d:%02d:%02d.%09d" % (self.hour, self.minute,
                                        self.second, self.nanosecond)


@total_ordering
class Date(object):
    '''
    Idealized date: year, month, day

    Offers wider year range than datetime.date. For Dates that cannot be represented
    as a datetime.date (because datetime.MINYEAR, datetime.MAXYEAR), this type falls back
    to printing days_from_epoch offset.
    '''

    MINUTE = 60
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR

    date_format = "%Y-%m-%d"

    days_from_epoch = 0

    def __init__(self, value):
        """
        Initializer value can be:

        - integer_type: absolute days from epoch (1970, 1, 1). Can be negative.
        - datetime.date: built-in date
        - string_type: a string time of the form "yyyy-mm-dd"
        """
        if isinstance(value, int):
            self.days_from_epoch = value
        elif isinstance(value, (datetime.date, datetime.datetime)):
            self._from_timetuple(value.timetuple())
        elif isinstance(value, str):
            self._from_datestring(value)
        else:
            raise TypeError('Date arguments must be a whole number, datetime.date, or string')

    @property
    def seconds(self):
        """
        Absolute seconds from epoch (can be negative)
        """
        return self.days_from_epoch * Date.DAY

    def date(self):
        """
        Return a built-in datetime.date for Dates falling in the years [datetime.MINYEAR, datetime.MAXYEAR]

        ValueError is raised for Dates outside this range.
        """
        try:
            dt = datetime_from_timestamp(self.seconds)
            return datetime.date(dt.year, dt.month, dt.day)
        except Exception:
            raise ValueError("%r exceeds ranges for built-in datetime.date" % self)

    def _from_timetuple(self, t):
        self.days_from_epoch = calendar.timegm(t) // Date.DAY

    def _from_datestring(self, s):
        if s[0] == '+':
            s = s[1:]
        dt = datetime.datetime.strptime(s, self.date_format)
        self._from_timetuple(dt.timetuple())

    def __hash__(self):
        return self.days_from_epoch

    def __eq__(self, other):
        if isinstance(other, Date):
            return self.days_from_epoch == other.days_from_epoch

        if isinstance(other, int):
            return self.days_from_epoch == other

        try:
            return self.date() == other
        except Exception:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        if not isinstance(other, Date):
            return NotImplemented
        return self.days_from_epoch < other.days_from_epoch

    def __repr__(self):
        return "Date(%s)" % self.days_from_epoch

    def __str__(self):
        try:
            dt = datetime_from_timestamp(self.seconds)
            return "%04d-%02d-%02d" % (dt.year, dt.month, dt.day)
        except:
            # If we overflow datetime.[MIN|MAX]
            return str(self.days_from_epoch)


inet_pton = socket.inet_pton
inet_ntop = socket.inet_ntop


# similar to collections.namedtuple, reproduced here because Python 2.6 did not have the rename logic
def _positional_rename_invalid_identifiers(field_names):
    names_out = list(field_names)
    for index, name in enumerate(field_names):
        if (not all(c.isalnum() or c == '_' for c in name)
            or keyword.iskeyword(name)
            or not name
            or name[0].isdigit()
            or name.startswith('_')):
            names_out[index] = 'field_%d_' % index
    return names_out


def _sanitize_identifiers(field_names):
    names_out = _positional_rename_invalid_identifiers(field_names)
    if len(names_out) != len(set(names_out)):
        observed_names = set()
        for index, name in enumerate(names_out):
            while names_out[index] in observed_names:
                names_out[index] = "%s_" % (names_out[index],)
            observed_names.add(names_out[index])
    return names_out


def list_contents_to_tuple(to_convert):
    if isinstance(to_convert, list):
        for n, i in enumerate(to_convert):
            if isinstance(to_convert[n], list):
                to_convert[n] = tuple(to_convert[n])
        return tuple(to_convert)
    else:
        return to_convert


class Point(object):
    """
    Represents a point geometry for DSE
    """

    x = None
    """
    x coordinate of the point
    """

    y = None
    """
    y coordinate of the point
    """

    def __init__(self, x=_nan, y=_nan):
        self.x = x
        self.y = y

    def __eq__(self, other):
        return isinstance(other, Point) and self.x == other.x and self.y == other.y

    def __hash__(self):
        return hash((self.x, self.y))

    def __str__(self):
        """
        Well-known text representation of the point
        """
        return "POINT (%r %r)" % (self.x, self.y)

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.x, self.y)

    @staticmethod
    def from_wkt(s):
        """
        Parse a Point geometry from a wkt string and return a new Point object.
        """
        if not _HAS_GEOMET:
            raise DriverException("Geomet is required to deserialize a wkt geometry.")

        try:
            geom = wkt.loads(s)
        except ValueError:
            raise ValueError("Invalid WKT geometry: '{0}'".format(s))

        if geom['type'] != 'Point':
            raise ValueError("Invalid WKT geometry type. Expected 'Point', got '{0}': '{1}'".format(geom['type'], s))

        coords = geom['coordinates']
        if len(coords) < 2:
            x = y = _nan
        else:
            x = coords[0]
            y = coords[1]

        return Point(x=x, y=y)


class LineString(object):
    """
    Represents a linestring geometry for DSE
    """

    coords = None
    """
    Tuple of (x, y) coordinates in the linestring
    """
    def __init__(self, coords=tuple()):
        """
        'coords`: a sequence of (x, y) coordinates of points in the linestring
        """
        self.coords = tuple(coords)

    def __eq__(self, other):
        return isinstance(other, LineString) and self.coords == other.coords

    def __hash__(self):
        return hash(self.coords)

    def __str__(self):
        """
        Well-known text representation of the LineString
        """
        if not self.coords:
            return "LINESTRING EMPTY"
        return "LINESTRING (%s)" % ', '.join("%r %r" % (x, y) for x, y in self.coords)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.coords)

    @staticmethod
    def from_wkt(s):
        """
        Parse a LineString geometry from a wkt string and return a new LineString object.
        """
        if not _HAS_GEOMET:
            raise DriverException("Geomet is required to deserialize a wkt geometry.")

        try:
            geom = wkt.loads(s)
        except ValueError:
            raise ValueError("Invalid WKT geometry: '{0}'".format(s))

        if geom['type'] != 'LineString':
            raise ValueError("Invalid WKT geometry type. Expected 'LineString', got '{0}': '{1}'".format(geom['type'], s))

        geom['coordinates'] = list_contents_to_tuple(geom['coordinates'])

        return LineString(coords=geom['coordinates'])


class _LinearRing(object):
    # no validation, no implicit closing; just used for poly composition, to
    # mimic that of shapely.geometry.Polygon
    def __init__(self, coords=tuple()):
        self.coords = list_contents_to_tuple(coords)

    def __eq__(self, other):
        return isinstance(other, _LinearRing) and self.coords == other.coords

    def __hash__(self):
        return hash(self.coords)

    def __str__(self):
        if not self.coords:
            return "LINEARRING EMPTY"
        return "LINEARRING (%s)" % ', '.join("%r %r" % (x, y) for x, y in self.coords)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.coords)


class Polygon(object):
    """
    Represents a polygon geometry for DSE
    """

    exterior = None
    """
    _LinearRing representing the exterior of the polygon
    """

    interiors = None
    """
    Tuple of _LinearRings representing interior holes in the polygon
    """

    def __init__(self, exterior=tuple(), interiors=None):
        """
        'exterior`: a sequence of (x, y) coordinates of points in the linestring
        `interiors`: None, or a sequence of sequences or (x, y) coordinates of points describing interior linear rings
        """
        self.exterior = _LinearRing(exterior)
        self.interiors = tuple(_LinearRing(e) for e in interiors) if interiors else tuple()

    def __eq__(self, other):
        return isinstance(other, Polygon) and self.exterior == other.exterior and self.interiors == other.interiors

    def __hash__(self):
        return hash((self.exterior, self.interiors))

    def __str__(self):
        """
        Well-known text representation of the polygon
        """
        if not self.exterior.coords:
            return "POLYGON EMPTY"
        rings = [ring.coords for ring in chain((self.exterior,), self.interiors)]
        rings = ["(%s)" % ', '.join("%r %r" % (x, y) for x, y in ring) for ring in rings]
        return "POLYGON (%s)" % ', '.join(rings)

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.exterior.coords, [ring.coords for ring in self.interiors])

    @staticmethod
    def from_wkt(s):
        """
        Parse a Polygon geometry from a wkt string and return a new Polygon object.
        """
        if not _HAS_GEOMET:
            raise DriverException("Geomet is required to deserialize a wkt geometry.")

        try:
            geom = wkt.loads(s)
        except ValueError:
            raise ValueError("Invalid WKT geometry: '{0}'".format(s))

        if geom['type'] != 'Polygon':
            raise ValueError("Invalid WKT geometry type. Expected 'Polygon', got '{0}': '{1}'".format(geom['type'], s))

        coords = geom['coordinates']
        exterior = coords[0] if len(coords) > 0 else tuple()
        interiors = coords[1:] if len(coords) > 1 else None

        return Polygon(exterior=exterior, interiors=interiors)


_distance_wkt_pattern = re.compile("distance *\\( *\\( *([\\d\\.-]+) *([\\d+\\.-]+) *\\) *([\\d+\\.-]+) *\\) *$", re.IGNORECASE)


class Distance(object):
    """
    Represents a Distance geometry for DSE
    """

    x = None
    """
    x coordinate of the center point
    """

    y = None
    """
    y coordinate of the center point
    """

    radius = None
    """
    radius to represent the distance from the center point
    """

    def __init__(self, x=_nan, y=_nan, radius=_nan):
        self.x = x
        self.y = y
        self.radius = radius

    def __eq__(self, other):
        return isinstance(other, Distance) and self.x == other.x and self.y == other.y and self.radius == other.radius

    def __hash__(self):
        return hash((self.x, self.y, self.radius))

    def __str__(self):
        """
        Well-known text representation of the point
        """
        return "DISTANCE ((%r %r) %r)" % (self.x, self.y, self.radius)

    def __repr__(self):
        return "%s(%r, %r, %r)" % (self.__class__.__name__, self.x, self.y, self.radius)

    @staticmethod
    def from_wkt(s):
        """
        Parse a Distance geometry from a wkt string and return a new Distance object.
        """

        distance_match = _distance_wkt_pattern.match(s)

        if distance_match is None:
            raise ValueError("Invalid WKT geometry: '{0}'".format(s))

        x, y, radius = distance_match.groups()
        return Distance(x, y, radius)


class Duration(object):
    """
    Cassandra Duration Type
    """

    months = 0
    ""
    days = 0
    ""
    nanoseconds = 0
    ""

    def __init__(self, months=0, days=0, nanoseconds=0):
        self.months = months
        self.days = days
        self.nanoseconds = nanoseconds

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.months == other.months and self.days == other.days and self.nanoseconds == other.nanoseconds

    def __repr__(self):
        return "Duration({0}, {1}, {2})".format(self.months, self.days, self.nanoseconds)

    def __str__(self):
        has_negative_values = self.months < 0 or self.days < 0 or self.nanoseconds < 0
        return '%s%dmo%dd%dns' % (
            '-' if has_negative_values else '',
            abs(self.months),
            abs(self.days),
            abs(self.nanoseconds)
        )


class DateRangePrecision(object):
    """
    An "enum" representing the valid values for :attr:`DateRange.precision`.
    """
    YEAR = 'YEAR'
    """
    """

    MONTH = 'MONTH'
    """
    """

    DAY = 'DAY'
    """
    """

    HOUR = 'HOUR'
    """
    """

    MINUTE = 'MINUTE'
    """
    """

    SECOND = 'SECOND'
    """
    """

    MILLISECOND = 'MILLISECOND'
    """
    """

    PRECISIONS = (YEAR, MONTH, DAY, HOUR,
                  MINUTE, SECOND, MILLISECOND)
    """
    """

    @classmethod
    def _to_int(cls, precision):
        return cls.PRECISIONS.index(precision.upper())

    @classmethod
    def _round_to_precision(cls, ms, precision, default_dt):
        try:
            dt = utc_datetime_from_ms_timestamp(ms)
        except OverflowError:
            return ms
        precision_idx = cls._to_int(precision)
        replace_kwargs = {}
        if precision_idx <= cls._to_int(DateRangePrecision.YEAR):
            replace_kwargs['month'] = default_dt.month
        if precision_idx <= cls._to_int(DateRangePrecision.MONTH):
            replace_kwargs['day'] = default_dt.day
        if precision_idx <= cls._to_int(DateRangePrecision.DAY):
            replace_kwargs['hour'] = default_dt.hour
        if precision_idx <= cls._to_int(DateRangePrecision.HOUR):
            replace_kwargs['minute'] = default_dt.minute
        if precision_idx <= cls._to_int(DateRangePrecision.MINUTE):
            replace_kwargs['second'] = default_dt.second
        if precision_idx <= cls._to_int(DateRangePrecision.SECOND):
            # truncate to nearest 1000, so we deal in ms, not us
            replace_kwargs['microsecond'] = (default_dt.microsecond // 1000) * 1000
        if precision_idx == cls._to_int(DateRangePrecision.MILLISECOND):
            replace_kwargs['microsecond'] = int(round(dt.microsecond, -3))
        return ms_timestamp_from_datetime(dt.replace(**replace_kwargs))

    @classmethod
    def round_up_to_precision(cls, ms, precision):
        # PYTHON-912: this is the only case in which we can't take as upper bound
        # datetime.datetime.max because the month from ms may be February, and we'd
        # be setting 31 as the month day
        if precision == cls.MONTH:
            date_ms = utc_datetime_from_ms_timestamp(ms)
            upper_date = datetime.datetime.max.replace(year=date_ms.year, month=date_ms.month,
                                                       day=calendar.monthrange(date_ms.year, date_ms.month)[1])
        else:
            upper_date = datetime.datetime.max
        return cls._round_to_precision(ms, precision, upper_date)

    @classmethod
    def round_down_to_precision(cls, ms, precision):
        return cls._round_to_precision(ms, precision, datetime.datetime.min)


@total_ordering
class DateRangeBound(object):
    """DateRangeBound(value, precision)
    Represents a single date value and its precision for :class:`DateRange`.

    .. attribute:: milliseconds

        Integer representing milliseconds since the UNIX epoch. May be negative.

    .. attribute:: precision

        String representing the precision of a bound. Must be a valid
        :class:`DateRangePrecision` member.

    :class:`DateRangeBound` uses a millisecond offset from the UNIX epoch to
    allow :class:`DateRange` to represent values `datetime.datetime` cannot.
    For such values, string representions will show this offset rather than the
    CQL representation.
    """
    milliseconds = None
    precision = None

    def __init__(self, value, precision):
        """
        :param value: a value representing ms since the epoch. Accepts an
            integer or a datetime.
        :param precision: a string representing precision
        """
        if precision is not None:
            try:
                self.precision = precision.upper()
            except AttributeError:
                raise TypeError('precision must be a string; got %r' % precision)

        if value is None:
            milliseconds = None
        elif isinstance(value, int):
            milliseconds = value
        elif isinstance(value, datetime.datetime):
            value = value.replace(
                microsecond=int(round(value.microsecond, -3))
            )
            milliseconds = ms_timestamp_from_datetime(value)
        else:
            raise ValueError('%r is not a valid value for DateRangeBound' % value)

        self.milliseconds = milliseconds
        self.validate()

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return (self.milliseconds == other.milliseconds and
                self.precision == other.precision)

    def __lt__(self, other):
        return ((str(self.milliseconds), str(self.precision)) <
                (str(other.milliseconds), str(other.precision)))

    def datetime(self):
        """
        Return :attr:`milliseconds` as a :class:`datetime.datetime` if possible.
        Raises an `OverflowError` if the value is out of range.
        """
        return utc_datetime_from_ms_timestamp(self.milliseconds)

    def validate(self):
        attrs = self.milliseconds, self.precision
        if attrs == (None, None):
            return
        if None in attrs:
            raise TypeError(
                ("%s.datetime and %s.precision must not be None unless both "
                 "are None; Got: %r") % (self.__class__.__name__,
                                         self.__class__.__name__,
                                         self)
            )
        if self.precision not in DateRangePrecision.PRECISIONS:
            raise ValueError(
                "%s.precision: expected value in %r; got %r" % (
                    self.__class__.__name__,
                    DateRangePrecision.PRECISIONS,
                    self.precision
                )
            )

    @classmethod
    def from_value(cls, value):
        """
        Construct a new :class:`DateRangeBound` from a given value. If
        possible, use the `value['milliseconds']` and `value['precision']` keys
        of the argument. Otherwise, use the argument as a `(milliseconds,
        precision)` iterable.

        :param value: a dictlike or iterable object
        """
        if isinstance(value, cls):
            return value

        # if possible, use as a mapping
        try:
            milliseconds, precision = value.get('milliseconds'), value.get('precision')
        except AttributeError:
            milliseconds = precision = None
        if milliseconds is not None and precision is not None:
            return DateRangeBound(value=milliseconds, precision=precision)

        # otherwise, use as an iterable
        return DateRangeBound(*value)

    def round_up(self):
        if self.milliseconds is None or self.precision is None:
            return self
        self.milliseconds = DateRangePrecision.round_up_to_precision(
            self.milliseconds, self.precision
        )
        return self

    def round_down(self):
        if self.milliseconds is None or self.precision is None:
            return self
        self.milliseconds = DateRangePrecision.round_down_to_precision(
            self.milliseconds, self.precision
        )
        return self

    _formatter_map = {
        DateRangePrecision.YEAR: '%Y',
        DateRangePrecision.MONTH: '%Y-%m',
        DateRangePrecision.DAY: '%Y-%m-%d',
        DateRangePrecision.HOUR: '%Y-%m-%dT%HZ',
        DateRangePrecision.MINUTE: '%Y-%m-%dT%H:%MZ',
        DateRangePrecision.SECOND: '%Y-%m-%dT%H:%M:%SZ',
        DateRangePrecision.MILLISECOND: '%Y-%m-%dT%H:%M:%S',
    }

    def __str__(self):
        if self == OPEN_BOUND:
            return '*'

        try:
            dt = self.datetime()
        except OverflowError:
            return '%sms' % (self.milliseconds,)

        formatted = dt.strftime(self._formatter_map[self.precision])

        if self.precision == DateRangePrecision.MILLISECOND:
            # we'd like to just format with '%Y-%m-%dT%H:%M:%S.%fZ', but %f
            # gives us more precision than we want, so we strftime up to %S and
            # do the rest ourselves
            return '%s.%03dZ' % (formatted, dt.microsecond / 1000)

        return formatted

    def __repr__(self):
        return '%s(milliseconds=%r, precision=%r)' % (
            self.__class__.__name__, self.milliseconds, self.precision
        )


OPEN_BOUND = DateRangeBound(value=None, precision=None)
"""
Represents `*`, an open value or bound for :class:`DateRange`.
"""


@total_ordering
class DateRange(object):
    """DateRange(lower_bound=None, upper_bound=None, value=None)
    DSE DateRange Type

    .. attribute:: lower_bound

        :class:`~DateRangeBound` representing the lower bound of a bounded range.

    .. attribute:: upper_bound

        :class:`~DateRangeBound` representing the upper bound of a bounded range.

    .. attribute:: value

        :class:`~DateRangeBound` representing the value of a single-value range.

    As noted in its documentation, :class:`DateRangeBound` uses a millisecond
    offset from the UNIX epoch to allow :class:`DateRange` to represent values
    `datetime.datetime` cannot. For such values, string representions will show
    this offset rather than the CQL representation.
    """
    lower_bound = None
    upper_bound = None
    value = None

    def __init__(self, lower_bound=None, upper_bound=None, value=None):
        """
        :param lower_bound: a :class:`DateRangeBound` or object accepted by
            :meth:`DateRangeBound.from_value` to be used as a
            :attr:`lower_bound`. Mutually exclusive with `value`. If
            `upper_bound` is specified and this is not, the :attr:`lower_bound`
            will be open.
        :param upper_bound: a :class:`DateRangeBound` or object accepted by
            :meth:`DateRangeBound.from_value` to be used as a
            :attr:`upper_bound`. Mutually exclusive with `value`. If
            `lower_bound` is specified and this is not, the :attr:`upper_bound`
            will be open.
        :param value: a :class:`DateRangeBound` or object accepted by
            :meth:`DateRangeBound.from_value` to be used as :attr:`value`. Mutually
            exclusive with `lower_bound` and `lower_bound`.
        """

        # if necessary, transform non-None args to DateRangeBounds
        lower_bound = (DateRangeBound.from_value(lower_bound).round_down()
                       if lower_bound else lower_bound)
        upper_bound = (DateRangeBound.from_value(upper_bound).round_up()
                       if upper_bound else upper_bound)
        value = (DateRangeBound.from_value(value).round_down()
                 if value else value)

        # if we're using a 2-ended range but one bound isn't specified, specify
        # an open bound
        if lower_bound is None and upper_bound is not None:
            lower_bound = OPEN_BOUND
        if upper_bound is None and lower_bound is not None:
            upper_bound = OPEN_BOUND

        self.lower_bound, self.upper_bound, self.value = (
            lower_bound, upper_bound, value
        )
        self.validate()

    def validate(self):
        if self.value is None:
            if self.lower_bound is None or self.upper_bound is None:
                raise ValueError(
                    '%s instances where value attribute is None must set '
                    'lower_bound or upper_bound; got %r' % (
                        self.__class__.__name__,
                        self
                    )
                )
        else:  # self.value is not None
            if self.lower_bound is not None or self.upper_bound is not None:
                raise ValueError(
                    '%s instances where value attribute is not None must not '
                    'set lower_bound or upper_bound; got %r' % (
                        self.__class__.__name__,
                        self
                    )
                )

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return (self.lower_bound == other.lower_bound and
                self.upper_bound == other.upper_bound and
                self.value == other.value)

    def __lt__(self, other):
        return ((str(self.lower_bound), str(self.upper_bound), str(self.value)) <
                (str(other.lower_bound), str(other.upper_bound), str(other.value)))

    def __str__(self):
        if self.value:
            return str(self.value)
        else:
            return '[%s TO %s]' % (self.lower_bound, self.upper_bound)

    def __repr__(self):
        return '%s(lower_bound=%r, upper_bound=%r, value=%r)' % (
            self.__class__.__name__,
            self.lower_bound, self.upper_bound, self.value
        )


@total_ordering
class Version(object):
    """
    Internal minimalist class to compare versions.
    A valid version is: <int>.<int>.<int>.<int or str>.

    TODO: when python2 support is removed, use packaging.version.
    """

    _version = None
    major = None
    minor = 0
    patch = 0
    build = 0
    prerelease = 0

    def __init__(self, version):
        self._version = version
        if '-' in version:
            version_without_prerelease, self.prerelease = version.split('-', 1)
        else:
            version_without_prerelease = version
        parts = list(reversed(version_without_prerelease.split('.')))
        if len(parts) > 4:
            prerelease_string = "-{}".format(self.prerelease) if self.prerelease else ""
            log.warning("Unrecognized version: {}. Only 4 components plus prerelease are supported. "
                        "Assuming version as {}{}".format(version, '.'.join(parts[:-5:-1]), prerelease_string))

        try:
            self.major = int(parts.pop())
        except ValueError as e:
            raise ValueError(
                "Couldn't parse version {}. Version should start with a number".format(version))\
                .with_traceback(e.__traceback__)
        try:
            self.minor = int(parts.pop()) if parts else 0
            self.patch = int(parts.pop()) if parts else 0

            if parts:  # we have a build version
                build = parts.pop()
                try:
                    self.build = int(build)
                except ValueError:
                    self.build = build
        except ValueError:
            assumed_version = "{}.{}.{}.{}-{}".format(self.major, self.minor, self.patch, self.build, self.prerelease)
            log.warning("Unrecognized version {}. Assuming version as {}".format(version, assumed_version))

    def __hash__(self):
        return self._version

    def __repr__(self):
        version_string = "Version({0}, {1}, {2}".format(self.major, self.minor, self.patch)
        if self.build:
            version_string += ", {}".format(self.build)
        if self.prerelease:
            version_string += ", {}".format(self.prerelease)
        version_string += ")"

        return version_string

    def __str__(self):
        return self._version

    @staticmethod
    def _compare_version_part(version, other_version, cmp):
        if not (isinstance(version, int) and
                isinstance(other_version, int)):
            version = str(version)
            other_version = str(other_version)

        return cmp(version, other_version)

    def __eq__(self, other):
        if not isinstance(other, Version):
            return NotImplemented

        return (self.major == other.major and
                self.minor == other.minor and
                self.patch == other.patch and
                self._compare_version_part(self.build, other.build, lambda s, o: s == o) and
                self._compare_version_part(self.prerelease, other.prerelease, lambda s, o: s == o)
                )

    def __gt__(self, other):
        if not isinstance(other, Version):
            return NotImplemented

        is_major_ge = self.major >= other.major
        is_minor_ge = self.minor >= other.minor
        is_patch_ge = self.patch >= other.patch
        is_build_gt = self._compare_version_part(self.build, other.build, lambda s, o: s > o)
        is_build_ge = self._compare_version_part(self.build, other.build, lambda s, o: s >= o)

        # By definition, a prerelease comes BEFORE the actual release, so if a version
        # doesn't have a prerelease, it's automatically greater than anything that does
        if self.prerelease and not other.prerelease:
            is_prerelease_gt = False
        elif other.prerelease and not self.prerelease:
            is_prerelease_gt = True
        else:
            is_prerelease_gt = self._compare_version_part(self.prerelease, other.prerelease, lambda s, o: s > o) \

        return (self.major > other.major or
                (is_major_ge and self.minor > other.minor) or
                (is_major_ge and is_minor_ge and self.patch > other.patch) or
                (is_major_ge and is_minor_ge and is_patch_ge and is_build_gt) or
                (is_major_ge and is_minor_ge and is_patch_ge and is_build_ge and is_prerelease_gt)
                )
