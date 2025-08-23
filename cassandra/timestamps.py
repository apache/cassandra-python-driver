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
This module contains utilities for generating timestamps for client-side
timestamp specification.
"""

import logging
import time
from threading import Lock

log = logging.getLogger(__name__)

class MonotonicTimestampGenerator(object):
    """
    An object that, when called, returns ``int(time.time() * 1e6)`` when
    possible, but, if the value returned by ``time.time`` doesn't increase,
    drifts into the future and logs warnings.
    Exposed configuration attributes can be configured with arguments to
    ``__init__`` or by changing attributes on an initialized object.

    .. versionadded:: 3.8.0
    """

    warn_on_drift = True
    """
    If true, log warnings when timestamps drift into the future as allowed by
    :attr:`warning_threshold` and :attr:`warning_interval`.
    """

    warning_threshold = 1
    """
    This object will only issue warnings when the returned timestamp drifts
    more than ``warning_threshold`` seconds into the future.
    Defaults to 1 second.
    """

    warning_interval = 1
    """
    This object will only issue warnings every ``warning_interval`` seconds.
    Defaults to 1 second.
    """

    def __init__(self, warn_on_drift=True, warning_threshold=1, warning_interval=1):
        self.lock = Lock()
        with self.lock:
            self.last = 0
            self._last_warn = 0
        self.warn_on_drift = warn_on_drift
        self.warning_threshold = warning_threshold
        self.warning_interval = warning_interval

    def _next_timestamp(self, now, last):
        """
        Returns the timestamp that should be used if ``now`` is the current
        time and ``last`` is the last timestamp returned by this object.
        Intended for internal and testing use only; to generate timestamps,
        call an instantiated ``MonotonicTimestampGenerator`` object.

        :param int now: an integer to be used as the current time, typically
            representing the current time in microseconds since the UNIX epoch
        :param int last: an integer representing the last timestamp returned by
            this object
        """
        if now > last:
            self.last = now
            return now
        else:
            self._maybe_warn(now=now)
            self.last = last + 1
            return self.last

    def __call__(self):
        """
        Makes ``MonotonicTimestampGenerator`` objects callable; defers
        internally to _next_timestamp.
        """
        with self.lock:
            return self._next_timestamp(now=int(time.time() * 1e6),
                                        last=self.last)

    def _maybe_warn(self, now):
        # should be called from inside the self.lock.
        diff = self.last - now
        since_last_warn = now - self._last_warn

        warn = (self.warn_on_drift and
                (diff >= self.warning_threshold * 1e6) and
                (since_last_warn >= self.warning_interval * 1e6))
        if warn:
            log.warning(
                "Clock skew detected: current tick ({now}) was {diff} "
                "microseconds behind the last generated timestamp "
                "({last}), returned timestamps will be artificially "
                "incremented to guarantee monotonicity.".format(
                    now=now, diff=diff, last=self.last))
            self._last_warn = now
