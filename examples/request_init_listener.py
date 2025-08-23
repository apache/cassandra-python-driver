#!/usr/bin/env python
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

# This script shows an example "request init listener" which can be registered to track certain request metrics
# for a session. In this case we're just accumulating total request and error counts, as well as some statistics
# about the encoded request size. Note that the counts would be available using the internal 'metrics' tracking --
# this is just demonstrating a way to track a few custom attributes.

from cassandra.cluster import Cluster
from greplin import scales

import pprint
pp = pprint.PrettyPrinter(indent=2)


class RequestAnalyzer(object):
    """
    Class used to track request and error counts for a Session.

    Also computes statistics on encoded request size.
    """

    requests = scales.PmfStat('request size')
    errors = scales.IntStat('errors')

    def __init__(self, session):
        scales.init(self, '/cassandra')
        # each instance will be registered with a session, and receive a callback for each request generated
        session.add_request_init_listener(self.on_request)

    def on_request(self, rf):
        # This callback is invoked each time a request is created, on the thread creating the request.
        # We can use this to count events, or add callbacks
        rf.add_callbacks(self.on_success, self.on_error, callback_args=(rf,), errback_args=(rf,))

    def on_success(self, _, response_future):
        # future callback on a successful request; just record the size
        self.requests.addValue(response_future.request_encoded_size)

    def on_error(self, _, response_future):
        # future callback for failed; record size and increment errors
        self.requests.addValue(response_future.request_encoded_size)
        self.errors += 1

    def __str__(self):
        # just extracting request count from the size stats (which are recorded on all requests)
        request_sizes = dict(self.requests)
        count = request_sizes.pop('count')
        return "%d requests (%d errors)\nRequest size statistics:\n%s" % (count, self.errors, pp.pformat(request_sizes))


# connect a session
session = Cluster().connect()

# attach a listener to this session
ra = RequestAnalyzer(session)

session.execute("SELECT release_version FROM system.local")
session.execute("SELECT release_version FROM system.local")

print(ra)
# 2 requests (0 errors)
# Request size statistics:
# { '75percentile': 74,
#   '95percentile': 74,
#   '98percentile': 74,
#   '999percentile': 74,
#   '99percentile': 74,
#   'max': 74,
#   'mean': 74.0,
#   'median': 74.0,
#   'min': 74,
#   'stddev': 0.0}

try:
    # intentional error to show that count increase
    session.execute("syntax err")
except Exception as e:
    pass

print()
print(ra)  # note: the counts are updated, but the stats are not because scales only updates every 20s
# 3 requests (1 errors)
# Request size statistics:
# { '75percentile': 74,
#   '95percentile': 74,
#   '98percentile': 74,
#   '999percentile': 74,
#   '99percentile': 74,
#   'max': 74,
#   'mean': 74.0,
#   'median': 74.0,
#   'min': 74,
#   'stddev': 0.0}
