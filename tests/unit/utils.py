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

from functools import wraps
from unittest.mock import patch

from concurrent.futures import Future
from cassandra.cluster import Session


def mock_session_pools(f):
    """
    Helper decorator that allows tests to initialize :class:.`Session` objects
    without actually connecting to a Cassandra cluster.
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        with patch.object(Session, "add_or_renew_pool") as mocked_add_or_renew_pool:
            future = Future()
            future.set_result(object())
            mocked_add_or_renew_pool.return_value = future
            f(*args, **kwargs)
    return wrapper
