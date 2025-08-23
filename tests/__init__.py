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
import logging
import sys
import socket
import platform
import os
from concurrent.futures import ThreadPoolExecutor

from cassandra import DependencyException

log = logging.getLogger()
log.setLevel('DEBUG')
# if nose didn't already attach a log handler, add one here
if not log.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s [%(module)s:%(lineno)s]: %(message)s'))
    log.addHandler(handler)


def is_eventlet_monkey_patched():
    if 'eventlet.patcher' not in sys.modules:
        return False
    try:
        import eventlet.patcher
        return eventlet.patcher.is_monkey_patched('socket')
    # Yet another case related to PYTHON-1364
    except AttributeError:
        return False

def is_gevent_monkey_patched():
    if 'gevent.monkey' not in sys.modules:
        return False
    import gevent.socket
    return socket.socket is gevent.socket.socket


def is_monkey_patched():
    return is_gevent_monkey_patched() or is_eventlet_monkey_patched()

MONKEY_PATCH_LOOP = bool(os.getenv('MONKEY_PATCH_LOOP', False))
EVENT_LOOP_MANAGER = os.getenv('EVENT_LOOP_MANAGER', "libev")


# If set to to true this will force the Cython tests to run regardless of whether they are installed
cython_env = os.getenv('VERIFY_CYTHON', "False")

VERIFY_CYTHON = False
if(cython_env == 'True'):
    VERIFY_CYTHON = True

thread_pool_executor_class = ThreadPoolExecutor

if "gevent" in EVENT_LOOP_MANAGER:
    import gevent.monkey
    gevent.monkey.patch_all()
    from cassandra.io.geventreactor import GeventConnection
    connection_class = GeventConnection
elif "eventlet" in EVENT_LOOP_MANAGER:
    from eventlet import monkey_patch
    monkey_patch()

    from cassandra.io.eventletreactor import EventletConnection
    connection_class = EventletConnection

    try:
        from futurist import GreenThreadPoolExecutor
        thread_pool_executor_class = GreenThreadPoolExecutor
    except:
        # futurist is installed only with python >=3.7
        pass
elif "asyncore" in EVENT_LOOP_MANAGER:
    from cassandra.io.asyncorereactor import AsyncoreConnection
    connection_class = AsyncoreConnection
elif "twisted" in EVENT_LOOP_MANAGER:
    from cassandra.io.twistedreactor import TwistedConnection
    connection_class = TwistedConnection
elif "asyncio" in EVENT_LOOP_MANAGER:
    from cassandra.io.asyncioreactor import AsyncioConnection
    connection_class = AsyncioConnection
else:
    log.debug("Using default event loop (libev)")
    try:
        from cassandra.io.libevreactor import LibevConnection
        connection_class = LibevConnection
    except DependencyException as e:
        log.debug('Could not import LibevConnection, '
                  'using connection_class=None; '
                  'failed with error:\n {}'.format(
                      repr(e)
                  ))
        log.debug("Will attempt to set connection class at cluster initialization")
        connection_class = None


def is_windows():
    return "Windows" in platform.system()


notwindows = unittest.skipUnless(not is_windows(), "This test is not adequate for windows")
notpypy = unittest.skipUnless(not platform.python_implementation() == 'PyPy', "This tests is not suitable for pypy")
