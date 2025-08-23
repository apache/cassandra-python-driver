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
 
import time
from functools import wraps


def wait_until(condition, delay, max_attempts):
    """
    Executes a function at regular intervals while the condition
    is false and the amount of attempts < maxAttempts.
    :param condition: a function
    :param delay: the delay in second
    :param max_attempts: the maximum number of attempts. So the timeout
                         of this function is delay*max_attempts
    """
    attempt = 0
    while not condition() and attempt < max_attempts:
        attempt += 1
        time.sleep(delay)

    if attempt >= max_attempts:
        raise Exception("Condition is still False after {} attempts.".format(max_attempts))


def wait_until_not_raised(condition, delay, max_attempts):
    """
    Executes a function at regular intervals while the condition
    doesn't raise an exception and the amount of attempts < maxAttempts.
    :param condition: a function
    :param delay: the delay in second
    :param max_attempts: the maximum number of attempts. So the timeout
                         of this function will be delay*max_attempts
    """
    def wrapped_condition():
        try:
            result = condition()
        except:
            return False, None

        return True, result

    attempt = 0
    while attempt < (max_attempts-1):
        attempt += 1
        success, result = wrapped_condition()
        if success:
            return result

        time.sleep(delay)

    # last attempt, let the exception raise
    return condition()


def late(seconds=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            time.sleep(seconds)
            func(*args, **kwargs)
        return wrapper
    return decorator
