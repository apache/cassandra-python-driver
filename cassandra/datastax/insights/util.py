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

import logging
import traceback
from warnings import warn

from cassandra.util import Version


DSE_60 = Version('6.0.0')
DSE_51_MIN_SUPPORTED = Version('5.1.13')
DSE_60_MIN_SUPPORTED = Version('6.0.5')


log = logging.getLogger(__name__)


def namespace(cls):
    """
    Best-effort method for getting the namespace in which a class is defined.
    """
    try:
        # __module__ can be None
        module = cls.__module__ or ''
    except Exception:
        warn("Unable to obtain namespace for {cls} for Insights, returning ''. "
             "Exception: \n{e}".format(e=traceback.format_exc(), cls=cls))
        module = ''

    module_internal_namespace = _module_internal_namespace_or_emtpy_string(cls)
    if module_internal_namespace:
        return '.'.join((module, module_internal_namespace))
    return module


def _module_internal_namespace_or_emtpy_string(cls):
    """
    Best-effort method for getting the module-internal namespace in which a
    class is defined -- i.e. the namespace _inside_ the module.
    """
    try:
        qualname = cls.__qualname__
    except AttributeError:
        return ''

    return '.'.join(
        # the last segment is the name of the class -- use everything else
        qualname.split('.')[:-1]
    )


def version_supports_insights(dse_version):
    if dse_version:
        try:
            dse_version = Version(dse_version)
            return (DSE_51_MIN_SUPPORTED <= dse_version < DSE_60
                    or
                    DSE_60_MIN_SUPPORTED <= dse_version)
        except Exception:
            warn("Unable to check version {v} for Insights compatibility, returning False. "
                 "Exception: \n{e}".format(e=traceback.format_exc(), v=dse_version))

    return False
