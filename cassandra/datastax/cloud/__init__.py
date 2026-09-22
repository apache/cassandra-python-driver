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

import os
import logging
import json
import sys
import tempfile
import shutil
from urllib.request import urlopen

_HAS_SSL = True
try:
    from ssl import SSLContext, PROTOCOL_TLS, CERT_REQUIRED
except:
    _HAS_SSL = False

from zipfile import ZipFile

# 2.7 vs 3.x
try:
    from zipfile import BadZipFile
except:
    from zipfile import BadZipfile as BadZipFile

from cassandra import DriverException

log = logging.getLogger(__name__)

__all__ = ['get_cloud_config']

DATASTAX_CLOUD_PRODUCT_TYPE = "DATASTAX_APOLLO"


class CloudConfig(object):

    username = None
    password = None
    host = None
    port = None
    keyspace = None
    local_dc = None
    ssl_context = None

    sni_host = None
    sni_port = None
    host_ids = None

    @classmethod
    def from_dict(cls, d):
        c = cls()

        c.port = d.get('port', None)
        try:
            c.port = int(d['port'])
        except:
            pass

        c.username = d.get('username', None)
        c.password = d.get('password', None)
        c.host = d.get('host', None)
        c.keyspace = d.get('keyspace', None)
        c.local_dc = d.get('localDC', None)

        return c


def get_cloud_config(cloud_config):
    if not _HAS_SSL:
        raise DriverException("A Python installation with SSL is required to connect to a cloud cluster.")

    if 'secure_connect_bundle' not in cloud_config:
        raise ValueError("The cloud config doesn't have a secure_connect_bundle specified.")

    try:
        config = read_cloud_config_from_zip(cloud_config)
    except BadZipFile:
        raise ValueError("Unable to open the zip file for the cloud config. Check your secure connect bundle.")

    config = read_metadata_info(config, cloud_config)
    return config


def read_cloud_config_from_zip(cloud_config):
    secure_bundle = cloud_config['secure_connect_bundle']
    use_default_tempdir = cloud_config.get('use_default_tempdir', None)
    with ZipFile(secure_bundle) as zipfile:
        base_dir = tempfile.gettempdir() if use_default_tempdir else os.path.dirname(secure_bundle)
        tmp_dir = tempfile.mkdtemp(dir=base_dir)
        try:
            zipfile.extractall(path=tmp_dir)
            return parse_cloud_config(os.path.join(tmp_dir, 'config.json'), cloud_config)
        finally:
            shutil.rmtree(tmp_dir)


def parse_cloud_config(path, cloud_config):
    with open(path, 'r') as stream:
        data = json.load(stream)

    config = CloudConfig.from_dict(data)
    config_dir = os.path.dirname(path)

    if 'ssl_context' in cloud_config:
        config.ssl_context = cloud_config['ssl_context']
    else:
        # Load the ssl_context before we delete the temporary directory
        ca_cert_location = os.path.join(config_dir, 'ca.crt')
        cert_location = os.path.join(config_dir, 'cert')
        key_location = os.path.join(config_dir, 'key')
        config.ssl_context = _ssl_context_from_cert(ca_cert_location, cert_location, key_location)

    return config


def read_metadata_info(config, cloud_config):
    url = "https://{}:{}/metadata".format(config.host, config.port)
    timeout = cloud_config['connect_timeout'] if 'connect_timeout' in cloud_config else 5
    try:
        response = urlopen(url, context=config.ssl_context, timeout=timeout)
    except Exception as e:
        log.exception(e)
        raise DriverException("Unable to connect to the metadata service at %s. "
                              "Check the cluster status in the cloud console. " % url)

    if response.code != 200:
        raise DriverException(("Error while fetching the metadata at: %s. "
                               "The service returned error code %d." % (url, response.code)))
    return parse_metadata_info(config, response.read().decode('utf-8'))


def parse_metadata_info(config, http_data):
    try:
        data = json.loads(http_data)
    except:
        msg = "Failed to load cluster metadata"
        raise DriverException(msg)

    contact_info = data['contact_info']
    config.local_dc = contact_info['local_dc']

    proxy_info = contact_info['sni_proxy_address'].split(':')
    config.sni_host = proxy_info[0]
    try:
        config.sni_port = int(proxy_info[1])
    except:
        config.sni_port = 9042

    config.host_ids = [host_id for host_id in contact_info['contact_points']]

    return config


def _ssl_context_from_cert(ca_cert_location, cert_location, key_location):
    ssl_context = SSLContext(PROTOCOL_TLS)
    ssl_context.load_verify_locations(ca_cert_location)
    ssl_context.verify_mode = CERT_REQUIRED
    ssl_context.load_cert_chain(certfile=cert_location, keyfile=key_location)

    return ssl_context

