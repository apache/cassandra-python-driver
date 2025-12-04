
.. |license| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
    :target: https://opensource.org/licenses/Apache-2.0
.. |version| image:: https://badge.fury.io/py/cassandra-driver.svg
    :target: https://badge.fury.io/py/cassandra-driver
.. |pyversion| image:: https://img.shields.io/pypi/pyversions/cassandra-driver.svg
.. |travis| image:: https://api.travis-ci.com/datastax/python-driver.svg?branch=master
    :target: https://travis-ci.com/github/datastax/python-driver

|license| |version| |pyversion| |travis|

Apache Cassandra Python Driver
==============================

A modern, `feature-rich <https://github.com/datastax/python-driver#features>`_ and highly-tunable Python client library for Apache Cassandra (2.1+) and
DataStax Enterprise (4.7+) using exclusively Cassandra's binary protocol and Cassandra Query Language v3.

The driver supports Python 3.9 through 3.13.

**Note:** DataStax products do not support big-endian systems.

Features
--------
* `Synchronous <https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/cluster/index.html#cassandra.cluster.Session.execute>`_ and `Asynchronous <https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/cluster/index.html#cassandra.cluster.Session.execute_async>`_ APIs
* `Simple, Prepared, and Batch statements <https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/query/index.html#cassandra.query.Statement>`_
* Asynchronous IO, parallel execution, request pipelining
* `Connection pooling <https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/cluster/index.html#cassandra.cluster.Cluster.get_core_connections_per_host>`_
* Automatic node discovery
* `Automatic reconnection <https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/policies/index.html#reconnecting-to-dead-hosts>`_
* Configurable `load balancing <https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/policies/index.html#load-balancing>`_ and `retry policies <https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/policies/index.html#retrying-failed-operations>`_
* `Concurrent execution utilities <https://docs.datastax.com/en/developer/python-driver/latest/api/cassandra/concurrent/index.html>`_
* `Object mapper <https://docs.datastax.com/en/developer/python-driver/latest/object_mapper/>`_
* `Connecting to DataStax Astra database (cloud) <https://docs.datastax.com/en/developer/python-driver/latest/cloud/>`_
* DSE Graph execution API
* DSE Geometric type serialization
* DSE PlainText and GSSAPI authentication

Installation
------------
Installation through pip is recommended::

    $ pip install cassandra-driver

For more complete installation instructions, see the
`installation guide <https://docs.datastax.com/en/developer/python-driver/latest/installation/index.html>`_.

Documentation
-------------
The documentation can be found online `here <https://docs.datastax.com/en/developer/python-driver/latest/index.html>`_.

A couple of links for getting up to speed:

* `Installation <https://docs.datastax.com/en/developer/python-driver/latest/installation/index.html>`_
* `Getting started guide <https://docs.datastax.com/en/developer/python-driver/latest/getting_started/index.html>`_
* `API docs <https://docs.datastax.com/en/developer/python-driver/latest/api/index.html>`_
* `Performance tips <https://docs.datastax.com/en/developer/python-driver/latest/performance/index.html>`_

Object Mapper
-------------
cqlengine (originally developed by Blake Eggleston and Jon Haddad, with contributions from the
community) is now maintained as an integral part of this package. Refer to
`documentation here <https://docs.datastax.com/en/developer/python-driver/latest/object_mapper/index.html>`_.

Contributing
------------
See `CONTRIBUTING.rst <https://github.com/datastax/python-driver/blob/master/CONTRIBUTING.rst>`_.

Reporting Problems
------------------
Please report any bugs and make any feature requests on the
`CASSPYTHON project <https://issues.apache.org/jira/issues/?jql=project%20%3D%20CASSPYTHON%20ORDER%20BY%20key%20DESC>`_
of the ASF JIRA.

If you would like to contribute, please feel free to open a pull request.

Getting Help
------------
You can talk about the driver, ask questions and get help in the #cassandra-drivers channel on 
`ASF Slack <https://the-asf.slack.com/>`_.

License
-------
Copyright 2013 The Apache Software Foundation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
