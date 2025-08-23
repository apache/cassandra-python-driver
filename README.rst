
.. |license| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg
    :target: https://opensource.org/licenses/Apache-2.0
.. |version| image:: https://badge.fury.io/py/cassandra-driver.svg
    :target: https://badge.fury.io/py/cassandra-driver
.. |pyversion| image:: https://img.shields.io/pypi/pyversions/cassandra-driver.svg

|license| |version| |pyversion|

Apache Cassandra Python Driver
==============================

.. image:: https://travis-ci.com/datastax/python-driver.png?branch=master
   :target: https://travis-ci.com/github/datastax/python-driver

A modern, `feature-rich <https://github.com/datastax/python-driver#features>`_ and highly-tunable Python client library for Apache Cassandra (2.1+) and
DataStax Enterprise (4.7+) using exclusively Cassandra's binary protocol and Cassandra Query Language v3.

The driver supports Python 3.9 through 3.13.

**Note:** DataStax products do not support big-endian systems.

Features
--------
* `Synchronous <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Session.execute>`_ and `Asynchronous <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Session.execute_async>`_ APIs
* `Simple, Prepared, and Batch statements <http://datastax.github.io/python-driver/api/cassandra/query.html#cassandra.query.Statement>`_
* Asynchronous IO, parallel execution, request pipelining
* `Connection pooling <http://datastax.github.io/python-driver/api/cassandra/cluster.html#cassandra.cluster.Cluster.get_core_connections_per_host>`_
* Automatic node discovery
* `Automatic reconnection <http://datastax.github.io/python-driver/api/cassandra/policies.html#reconnecting-to-dead-hosts>`_
* Configurable `load balancing <http://datastax.github.io/python-driver/api/cassandra/policies.html#load-balancing>`_ and `retry policies <http://datastax.github.io/python-driver/api/cassandra/policies.html#retrying-failed-operations>`_
* `Concurrent execution utilities <http://datastax.github.io/python-driver/api/cassandra/concurrent.html>`_
* `Object mapper <http://datastax.github.io/python-driver/object_mapper.html>`_
* `Connecting to DataStax Astra database (cloud) <https://docs.datastax.com/en/developer/python-driver/latest/cloud/>`_
* DSE Graph execution API
* DSE Geometric type serialization
* DSE PlainText and GSSAPI authentication

Installation
------------
Installation through pip is recommended::

    $ pip install cassandra-driver

For more complete installation instructions, see the
`installation guide <http://datastax.github.io/python-driver/installation.html>`_.

Documentation
-------------
The documentation can be found online `here <http://datastax.github.io/python-driver/index.html>`_.

A couple of links for getting up to speed:

* `Installation <http://datastax.github.io/python-driver/installation.html>`_
* `Getting started guide <http://datastax.github.io/python-driver/getting_started.html>`_
* `API docs <http://datastax.github.io/python-driver/api/index.html>`_
* `Performance tips <http://datastax.github.io/python-driver/performance.html>`_

Object Mapper
-------------
cqlengine (originally developed by Blake Eggleston and Jon Haddad, with contributions from the
community) is now maintained as an integral part of this package. Refer to
`documentation here <http://datastax.github.io/python-driver/object_mapper.html>`_.

Contributing
------------
See `CONTRIBUTING.md <https://github.com/datastax/python-driver/blob/master/CONTRIBUTING.rst>`_.

Error Handling
--------------
While originally written for the Java driver, users may reference the `Cassandra error handling done right blog <https://www.datastax.com/blog/cassandra-error-handling-done-right>`_ for resolving error handling scenarios with Apache Cassandra.

Reporting Problems
------------------
Please report any bugs and make any feature requests on the
`JIRA <https://datastax-oss.atlassian.net/browse/PYTHON>`_ issue tracker.

If you would like to contribute, please feel free to open a pull request.

Getting Help
------------
Your best options for getting help with the driver are the
`mailing list <https://groups.google.com/a/lists.datastax.com/forum/#!forum/python-driver-user>`_
and the `DataStax Community <https://community.datastax.com>`_.

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
