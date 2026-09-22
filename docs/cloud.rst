Cloud
-----
Connecting
==========
To connect to a DataStax Astra cluster:

1. Download the secure connect bundle from your Astra account.
2. Connect to your cluster with

.. code-block:: python

    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider

    cloud_config = {
        'secure_connect_bundle': '/path/to/secure-connect-dbname.zip'
    }
    auth_provider = PlainTextAuthProvider(username='user', password='pass')
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()

Cloud Config Options
====================

use_default_tempdir
+++++++++++++++++++

The secure connect bundle needs to be extracted to load the certificates into the SSLContext.
By default, the zip location is used as the base dir for the extraction. In some environments,
the zip location file system is read-only (e.g Azure Function). With *use_default_tempdir* set to *True*,
the default temporary directory of the system will be used as base dir.

.. code:: python

  cloud_config = {
        'secure_connect_bundle': '/path/to/secure-connect-dbname.zip',
        'use_default_tempdir': True
  }
  ...

connect_timeout
+++++++++++++++++++

As part of the process of connecting to Astra the Python driver will query a service to retrieve
current information about your cluster.  You can control the connection timeout for this operation
using *connect_timeout*.  If you observe errors in `read_metadata_info` you might consider increasing
this parameter.  This timeout is specified in seconds.

.. code:: python

  cloud_config = {
        'secure_connect_bundle': '/path/to/secure-connect-dbname.zip',
        'connect_timeout': 120
  }
  ...

Astra Differences
==================
In most circumstances, the client code for interacting with an Astra cluster will be the same as interacting with any other Cassandra cluster. The exceptions being:

* A cloud configuration must be passed to a :class:`~.Cluster` instance via the `cloud` attribute (as demonstrated above).
* An SSL connection will be established automatically. Manual SSL configuration is not allowed, and using `ssl_context` or `ssl_options` will result in an exception.
* A :class:`~.Cluster`'s `contact_points` attribute should not be used. The cloud config contains all of the necessary contact information.
* If a consistency level is not specified for an execution profile or query, then :attr:`.ConsistencyLevel.LOCAL_QUORUM` will be used as the default.


Limitations
===========


CqlEngine
=========

When using the object mapper, you can configure cqlengine with :func:`~.cqlengine.connection.set_session`:

.. code:: python

  from cassandra.cqlengine import connection
  ...

  c = Cluster(cloud={'secure_connect_bundle':'/path/to/secure-connect-test.zip'},
              auth_provider=PlainTextAuthProvider('user', 'pass'))
  s = c.connect('myastrakeyspace')
  connection.set_session(s)
  ...

If you are using some third-party libraries (flask, django, etc.), you might not be able to change the
configuration mechanism. For this reason, the `hosts` argument of the default
:func:`~.cqlengine.connection.setup` function will be ignored if a `cloud` config is provided:

.. code:: python

  from cassandra.cqlengine import connection
  ...

  connection.setup(
    None,  # or anything else
    "myastrakeyspace", cloud={
      'secure_connect_bundle':'/path/to/secure-connect-test.zip'
    },
    auth_provider=PlainTextAuthProvider('user', 'pass'))
