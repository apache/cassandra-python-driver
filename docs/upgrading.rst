Upgrading
=========

.. toctree::
   :maxdepth: 1

Upgrading to 3.30.0
-------------------
Version 3.30.0 of the Python driver is the first release since the driver's donation
to the Apache Software Foundation (ASF).

Supported Python Versions
^^^^^^^^^^^^^^^^^^^^^^^^^
An individual version of the Python driver aims to officially support all Python runtimes
that are not end-of-life (EOL) at the time of that version's release.  For 3.30.0 this policy
entails support for Python 3.10 through Python 3.14.  The driver will likely continue to work
reasonably well on older Python runtimes but only these versions are officially supported.

Conversion to pyproject.toml
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
With this release we are moving away from the install and configuration process based on
setup.py and towards the use of pyproject.toml.  As a result of this change (and in keeping
with the typical use of pyproject.toml) configuration of a driver build is now declarative.  All
build options should be specified in pyproject.toml and overrides via command-line flags or
environment variables are no longer supported.  Please consult
`CASSPYTHON-7 <https://issues.apache.org/jira/browse/CASSPYTHON-7>`_ for additional details.

Event Loop Deprecation
^^^^^^^^^^^^^^^^^^^^^^
With this release the eventlet, gevent and Twisted event loops are considered deprecated.  Use
of these event loops in this version will generate a warning to this effect.  We are planning on
removing these event loops in their entirety in the next minor release.  Please consult
`CASSPYTHON-12 <https://issues.apache.org/jira/browse/CASSPYTHON-12>`_ for additional details.

Removal of Win32 Wheels
^^^^^^^^^^^^^^^^^^^^^^^
As of this release we will no longer be offering wheels for Win32 platforms.  Wheels for other
Windows platforms will continue to be deployed to PyPI.  Please consult
`CASSPYTHON-5 <https://issues.apache.org/jira/browse/CASSPYTHON-5>`_ for additional details.

Change to DRIVER_NAME in STARTUP Messages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The DRIVER_NAME property in STARTUP messages has been changed in this release to allow
administrators to clearly distinguish between uses of the previous DataStax Python drivers
and this driver.  Any monitoring/management applications which were monitoring driver usage
based on this string should be aware of this change and update accordingly.  Please consult
`CASSPYTHON-17 <https://issues.apache.org/jira/browse/CASSPYTHON-17>`_ for additional details.


Upgrading from dse-driver
-------------------------
Since 3.21.0, cassandra-driver fully supports DataStax products. dse-driver and
dse-graph users should now migrate to cassandra-driver to benefit from latest bug fixes
and new features. The upgrade to this new unified driver version is straightforward
with no major API changes.

Installation
^^^^^^^^^^^^

Only the `cassandra-driver` package should be installed. `dse-driver` and `dse-graph`
are not required anymore::

    pip install cassandra-driver

If you need the Graph *Fluent* API (features provided by dse-graph)::

    pip install cassandra-driver[graph]

See :doc:`installation` for more details.

Import from the cassandra module
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There is no `dse` module, so you should import from the `cassandra` module. You
need to change only the first module of your import statements, not the submodules.

.. code-block:: python

    from dse.cluster import Cluster, EXEC_PROFILE_GRAPH_DEFAULT
    from dse.auth import PlainTextAuthProvider
    from dse.policies import WhiteListRoundRobinPolicy

    # becomes

    from cassandra.cluster import Cluster, EXEC_PROFILE_GRAPH_DEFAULT
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import WhiteListRoundRobinPolicy

Also note that the cassandra.hosts module doesn't exist in cassandra-driver. This
module is named cassandra.pool.

dse-graph
^^^^^^^^^

dse-graph features are now built-in in cassandra-driver. The only change you need
to do is your import statements:

.. code-block:: python

    from dse_graph import ..
    from dse_graph.query import ..

    # becomes

    from cassandra.datastax.graph.fluent import ..
    from cassandra.datastax.graph.fluent.query import ..

See :mod:`~.datastax.graph.fluent`.

Session.execute and Session.execute_async API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Although it is not common to use this API with positional arguments, it is
important to be aware that the `host` and `execute_as` parameters have had
their positional order swapped. This is only because `execute_as` was added
in dse-driver before `host`.

See :meth:`.Session.execute`.

Deprecations
^^^^^^^^^^^^

These changes are optional, but recommended:

* Importing from `cassandra.graph` is deprecated. Consider importing from `cassandra.datastax.graph`.
* Use :class:`~.policies.DefaultLoadBalancingPolicy` instead of DSELoadBalancingPolicy.

Upgrading to 3.0
----------------
Version 3.0 of the DataStax Python driver for Apache Cassandra
adds support for Cassandra 3.0 while maintaining support for
previously supported versions. In addition to substantial internal rework,
there are several updates to the API that integrators will need
to consider:

Default consistency is now ``LOCAL_ONE``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Previous value was ``ONE``. The new value is introduced to mesh with the default
DC-aware load balancing policy and to match other drivers.

Execution API Updates
^^^^^^^^^^^^^^^^^^^^^
Result return normalization
~~~~~~~~~~~~~~~~~~~~~~~~~~~
`PYTHON-368 <https://datastax-oss.atlassian.net/browse/PYTHON-368>`_

Previously results would be returned as a ``list`` of rows for result rows
up to ``fetch_size``, and ``PagedResult`` afterward. This could break
application code that assumed one type and got another.

Now, all results are returned as an iterable :class:`~.ResultSet`.

The preferred way to consume results of unknown size is to iterate through
them, letting automatic paging occur as they are consumed.

.. code-block:: python

    results = session.execute("SELECT * FROM system.local")
    for row in results:
        process(row)

If the expected size of the results is known, it is still possible to
materialize a list using the iterator:

.. code-block:: python

    results = session.execute("SELECT * FROM system.local")
    row_list = list(results)

For backward compatibility, :class:`~.ResultSet` supports indexing. When
accessed at an index, a `~.ResultSet` object will materialize all its pages:

.. code-block:: python

    results = session.execute("SELECT * FROM system.local")
    first_result = results[0]  # materializes results, fetching all pages

This can send requests and load (possibly large) results into memory, so
`~.ResultSet` will log a warning on implicit materialization.

Trace information is not attached to executed Statements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
`PYTHON-318 <https://datastax-oss.atlassian.net/browse/PYTHON-318>`_

Previously trace data was attached to Statements if tracing was enabled. This
could lead to confusion if the same statement was used for multiple executions.

Now, trace data is associated with the ``ResponseFuture`` and ``ResultSet``
returned for each query:

:meth:`.ResponseFuture.get_query_trace()`

:meth:`.ResponseFuture.get_all_query_traces()`

:meth:`.ResultSet.get_query_trace()`

:meth:`.ResultSet.get_all_query_traces()`

Binding named parameters now ignores extra names
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
`PYTHON-178 <https://datastax-oss.atlassian.net/browse/PYTHON-178Cassadfasdf>`_

Previously, :meth:`.BoundStatement.bind()` would raise if a mapping
was passed with extra names not found in the prepared statement.

Behavior in 3.0+ is to ignore extra names.

blist removed as soft dependency
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
`PYTHON-385 <https://datastax-oss.atlassian.net/browse/PYTHON-385>`_

Previously the driver had a soft dependency on ``blist sortedset``, using
that where available and using an internal fallback where possible.

Now, the driver never chooses the ``blist`` variant, instead returning the
internal :class:`.util.SortedSet` for all ``set`` results. The class implements
all standard set operations, so no integration code should need to change unless
it explicitly checks for ``sortedset`` type.

Metadata API Updates
^^^^^^^^^^^^^^^^^^^^
`PYTHON-276 <https://datastax-oss.atlassian.net/browse/PYTHON-276>`_, `PYTHON-408 <https://datastax-oss.atlassian.net/browse/PYTHON-408>`_, `PYTHON-400 <https://datastax-oss.atlassian.net/browse/PYTHON-400>`_, `PYTHON-422 <https://datastax-oss.atlassian.net/browse/PYTHON-422>`_

Cassandra 3.0 brought a substantial overhaul to the internal schema metadata representation.
This version of the driver supports that metadata in addition to the legacy version. Doing so
also brought some changes to the metadata model.

The present API is documented: :any:`cassandra.metadata`. Changes highlighted below:

* All types are now exposed as CQL types instead of types derived from the internal server implementation
* Some metadata attributes have changed names to match current nomenclature (for example, :attr:`.Index.kind` in place of ``Index.type``).
* Some metadata attributes removed

  * ``TableMetadata.keyspace`` reference replaced with :attr:`.TableMetadata.keyspace_name`
  * ``ColumnMetadata.index`` is removed table- and keyspace-level mappings are still maintained

Several deprecated features are removed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
`PYTHON-292 <https://datastax-oss.atlassian.net/browse/PYTHON-292>`_

* ``ResponseFuture.result`` timeout parameter is removed, use ``Session.execute`` timeout instead (`031ebb0 <https://github.com/datastax/python-driver/commit/031ebb0>`_)
* ``Cluster.refresh_schema`` removed, use ``Cluster.refresh_*_metadata`` instead (`419fcdf <https://github.com/datastax/python-driver/commit/419fcdf>`_)
* ``Cluster.submit_schema_refresh`` removed (`574266d <https://github.com/datastax/python-driver/commit/574266d>`_)
* ``cqltypes`` time/date functions removed, use ``util`` entry points instead (`bb984ee <https://github.com/datastax/python-driver/commit/bb984ee>`_)
* ``decoder`` module removed (`e16a073 <https://github.com/datastax/python-driver/commit/e16a073>`_)
* ``TableMetadata.keyspace`` attribute replaced with ``keyspace_name`` (`cc94073 <https://github.com/datastax/python-driver/commit/cc94073>`_)
* ``cqlengine.columns.TimeUUID.from_datetime`` removed, use ``util`` variant instead (`96489cc <https://github.com/datastax/python-driver/commit/96489cc>`_)
* ``cqlengine.columns.Float(double_precision)`` parameter removed, use ``columns.Double`` instead (`a2d3a98 <https://github.com/datastax/python-driver/commit/a2d3a98>`_)
* ``cqlengine`` keyspace management functions are removed in favor of the strategy-specific entry points (`4bd5909 <https://github.com/datastax/python-driver/commit/4bd5909>`_)
* ``cqlengine.Model.__polymorphic_*__`` attributes removed, use ``__discriminator*`` attributes instead (`9d98c8e <https://github.com/datastax/python-driver/commit/9d98c8e>`_)
* ``cqlengine.statements`` will no longer warn about list list prepend behavior (`79efe97 <https://github.com/datastax/python-driver/commit/79efe97>`_)


Upgrading to 2.1 from 2.0
-------------------------
Version 2.1 of the DataStax Python driver for Apache Cassandra
adds support for Cassandra 2.1 and version 3 of the native protocol.

Cassandra 1.2, 2.0, and 2.1 are all supported.  However, 1.2 only
supports protocol version 1, and 2.0 only supports versions 1 and
2, so some features may not be available.

Using the v3 Native Protocol
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default, the driver will attempt to use version 2 of the
native protocol.  To use version 3, you must explicitly
set the :attr:`~.Cluster.protocol_version`:

.. code-block:: python

    from cassandra.cluster import Cluster

    cluster = Cluster(protocol_version=3)

Note that protocol version 3 is only supported by Cassandra 2.1+.

In future releases, the driver may default to using protocol version
3.

Working with User-Defined Types
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Cassandra 2.1 introduced the ability to define new types::

    USE KEYSPACE mykeyspace;

    CREATE TYPE address (street text, city text, zip int);

The driver generally expects you to use instances of a specific
class to represent column values of this type.  You can let the
driver know what class to use with :meth:`.Cluster.register_user_type`:

.. code-block:: python

    cluster = Cluster()

    class Address(object):

        def __init__(self, street, city, zipcode):
            self.street = street
            self.city = text
            self.zipcode = zipcode

    cluster.register_user_type('mykeyspace', 'address', Address)

When inserting data for ``address`` columns, you should pass in
instances of ``Address``.  When querying data, ``address`` column
values will be instances of ``Address``.

If no class is registered for a user-defined type, query results
will use a ``namedtuple`` class and data may only be inserted
though prepared statements.

See :ref:`udts` for more details.

Customizing Encoders for Non-prepared Statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Starting with version 2.1 of the driver, it is possible to customize
how Python types are converted to CQL literals when working with
non-prepared statements.  This is done on a per-:class:`~.Session`
basis through :attr:`.Session.encoder`:

.. code-block:: python

    cluster = Cluster()
    session = cluster.connect()
    session.encoder.add_or_override_mapping(tuple, session.encoder.cql_encode_tuple)

See :ref:`type-conversions` for the table of default CQL literal conversions.

Using Client-Side Protocol-Level Timestamps
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
With version 3 of the native protocol, timestamps may be supplied by the
client at the protocol level.  (Normally, if they are not specified within
the CQL query itself, a timestamp is generated server-side.)

When :attr:`~.Cluster.protocol_version` is set to 3 or higher, the driver
will automatically use client-side timestamps with microsecond precision
unless :attr:`.Session.use_client_timestamp` is changed to :const:`False`.
If a timestamp is specified within the CQL query, it will override the
timestamp generated by the driver.

Upgrading to 2.0 from 1.x
-------------------------
Version 2.0 of the DataStax Python driver for Apache Cassandra
includes some notable improvements over version 1.x.  This version
of the driver supports Cassandra 1.2, 2.0, and 2.1.  However, not
all features may be used with Cassandra 1.2, and some new features
in 2.1 are not yet supported.

Using the v2 Native Protocol
^^^^^^^^^^^^^^^^^^^^^^^^^^^^
By default, the driver will attempt to use version 2 of Cassandra's
native protocol. You can explicitly set the protocol version to
2, though:

.. code-block:: python

    from cassandra.cluster import Cluster

    cluster = Cluster(protocol_version=2)

When working with Cassandra 1.2, you will need to
explicitly set the :attr:`~.Cluster.protocol_version` to 1:

.. code-block:: python

    from cassandra.cluster import Cluster

    cluster = Cluster(protocol_version=1)

Automatic Query Paging
^^^^^^^^^^^^^^^^^^^^^^
Version 2 of the native protocol adds support for automatic query
paging, which can make dealing with large result sets much simpler.

See :ref:`query-paging` for full details.

Protocol-Level Batch Statements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
With version 1 of the native protocol, batching of statements required
using a `BATCH cql query <http://cassandra.apache.org/doc/cql3/CQL-3.0.html#batchStmt>`_.
With version 2 of the native protocol, you can now batch statements at
the protocol level. This allows you to use many different prepared
statements within a single batch.

See :class:`~.query.BatchStatement` for details and usage examples.

SASL-based Authentication
^^^^^^^^^^^^^^^^^^^^^^^^^
Also new in version 2 of the native protocol is SASL-based authentication.
See the section on :ref:`security` for details and examples.

Lightweight Transactions
^^^^^^^^^^^^^^^^^^^^^^^^
`Lightweight transactions <http://www.datastax.com/dev/blog/lightweight-transactions-in-cassandra-2-0>`_ are another new feature.  To use lightweight transactions, add ``IF`` clauses
to your CQL queries and set the :attr:`~.Statement.serial_consistency_level`
on your statements.

Calling Cluster.shutdown()
^^^^^^^^^^^^^^^^^^^^^^^^^^
In order to fix some issues around garbage collection and unclean interpreter
shutdowns, version 2.0 of the driver requires you to call :meth:`.Cluster.shutdown()`
on your :class:`~.Cluster` objects when you are through with them.
This helps to guarantee a clean shutdown.

Deprecations
^^^^^^^^^^^^
The following functions have moved from ``cassandra.decoder`` to ``cassandra.query``.
The original functions have been left in place with a :exc:`DeprecationWarning` for
now:

* :attr:`cassandra.decoder.tuple_factory` has moved to
  :attr:`cassandra.query.tuple_factory`
* :attr:`cassandra.decoder.named_tuple_factory` has moved to
  :attr:`cassandra.query.named_tuple_factory`
* :attr:`cassandra.decoder.dict_factory` has moved to
  :attr:`cassandra.query.dict_factory`
* :attr:`cassandra.decoder.ordered_dict_factory` has moved to
  :attr:`cassandra.query.ordered_dict_factory`

Dependency Changes
^^^^^^^^^^^^^^^^^^
The following dependencies have officially been made optional:

* ``scales``
* ``blist``
