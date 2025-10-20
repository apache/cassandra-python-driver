Installation
============

Supported Platforms
-------------------
Python 3.9 through 3.13 are supported.  Both CPython (the standard Python
implementation) and `PyPy <http://pypy.org>`_ are supported and tested.

Linux, OSX, and Windows are supported.

Installation through pip
------------------------
`pip <https://pypi.org/project/pip/>`_ is the suggested tool for installing
packages.  It will handle installing all Python dependencies for the driver at
the same time as the driver itself.  To install the driver*::

    pip install cassandra-driver

You can use ``pip install --pre cassandra-driver`` if you need to install a beta version.

***Note**: if intending to use optional extensions, install the `dependencies <#optional-non-python-dependencies>`_ first. The driver may need to be reinstalled if dependencies are added after the initial installation.

Verifying your Installation
---------------------------
To check if the installation was successful, you can run::

    python -c 'import cassandra; print(cassandra.__version__)'

It should print something like "3.29.3".

.. _installation-datastax-graph:

(*Optional*) DataStax Graph
---------------------------
The driver provides an optional fluent graph API that depends on Apache TinkerPop (gremlinpython). It is
not installed by default. To be able to build Gremlin traversals, you need to install
the `graph` extra::

    pip install cassandra-driver[graph]

See :doc:`graph_fluent` for more details about this API.

(*Optional*) Compression Support
--------------------------------
Compression can optionally be used for communication between the driver and
Cassandra.  There are currently two supported compression algorithms:
snappy (in Cassandra 1.2+) and LZ4 (only in Cassandra 2.0+).  If either is
available for the driver and Cassandra also supports it, it will
be used automatically.

For lz4 support::

    pip install lz4

For snappy support::

    pip install python-snappy

(If using a Debian Linux derivative such as Ubuntu, it may be easier to
just run ``apt-get install python-snappy``.)

(*Optional*) Metrics Support
----------------------------
The driver has built-in support for capturing :attr:`.Cluster.metrics` about
the queries you run.  However, the ``scales`` library is required to
support this::

    pip install scales

*Optional:* Column-Level Encryption (CLE) Support
--------------------------------------------------
The driver has built-in support for client-side encryption and
decryption of data. For more, see :doc:`column_encryption`.

CLE depends on the Python `cryptography <https://cryptography.io/en/latest/>`_ module.
When installing Python driver 3.27.0. the `cryptography` module is
also downloaded and installed.
If you are using Python driver 3.28.0 or later and want to use CLE, you must
install the `cryptography <https://cryptography.io/en/latest/>`_ module.

You can install this module along with the driver by specifying the `cle` extra::

    pip install cassandra-driver[cle]

Alternatively, you can also install the module directly via `pip`::

    pip install cryptography

Any version of cryptography >= 35.0 will work for the CLE feature.  You can find additional
details at `PYTHON-1351 <https://datastax-oss.atlassian.net/browse/PYTHON-1351>`_

Speeding Up Installation
^^^^^^^^^^^^^^^^^^^^^^^^

By default, installing the driver through ``pip`` uses a pre-compiled, platform-specific wheel when available.
If using a source distribution rather than a wheel, Cython is used to compile certain parts of the driver.
This makes those hot paths faster at runtime, but the Cython compilation
process can take a long time -- as long as 10 minutes in some environments.

In environments where performance is less important, it may be worth it to
:ref:`disable Cython as documented below <cython-extensions>`.
You can also use ``CASS_DRIVER_BUILD_CONCURRENCY`` to increase the number of
threads used to build the driver and any C extensions:

.. code-block:: bash

    $ # installing from source
    $ CASS_DRIVER_BUILD_CONCURRENCY=8 python setup.py install
    $ # installing from pip
    $ CASS_DRIVER_BUILD_CONCURRENCY=8 pip install cassandra-driver

OSX Installation Error
^^^^^^^^^^^^^^^^^^^^^^
If you're installing on OSX and have XCode 5.1 installed, you may see an error like this::

    clang: error: unknown argument: '-mno-fused-madd' [-Wunused-command-line-argument-hard-error-in-future]

To fix this, re-run the installation with an extra compilation flag:

.. code-block:: bash

    ARCHFLAGS=-Wno-error=unused-command-line-argument-hard-error-in-future pip install cassandra-driver

.. _windows_build:

Windows Installation Notes
--------------------------
Installing the driver with extensions in Windows sometimes presents some challenges. A few notes about common
hang-ups:

Setup requires a compiler. When using Python 2, this is as simple as installing `this package <http://aka.ms/vcpython27>`_
(this link is also emitted during install if setuptools is unable to find the resources it needs). Depending on your
system settings, this package may install as a user-specific application. Make sure to install for everyone, or at least
as the user that will be building the Python environment.

It is also possible to run the build with your compiler of choice. Just make sure to have your environment setup with
the proper paths. Make sure the compiler target architecture matches the bitness of your Python runtime.
Perhaps the easiest way to do this is to run the build/install from a Visual Studio Command Prompt (a
shortcut installed with Visual Studio that sources the appropriate environment and presents a shell).

Manual Installation
-------------------
You can always install the driver directly from a source checkout or tarball.
When installing manually, ensure the python dependencies are already
installed. You can find the list of dependencies in
`requirements.txt <https://github.com/datastax/python-driver/blob/master/requirements.txt>`_.

Once the dependencies are installed, simply run::

    python setup.py install


(*Optional*) Non-python Dependencies
------------------------------------
The driver has several **optional** features that have non-Python dependencies.

C Extensions
^^^^^^^^^^^^
By default, a number of extensions are compiled, providing faster hashing
for token-aware routing with the ``Murmur3Partitioner``,
`libev <http://software.schmorp.de/pkg/libev.html>`_ event loop integration,
and Cython optimized extensions.

When installing manually through setup.py, you can disable both with
the ``--no-extensions`` option, or selectively disable them with
with ``--no-murmur3``, ``--no-libev``, or ``--no-cython``.

To compile the extensions, ensure that GCC and the Python headers are available.

On Ubuntu and Debian, this can be accomplished by running::

    $ sudo apt-get install gcc python-dev

On RedHat and RedHat-based systems like CentOS and Fedora::

    $ sudo yum install gcc python-devel

On OS X, homebrew installations of Python should provide the necessary headers.

See :ref:`windows_build` for notes on configuring the build environment on Windows.

.. _cython-extensions:

Cython-based Extensions
~~~~~~~~~~~~~~~~~~~~~~~
By default, this package uses `Cython <http://cython.org/>`_ to optimize core modules and build custom extensions.
This is not a hard requirement, but is engaged by default to build extensions offering better performance than the
pure Python implementation.

This is a costly build phase, especially in clean environments where the Cython compiler must be built
This build phase can be avoided using the build switch, or an environment variable::

    python setup.py install --no-cython

Alternatively, an environment variable can be used to switch this option regardless of
context::

    CASS_DRIVER_NO_CYTHON=1 <your script here>
    - or, to disable all extensions:
    CASS_DRIVER_NO_EXTENSIONS=1 <your script here>

This method is required when using pip, which provides no other way of injecting user options in a single command::

    CASS_DRIVER_NO_CYTHON=1 pip install cassandra-driver
    CASS_DRIVER_NO_CYTHON=1 sudo -E pip install ~/python-driver

The environment variable is the preferred option because it spans all invocations of setup.py, and will
prevent Cython from being materialized as a setup requirement.

If your sudo configuration does not allow SETENV, you must push the option flag down via pip. However, pip
applies these options to all dependencies (which break on the custom flag). Therefore, you must first install
dependencies, then use install-option::

    sudo pip install futures
    sudo pip install --install-option="--no-cython"


Supported Event Loops
^^^^^^^^^^^^^^^^^^^^^
For Python versions before 3.12 the driver uses the ``asyncore`` module for its default
event loop.  Other event loops such as ``libev``, ``gevent`` and ``eventlet`` are also
available via Python modules or C extensions.  Python 3.12 has removed ``asyncore`` entirely
so for this platform one of these other event loops must be used.

libev support
^^^^^^^^^^^^^
If you're on Linux, you should be able to install libev
through a package manager.  For example, on Debian/Ubuntu::

    $ sudo apt-get install libev4 libev-dev

On RHEL/CentOS/Fedora::

    $ sudo yum install libev libev-devel

If you're on Mac OS X, you should be able to install libev
through `Homebrew <http://brew.sh/>`_. For example, on Mac OS X::

    $ brew install libev

The libev extension can now be built for Windows as of Python driver version 3.29.2.  You can
install libev using any Windows package manager.  For example, to install using `vcpkg <https://vcpkg.io>`_::

    $ vcpkg install libev

If successful, you should be able to build and install the extension
(just using ``setup.py build`` or ``setup.py install``) and then use
the libev event loop by doing the following:

.. code-block:: python

    >>> from cassandra.io.libevreactor import LibevConnection
    >>> from cassandra.cluster import Cluster

    >>> cluster = Cluster()
    >>> cluster.connection_class = LibevConnection
    >>> session = cluster.connect()

(*Optional*) Configuring SSL
-----------------------------
Andrew Mussey has published a thorough guide on
`Using SSL with the DataStax Python driver <http://blog.amussey.com/post/64036730812/cassandra-2-0-client-server-ssl-with-datastax-python>`_.
