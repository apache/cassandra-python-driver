3.29.3
======
October 20, 2025

Features
--------
* Upgraded cython to 3.0.x (PR 1221 & PYTHON-1390)
* Add support for DSE 6.9.x and HCD releases to CI (PYTHON-1402)
* Add execute_concurrent_async and expose execute_concurrent_* in Session (PR 1229)

Bug Fixes
---------
* Update geomet to align with requirements.txt (PR 1236)
* Connection failure to SNI endpoint when first host is unavailable (PYTHON-1419)
* Maintain compatibility with CPython 3.13 (PR 1242)

Others
------
* Remove duplicated condition in primary key check (PR 1240)
* Remove Python 3.8 which reached EOL on Oct 2024, update cryptography lib to 42 (PR 1247)
* Remove obsolete urllib2 from ez_setup.py (PR 1248)
* Remove stale dependency on sure (PR 1227)
* Removed 2.7 Cpython defines (PR 1252)

3.29.2
======
September 9, 2024

Features
--------
* Convert to pytest for running unit and integration tests (PYTHON-1297)
* Add support for Cassandra 4.1.x and 5.0 releases to CI (PYTHON-1393)
* Extend driver vector support to arbitrary subtypes and fix handling of variable length types (PYTHON-1369)

Bug Fixes
---------
* Python NumpyProtocolHandler does not work with NumPy 1.24.0 or greater (PYTHON-1359)
* cibuildwheel appears to not be stripping Cython-generated shared objects (PYTHON-1387)
* Windows build for Python 3.12 compiled without libev support (PYTHON-1386)

Others
------
* Update README.rst with badges for version and license (PR 1210)
* Remove dependency on old mock external module (PR 1201)
* Removed future print_function, division, and with and some pre 3.7 handling (PR 1208)
* Update geomet dependency (PR 1207)
* Remove problematic escape sequences in some docstrings to avoid SyntaxWarning in Python 3.12 (PR 1205)
* Use timezone-aware API to avoid deprecated warning (PR 1213)

3.29.1
======
March 19, 2024

Bug Fixes
---------
* cassandra-driver for Python 3.12 Linux is compiled without libev support (PYTHON-1378)
* Consider moving to native wheel builds for OS X and removing universal2 wheels (PYTHON-1379)

3.29.0
======
December 19, 2023

Features
--------
* Add support for Python 3.9 through 3.12, drop support for 3.7 (PYTHON-1283)
* Removal of dependency on six module (PR 1172)
* Raise explicit exception when deserializing a vector with a subtype that isn’t a constant size (PYTHON-1371)

Others
------
* Remove outdated Python pre-3.7 references (PR 1186)
* Remove backup(.bak) files (PR 1185)
* Fix doc typo in add_callbacks (PR 1177)

3.28.0
======
June 5, 2023

Features
--------
* Add support for vector type (PYTHON-1352)
* Cryptography module is now an optional dependency (PYTHON-1351)

Bug Fixes
---------
* Store IV along with encrypted text when using column-level encryption (PYTHON-1350)
* Create session-specific protocol handlers to contain session-specific CLE policies (PYTHON-1356)

Others
------
* Use Cython for smoke builds (PYTHON-1343)
* Don't fail when inserting UDTs with prepared queries with some missing fields (PR 1151)
* Convert print statement to function in docs (PR 1157)
* Update comment for retry policy (DOC-3278)
* Added error handling blog reference (DOC-2813)

3.27.0
======
May 1, 2023

Features
--------
* Add support for client-side encryption (PYTHON-1341)

3.26.0
======
March 13, 2023

Features
--------
* Add support for execution profiles in execute_concurrent (PR 1122)

Bug Fixes
---------
* Handle empty non-final result pages (PR 1110)
* Do not re-use stream IDs for in-flight requests (PR 1114)
* Asyncore race condition cause logging exception on shutdown (PYTHON-1266)

Others
------
* Fix deprecation warning in query tracing (PR 1103)
* Remove mutable default values from some tests (PR 1116)
* Remove dependency on unittest2 (PYTHON-1289)
* Fix deprecation warnings for asyncio.coroutine annotation in asyncioreactor (PYTHON-1290)
* Fix typos in source files (PR 1126)
* HostFilterPolicyInitTest fix for Python 3.11 (PR 1131)
* Fix for DontPrepareOnIgnoredHostsTest (PYTHON-1287)
* tests.integration.simulacron.test_connection failures (PYTHON-1304)
* tests.integration.standard.test_single_interface.py appears to be failing for C* 4.0 (PYTHON-1329)
* Authentication tests appear to be failing fraudulently (PYTHON-1328)
* PreparedStatementTests.test_fail_if_different_query_id_on_reprepare() failing unexpectedly (PTYHON-1327)
* Refactor deprecated unittest aliases for Python 3.11 compatibility (PR 1112)

Deprecations
------------
* This release removes support for Python 2.7.x as well as Python 3.5.x and 3.6.x

3.25.0
======
March 18, 2021

Features
--------
* Ensure the driver can connect when invalid peer hosts are in system.peers (PYTHON-1260)
* Implement protocol v5 checksumming (PYTHON-1258)
* Fix the default cqlengine connection mechanism to work with Astra (PYTHON-1265)

Bug Fixes
---------
* Asyncore race condition cause logging exception on shutdown (PYTHON-1266)
* Update list of reserved keywords (PYTHON-1269)

Others
------
* Drop Python 3.4 support (PYTHON-1220)
* Update security documentation and examples to use PROTOCOL_TLS (PYTHON-1264)

3.24.0
======
June 18, 2020

Features
--------
* Make geomet an optional dependency at runtime (PYTHON-1237)
* Add use_default_tempdir cloud config options (PYTHON-1245)
* Tcp flow control for libevreactor (PYTHON-1248)

Bug Fixes
---------
* Unable to connect to a cloud cluster using Ubuntu 20.04 (PYTHON-1238)
* PlainTextAuthProvider fails with unicode chars and Python3 (PYTHON-1241)
* [GRAPH] Graph execution profiles consistency level are not set to LOCAL_QUORUM with a cloud cluster (PYTHON-1240)
* [GRAPH] Can't write data in a Boolean field using the Fluent API (PYTHON-1239)
* [GRAPH] Fix elementMap() result deserialization (PYTHON-1233)

Others
------
* Bump geomet dependency version to 0.2 (PYTHON-1243)
* Bump gremlinpython dependency version to 3.4.6 (PYTHON-1212)
* Improve fluent graph documentation for core graphs (PYTHON-1244)

3.23.0
======
April 6, 2020

Features
--------
* Transient Replication Support (PYTHON-1207)
* Support system.peers_v2 and port discovery for C* 4.0 (PYTHON-700)

Bug Fixes
---------
* Asyncore logging exception on shutdown (PYTHON-1228)

3.22.0
======
February 26, 2020

Features
--------

* Add all() function to the ResultSet API (PYTHON-1203)
* Parse new schema metadata in NGDG and generate table edges CQL syntax (PYTHON-996)
* Add GraphSON3 support (PYTHON-788)
* Use GraphSON3 as default for Native graphs (PYTHON-1004)
* Add Tuple and UDT types for native graph (PYTHON-1005)
* Add Duration type for native graph (PYTHON-1000)
* Add gx:ByteBuffer graphson type support for Blob field (PYTHON-1027)
* Enable Paging Through DSE Driver for Gremlin Traversals (PYTHON-1045)
* Provide numerical wrappers to ensure proper graphson schema definition (PYTHON-1051)
* Resolve the row_factory automatically for native graphs (PYTHON-1056)
* Add g:TraversalMetrics/g:Metrics graph deserializers (PYTHON-1057)
* Add g:BulkSet graph deserializers (PYTHON-1060)
* Update Graph Engine names and the way to create a Classic/Native Graph (PYTHON-1090)
* Update Native to Core Graph Engine
* Add graphson3 and native graph support (PYTHON-1039)
* Enable Paging Through DSE Driver for Gremlin Traversals (PYTHON-1045)
* Expose filter predicates for cql collections (PYTHON-1019)
* Add g:TraversalMetrics/Metrics deserializers (PYTHON-1057)
* Make graph metadata handling more robust (PYTHON-1204)

Bug Fixes
---------
* Make sure to only query the native_transport_address column with DSE (PYTHON-1205)

3.21.0
======
January 15, 2020

Features
--------
* Unified driver: merge core and DSE drivers into a single package (PYTHON-1130)
* Add Python 3.8 support (PYTHON-1189)
* Allow passing ssl context for Twisted (PYTHON-1161)
* Ssl context and cloud support for Eventlet (PYTHON-1162)
* Cloud Twisted support (PYTHON-1163)
* Add additional_write_policy and read_repair to system schema parsing (PYTHON-1048)
* Flexible version parsing (PYTHON-1174)
* Support NULL in collection deserializer (PYTHON-1123)
* [GRAPH] Ability to execute Fluent Graph queries asynchronously (PYTHON-1129)

Bug Fixes
---------
* Handle prepared id mismatch when repreparing on the fly (PYTHON-1124)
* re-raising the CQLEngineException will fail on Python 3 (PYTHON-1166)
* asyncio message chunks can be processed discontinuously (PYTHON-1185)
* Reconnect attempts persist after downed node removed from peers (PYTHON-1181)
* Connection fails to validate ssl certificate hostname when SSLContext.check_hostname is set (PYTHON-1186)
* ResponseFuture._set_result crashes on connection error when used with PrepareMessage (PYTHON-1187)
* Insights fail to serialize the startup message when the SSL Context is from PyOpenSSL (PYTHON-1192)

Others
------
* The driver has a new dependency: geomet. It comes from the dse-driver unification and
  is used to support DSE geo types.
* Remove *read_repair_chance table options (PYTHON-1140)
* Avoid warnings about unspecified load balancing policy when connecting to a cloud cluster (PYTHON-1177)
* Add new DSE CQL keywords (PYTHON-1122)
* Publish binary wheel distributions (PYTHON-1013)

Deprecations
------------

* DSELoadBalancingPolicy will be removed in the next major, consider using
  the DefaultLoadBalancingPolicy.

Merged from dse-driver:

Features
--------

* Insights integration (PYTHON-1047)
* Graph execution profiles should preserve their graph_source when graph_options is overridden (PYTHON-1021)
* Add NodeSync metadata (PYTHON-799)
* Add new NodeSync failure values (PYTHON-934)
* DETERMINISTIC and MONOTONIC Clauses for Functions and Aggregates (PYTHON-955)
* GraphOptions should show a warning for unknown parameters (PYTHON-819)
* DSE protocol version 2 and continous paging backpressure (PYTHON-798)
* GraphSON2 Serialization/Deserialization Support (PYTHON-775)
* Add graph-results payload option for GraphSON format (PYTHON-773)
* Create an AuthProvider for the DSE transitional mode (PYTHON-831)
* Implement serializers for the Graph String API (PYTHON-778)
* Provide deserializers for GraphSON types (PYTHON-782)
* Add Graph DurationType support (PYTHON-607)
* Support DSE DateRange type (PYTHON-668)
* RLAC CQL output for materialized views (PYTHON-682)
* Add Geom Types wkt deserializer
* DSE Graph Client timeouts in custom payload (PYTHON-589)
* Make DSEGSSAPIAuthProvider accept principal name (PYTHON-574)
* Add config profiles to DSE graph execution (PYTHON-570)
* DSE Driver version checking (PYTHON-568)
* Distinct default timeout for graph queries (PYTHON-477)
* Graph result parsing for known types (PYTHON-479,487)
* Distinct read/write CL for graph execution (PYTHON-509)
* Target graph analytics query to spark master when available (PYTHON-510)

Bug Fixes
---------

* Continuous paging sessions raise RuntimeError when results are not entirely consumed (PYTHON-1054)
* GraphSON Property deserializer should return a dict instead of a set (PYTHON-1033)
* ResponseFuture.has_more_pages may hold the wrong value (PYTHON-946)
* DETERMINISTIC clause in AGGREGATE misplaced in CQL generation (PYTHON-963)
* graph module import cause a DLL issue on Windows due to its cythonizing failure (PYTHON-900)
* Update date serialization to isoformat in graph (PYTHON-805)
* DateRange Parse Error (PYTHON-729)
* MontonicTimestampGenerator.__init__ ignores class defaults (PYTHON-728)
* metadata.get_host returning None unexpectedly (PYTHON-709)
* Sockets associated with sessions not getting cleaned up on session.shutdown() (PYTHON-673)
* Resolve FQDN from ip address and use that as host passed to SASLClient (PYTHON-566)
* Geospatial type implementations don't handle 'EMPTY' values. (PYTHON-481)
* Correctly handle other types in geo type equality (PYTHON-508)

Other
-----
* Add tests around cqlengine and continuous paging (PYTHON-872)
* Add an abstract GraphStatement to handle different graph statements  (PYTHON-789)
* Write documentation examples for DSE 2.0 features (PYTHON-732)
* DSE_V1 protocol should not include all of protocol v5 (PYTHON-694)

3.20.2
======
November 19, 2019

Bug Fixes
---------
* Fix import error for old python installation without SSLContext (PYTHON-1183)

3.20.1
======
November 6, 2019

Bug Fixes
---------
* ValueError: too many values to unpack (expected 2)" when there are two dashes in server version number (PYTHON-1172)

3.20.0
======
October 28, 2019

Features
--------
* DataStax Astra Support (PYTHON-1074)
* Use 4.0 schema parser in 4 alpha and snapshot builds (PYTHON-1158)

Bug Fixes
---------
* Connection setup methods prevent using ExecutionProfile in cqlengine (PYTHON-1009)
* Driver deadlock if all connections dropped by heartbeat whilst request in flight and request times out (PYTHON-1044)
* Exception when use pk__token__gt filter In python 3.7 (PYTHON-1121)

3.19.0
======
August 26, 2019

Features
--------
* Add Python 3.7 support (PYTHON-1016)
* Future-proof Mapping imports (PYTHON-1023)
* Include param values in cqlengine logging (PYTHON-1105)
* NTS Token Replica Map Generation is slow (PYTHON-622)

Bug Fixes
---------
* as_cql_query UDF/UDA parameters incorrectly includes "frozen" if arguments are collections (PYTHON-1031)
* cqlengine does not currently support combining TTL and TIMESTAMP on INSERT (PYTHON-1093)
* Fix incorrect metadata for compact counter tables (PYTHON-1100)
* Call ConnectionException with correct kwargs (PYTHON-1117)
* Can't connect to clusters built from source because version parsing doesn't handle 'x.y-SNAPSHOT' (PYTHON-1118)
* Discovered node doesn´t honor the configured Cluster port on connection (PYTHON-1127)
* Exception when use pk__token__gt filter In python 3.7 (PYTHON-1121)

Other
-----
* Remove invalid warning in set_session when we initialize a default connection (PYTHON-1104)
* Set the proper default ExecutionProfile.row_factory value (PYTHON-1119)

3.18.0
======
May 27, 2019

Features
--------

* Abstract Host Connection information (PYTHON-1079)
* Improve version parsing to support a non-integer 4th component (PYTHON-1091)
* Expose on_request_error method in the RetryPolicy (PYTHON-1064)
* Add jitter to ExponentialReconnectionPolicy (PYTHON-1065)

Bug Fixes
---------

* Fix error when preparing queries with beta protocol v5 (PYTHON-1081)
* Accept legacy empty strings as column names (PYTHON-1082)
* Let util.SortedSet handle uncomparable elements (PYTHON-1087)

3.17.1
======
May 2, 2019

Bug Fixes
---------
* Socket errors EAGAIN/EWOULDBLOCK are not handled properly and cause timeouts (PYTHON-1089)

3.17.0
======
February 19, 2019

Features
--------
* Send driver name and version in startup message (PYTHON-1068)
* Add Cluster ssl_context option to enable SSL (PYTHON-995)
* Allow encrypted private keys for 2-way SSL cluster connections (PYTHON-995)
* Introduce new method ConsistencyLevel.is_serial (PYTHON-1067)
* Add Session.get_execution_profile (PYTHON-932)
* Add host kwarg to Session.execute/execute_async APIs to send a query to a specific node (PYTHON-993)

Bug Fixes
---------
* NoHostAvailable when all hosts are up and connectable (PYTHON-891)
* Serial consistency level is not used (PYTHON-1007)

Other
-----
* Fail faster on incorrect lz4 import (PYTHON-1042)
* Bump Cython dependency version to 0.29 (PYTHON-1036)
* Expand Driver SSL Documentation (PYTHON-740)

Deprecations
------------

* Using Cluster.ssl_options to enable SSL is deprecated and will be removed in
  the next major release, use ssl_context.
* DowngradingConsistencyRetryPolicy is deprecated and will be
  removed in the next major release. (PYTHON-937)

3.16.0
======
November 12, 2018

Bug Fixes
---------
* Improve and fix socket error-catching code in nonblocking-socket reactors (PYTHON-1024)
* Non-ASCII characters in schema break CQL string generation (PYTHON-1008)
* Fix OSS driver's virtual table support against DSE 6.0.X and future server releases (PYTHON-1020)
* ResultSet.one() fails if the row_factory is using a generator (PYTHON-1026)
* Log profile name on attempt to create existing profile (PYTHON-944)
* Cluster instantiation fails if any contact points' hostname resolution fails (PYTHON-895)

Other
-----
* Fix tests when RF is not maintained if we decomission a node (PYTHON-1017)
* Fix wrong use of ResultSet indexing (PYTHON-1015)

3.15.1
======
September 6, 2018

Bug Fixes
---------
* C* 4.0 schema-parsing logic breaks running against DSE 6.0.X (PYTHON-1018)

3.15.0
======
August 30, 2018

Features
--------
* Parse Virtual Keyspace Metadata (PYTHON-992)

Bug Fixes
---------
* Tokenmap.get_replicas returns the wrong value if token coincides with the end of the range (PYTHON-978)
* Python Driver fails with "more than 255 arguments" python exception when > 255 columns specified in query response (PYTHON-893)
* Hang in integration.standard.test_cluster.ClusterTests.test_set_keyspace_twice (PYTHON-998)
* Asyncore reactors should use a global variable instead of a class variable for the event loop (PYTHON-697)

Other
-----
* Use global variable for libev loops so it can be subclassed (PYTHON-973)
* Update SchemaParser for V4 (PYTHON-1006)
* Bump Cython dependency version to 0.28 (PYTHON-1012)

3.14.0
======
April 17, 2018

Features
--------
* Add one() function to the ResultSet API (PYTHON-947)
* Create an utility function to fetch concurrently many keys from the same replica (PYTHON-647)
* Allow filter queries with fields that have an index managed outside of cqlengine (PYTHON-966)
* Twisted SSL Support (PYTHON-343)
* Support IS NOT NULL operator in cqlengine (PYTHON-968)

Other
-----
* Fix Broken Links in Docs (PYTHON-916)
* Reevaluate MONKEY_PATCH_LOOP in test codebase (PYTHON-903)
* Remove CASS_SERVER_VERSION and replace it for CASSANDRA_VERSION in tests (PYTHON-910)
* Refactor CASSANDRA_VERSION to a some kind of version object (PYTHON-915)
* Log warning when driver configures an authenticator, but server does not request authentication (PYTHON-940)
* Warn users when using the deprecated Session.default_consistency_level (PYTHON-953)
* Add DSE smoke test to OSS driver tests (PYTHON-894)
* Document long compilation times and workarounds (PYTHON-868)
* Improve error for batch WriteTimeouts (PYTHON-941)
* Deprecate ResultSet indexing (PYTHON-945)

3.13.0
======
January 30, 2018

Features
--------
* cqlengine: LIKE filter operator (PYTHON-512)
* Support cassandra.query.BatchType with cqlengine BatchQuery (PYTHON-888)

Bug Fixes
---------
* AttributeError: 'NoneType' object has no attribute 'add_timer' (PYTHON-862)
* Support retry_policy in PreparedStatement (PYTHON-861)
* __del__ method in Session is throwing an exception (PYTHON-813)
* LZ4 import issue with recent versions (PYTHON-897)
* ResponseFuture._connection can be None when returning request_id (PYTHON-853)
* ResultSet.was_applied doesn't support batch with LWT statements (PYTHON-848)

Other
-----
* cqlengine: avoid warning when unregistering connection on shutdown (PYTHON-865)
* Fix DeprecationWarning of log.warn (PYTHON-846)
* Fix example_mapper.py for python3 (PYTHON-860)
* Possible deadlock on cassandra.concurrent.execute_concurrent (PYTHON-768)
* Add some known deprecated warnings for 4.x (PYTHON-877)
* Remove copyright dates from copyright notices (PYTHON-863)
* Remove "Experimental" tag from execution profiles documentation (PYTHON-840)
* request_timer metrics descriptions are slightly incorrect (PYTHON-885)
* Remove "Experimental" tag from cqlengine connections documentation (PYTHON-892)
* Set in documentation default consistency for operations is LOCAL_ONE (PYTHON-901)

3.12.0
======
November 6, 2017

Features
--------
* Send keyspace in QUERY, PREPARE, and BATCH messages (PYTHON-678)
* Add IPv4Address/IPv6Address support for inet types (PYTHON-751)
* WriteType.CDC and VIEW missing (PYTHON-794)
* Warn on Cluster init if contact points are specified but LBP isn't (legacy mode) (PYTHON-812)
* Warn on Cluster init if contact points are specified but LBP isn't (exection profile mode) (PYTHON-838)
* Include hash of result set metadata in prepared stmt id (PYTHON-808)
* Add NO_COMPACT startup option (PYTHON-839)
* Add new exception type for CDC (PYTHON-837)
* Allow 0ms in ConstantSpeculativeExecutionPolicy (PYTHON-836)
* Add asyncio reactor (PYTHON-507)

Bug Fixes
---------
* Both _set_final_exception/result called for the same ResponseFuture (PYTHON-630)
* Use of DCAwareRoundRobinPolicy raises NoHostAvailable exception (PYTHON-781)
* Not create two sessions by default in CQLEngine (PYTHON-814)
* Bug when subclassing AyncoreConnection (PYTHON-827)
* Error at cleanup when closing the asyncore connections (PYTHON-829)
* Fix sites where `sessions` can change during iteration (PYTHON-793)
* cqlengine: allow min_length=0 for Ascii and Text column types (PYTHON-735)
* Rare exception when "sys.exit(0)" after query timeouts (PYTHON-752)
* Dont set the session keyspace when preparing statements (PYTHON-843)
* Use of DCAwareRoundRobinPolicy raises NoHostAvailable exception	(PYTHON-781)

Other
------
* Remove DeprecationWarning when using WhiteListRoundRobinPolicy (PYTHON-810)
* Bump Cython dependency version to 0.27 (PYTHON-833)

3.11.0
======
July 24, 2017


Features
--------
* Add idle_heartbeat_timeout cluster option to tune how long to wait for heartbeat responses. (PYTHON-762)
* Add HostFilterPolicy (PYTHON-761)

Bug Fixes
---------
* is_idempotent flag is not propagated from PreparedStatement to BoundStatement (PYTHON-736)
* Fix asyncore hang on exit (PYTHON-767)
* Driver takes several minutes to remove a bad host from session (PYTHON-762)
* Installation doesn't always fall back to no cython in Windows (PYTHON-763)
* Avoid to replace a connection that is supposed to shutdown (PYTHON-772)
* request_ids may not be returned to the pool (PYTHON-739)
* Fix murmur3 on big-endian systems (PYTHON-653)
* Ensure unused connections are closed if a Session is deleted by the GC (PYTHON-774)
* Fix .values_list by using db names internally (cqlengine) (PYTHON-785)


Other
-----
* Bump Cython dependency version to 0.25.2 (PYTHON-754)
* Fix DeprecationWarning when using lz4 (PYTHON-769)
* Deprecate WhiteListRoundRobinPolicy (PYTHON-759)
* Improve upgrade guide for materializing pages (PYTHON-464)
* Documentation for time/date specifies timestamp inupt as microseconds (PYTHON-717)
* Point to DSA Slack, not IRC, in docs index

3.10.0
======
May 24, 2017

Features
--------
* Add Duration type to cqlengine (PYTHON-750)
* Community PR review: Raise error on primary key update only if its value changed (PYTHON-705)
* get_query_trace() contract is ambiguous (PYTHON-196)

Bug Fixes
---------
* Queries using speculative execution policy timeout prematurely (PYTHON-755)
* Fix `map` where results are not consumed (PYTHON-749)
* Driver fails to encode Duration's with large values (PYTHON-747)
* UDT values are not updated correctly in CQLEngine (PYTHON-743)
* UDT types are not validated in CQLEngine (PYTHON-742)
* to_python is not implemented for types columns.Type and columns.Date in CQLEngine (PYTHON-741)
* Clients spin infinitely trying to connect to a host that is drained (PYTHON-734)
* Resulset.get_query_trace returns empty trace sometimes (PYTHON-730)
* Memory grows and doesn't get removed (PYTHON-720)
* Fix RuntimeError caused by change dict size during iteration (PYTHON-708)
* fix ExponentialReconnectionPolicy may throw OverflowError problem (PYTHON-707)
* Avoid using nonexistent prepared statement in ResponseFuture (PYTHON-706)

Other
-----
* Update README (PYTHON-746)
* Test python versions 3.5 and 3.6 (PYTHON-737)
* Docs Warning About Prepare "select *" (PYTHON-626)
* Increase Coverage in CqlEngine Test Suite (PYTHON-505)
* Example SSL connection code does not verify server certificates (PYTHON-469)

3.9.0
=====

Features
--------
* cqlengine: remove elements by key from a map (PYTHON-688)

Bug Fixes
---------
* improve error handling when connecting to non-existent keyspace (PYTHON-665)
* Sockets associated with sessions not getting cleaned up on session.shutdown() (PYTHON-673)
* rare flake on integration.standard.test_cluster.ClusterTests.test_clone_shared_lbp (PYTHON-727)
* MontonicTimestampGenerator.__init__ ignores class defaults (PYTHON-728)
* race where callback or errback for request may not be called (PYTHON-733)
* cqlengine: model.update() should not update columns with a default value that hasn't changed (PYTHON-657)
* cqlengine: field value manager's explicit flag is True when queried back from cassandra (PYTHON-719)

Other
-----
* Connection not closed in example_mapper (PYTHON-723)
* Remove mention of pre-2.0 C* versions from OSS 3.0+ docs (PYTHON-710)

3.8.1
=====
March 16, 2017

Bug Fixes
---------

* implement __le__/__ge__/__ne__ on some custom types (PYTHON-714)
* Fix bug in eventlet and gevent reactors that could cause hangs (PYTHON-721)
* Fix DecimalType regression (PYTHON-724)

3.8.0
=====

Features
--------

* Quote index names in metadata CQL generation (PYTHON-616)
* On column deserialization failure, keep error message consistent between python and cython (PYTHON-631)
* TokenAwarePolicy always sends requests to the same replica for a given key (PYTHON-643)
* Added cql types to result set (PYTHON-648)
* Add __len__ to BatchStatement (PYTHON-650)
* Duration Type for Cassandra (PYTHON-655)
* Send flags with PREPARE message in v5 (PYTHON-684)

Bug Fixes
---------

* Potential Timing issue if application exits prior to session pool initialization (PYTHON-636)
* "Host X.X.X.X has been marked down" without any exceptions (PYTHON-640)
* NoHostAvailable or OperationTimedOut when using execute_concurrent with a generator that inserts into more than one table (PYTHON-642)
* ResponseFuture creates Timers and don't cancel them even when result is received which leads to memory leaks (PYTHON-644)
* Driver cannot connect to Cassandra version > 3 (PYTHON-646)
* Unable to import model using UserType without setuping connection since 3.7 (PYTHON-649)
* Don't prepare queries on ignored hosts on_up (PYTHON-669)
* Sockets associated with sessions not getting cleaned up on session.shutdown() (PYTHON-673)
* Make client timestamps strictly monotonic (PYTHON-676)
* cassandra.cqlengine.connection.register_connection broken when hosts=None (PYTHON-692)

Other
-----

* Create a cqlengine doc section explaining None semantics (PYTHON-623)
* Resolve warnings in documentation generation (PYTHON-645)
* Cython dependency (PYTHON-686)
* Drop Support for Python 2.6 (PYTHON-690)

3.7.1
=====
October 26, 2016

Bug Fixes
---------
* Cython upgrade has broken stable version of cassandra-driver (PYTHON-656)

3.7.0
=====
September 13, 2016

Features
--------
* Add v5 protocol failure map (PYTHON-619)
* Don't return from initial connect on first error (PYTHON-617)
* Indicate failed column when deserialization fails (PYTHON-361)
* Let Cluster.refresh_nodes force a token map rebuild (PYTHON-349)
* Refresh UDTs after "keyspace updated" event with v1/v2 protocol (PYTHON-106)
* EC2 Address Resolver (PYTHON-198)
* Speculative query retries (PYTHON-218)
* Expose paging state in API (PYTHON-200)
* Don't mark host down while one connection is active (PYTHON-498)
* Query request size information (PYTHON-284)
* Avoid quadratic ring processing with invalid replication factors (PYTHON-379)
* Improve Connection/Pool creation concurrency on startup (PYTHON-82)
* Add beta version native protocol flag (PYTHON-614)
* cqlengine: Connections: support of multiple keyspaces and sessions (PYTHON-613)

Bug Fixes
---------
* Race when adding a pool while setting keyspace (PYTHON-628)
* Update results_metadata when prepared statement is reprepared (PYTHON-621)
* CQL Export for Thrift Tables (PYTHON-213)
* cqlengine: default value not applied to UserDefinedType (PYTHON-606)
* cqlengine: columns are no longer hashable (PYTHON-618)
* cqlengine: remove clustering keys from where clause when deleting only static columns (PYTHON-608)

3.6.0
=====
August 1, 2016

Features
--------
* Handle null values in NumpyProtocolHandler (PYTHON-553)
* Collect greplin scales stats per cluster (PYTHON-561)
* Update mock unit test dependency requirement (PYTHON-591)
* Handle Missing CompositeType metadata following C* upgrade (PYTHON-562)
* Improve Host.is_up state for HostDistance.IGNORED hosts (PYTHON-551)
* Utilize v2 protocol's ability to skip result set metadata for prepared statement execution (PYTHON-71)
* Return from Cluster.connect() when first contact point connection(pool) is opened (PYTHON-105)
* cqlengine: Add ContextQuery to allow cqlengine models to switch the keyspace context easily (PYTHON-598)
* Standardize Validation between Ascii and Text types in Cqlengine (PYTHON-609)

Bug Fixes
---------
* Fix geventreactor with SSL support (PYTHON-600)
* Don't downgrade protocol version if explicitly set (PYTHON-537)
* Nonexistent contact point tries to connect indefinitely (PYTHON-549)
* Execute_concurrent can exceed max recursion depth in failure mode (PYTHON-585)
* Libev loop shutdown race (PYTHON-578)
* Include aliases in DCT type string (PYTHON-579)
* cqlengine: Comparison operators for Columns (PYTHON-595)
* cqlengine: disentangle default_time_to_live table option from model query default TTL (PYTHON-538)
* cqlengine: pk__token column name issue with the equality operator (PYTHON-584)
* cqlengine: Fix "__in" filtering operator converts True to string "True" automatically (PYTHON-596)
* cqlengine: Avoid LWTExceptions when updating columns that are part of the condition (PYTHON-580)
* cqlengine: Cannot execute a query when the filter contains all columns (PYTHON-599)
* cqlengine: routing key computation issue when a primary key column is overriden by model inheritance (PYTHON-576)

3.5.0
=====
June 27, 2016

Features
--------
* Optional Execution Profiles for the core driver (PYTHON-569)
* API to get the host metadata associated with the control connection node (PYTHON-583)
* Expose CDC option in table metadata CQL (PYTHON-593)

Bug Fixes
---------
* Clean up Asyncore socket map when fork is detected (PYTHON-577)
* cqlengine: QuerySet only() is not respected when there are deferred fields (PYTHON-560)

3.4.1
=====
May 26, 2016

Bug Fixes
---------
* Gevent connection closes on IO timeout (PYTHON-573)
* "dictionary changed size during iteration" with Python 3 (PYTHON-572)

3.4.0
=====
May 24, 2016

Features
--------
*  Include DSE version and workload in Host data (PYTHON-555)
*  Add a context manager to Cluster and Session (PYTHON-521)
*  Better Error Message for Unsupported Protocol Version (PYTHON-157)
*  Make the error message explicitly state when an error comes from the server (PYTHON-412)
*  Short Circuit meta refresh on topo change if NEW_NODE already exists (PYTHON-557)
*  Show warning when the wrong config is passed to SimpleStatement (PYTHON-219)
*  Return namedtuple result pairs from execute_concurrent (PYTHON-362)
*  BatchStatement should enforce batch size limit in a better way (PYTHON-151)
*  Validate min/max request thresholds for connection pool scaling (PYTHON-220)
*  Handle or warn about multiple hosts with the same rpc_address (PYTHON-365)
*  Write docs around working with datetime and timezones (PYTHON-394)

Bug Fixes
---------
*  High CPU utilization when using asyncore event loop (PYTHON-239)
*  Fix CQL Export for non-ASCII Identifiers (PYTHON-447)
*  Make stress scripts Python 2.6 compatible (PYTHON-434)
*  UnicodeDecodeError when unicode characters in key in BOP (PYTHON-559)
*  WhiteListRoundRobinPolicy should resolve hosts (PYTHON-565)
*  Cluster and Session do not GC after leaving scope (PYTHON-135)
*  Don't wait for schema agreement on ignored nodes (PYTHON-531)
*  Reprepare on_up with many clients causes node overload (PYTHON-556)
*  None inserted into host map when control connection node is decommissioned (PYTHON-548)
*  weakref.ref does not accept keyword arguments (github #585)

3.3.0
=====
May 2, 2016

Features
--------
* Add an AddressTranslator interface (PYTHON-69)
* New Retry Policy Decision - try next host (PYTHON-285)
* Don't mark host down on timeout (PYTHON-286)
* SSL hostname verification (PYTHON-296)
* Add C* version to metadata or cluster objects (PYTHON-301)
* Options to Disable Schema, Token Metadata Processing (PYTHON-327)
* Expose listen_address of node we get ring information from (PYTHON-332)
* Use A-record with multiple IPs for contact points (PYTHON-415)
* Custom consistency level for populating query traces (PYTHON-435)
* Normalize Server Exception Types (PYTHON-443)
* Propagate exception message when DDL schema agreement fails (PYTHON-444)
* Specialized exceptions for metadata refresh methods failure (PYTHON-527)

Bug Fixes
---------
* Resolve contact point hostnames to avoid duplicate hosts (PYTHON-103)
* GeventConnection stalls requests when read is a multiple of the input buffer size (PYTHON-429)
* named_tuple_factory breaks with duplicate "cleaned" col names (PYTHON-467)
* Connection leak if Cluster.shutdown() happens during reconnection (PYTHON-482)
* HostConnection.borrow_connection does not block when all request ids are used (PYTHON-514)
* Empty field not being handled by the NumpyProtocolHandler (PYTHON-550)

3.2.2
=====
April 19, 2016

* Fix counter save-after-no-update (PYTHON-547)

3.2.1
=====
April 13, 2016

* Introduced an update to allow deserializer compilation with recently released Cython 0.24 (PYTHON-542)

3.2.0
=====
April 12, 2016

Features
--------
* cqlengine: Warn on sync_schema type mismatch (PYTHON-260)
* cqlengine: Automatically defer fields with the '=' operator (and immutable values) in select queries (PYTHON-520)
* cqlengine: support non-equal conditions for LWT (PYTHON-528)
* cqlengine: sync_table should validate the primary key composition (PYTHON-532)
* cqlengine: token-aware routing for mapper statements (PYTHON-535)

Bug Fixes
---------
* Deleting a column in a lightweight transaction raises a SyntaxException #325 (PYTHON-249)
* cqlengine: make Token function works with named tables/columns #86 (PYTHON-272)
* comparing models with datetime fields fail #79 (PYTHON-273)
* cython date deserializer integer math should be aligned with CPython (PYTHON-480)
* db_field is not always respected with UpdateStatement (PYTHON-530)
* Sync_table fails on column.Set with secondary index (PYTHON-533)

3.1.1
=====
March 14, 2016

Bug Fixes
---------
* cqlengine: Fix performance issue related to additional "COUNT" queries (PYTHON-522)

3.1.0
=====
March 10, 2016

Features
--------
* Pass name of server auth class to AuthProvider (PYTHON-454)
* Surface schema agreed flag for DDL statements (PYTHON-458)
* Automatically convert float and int to Decimal on serialization (PYTHON-468)
* Eventlet Reactor IO improvement (PYTHON-495)
* Make pure Python ProtocolHandler available even when Cython is present (PYTHON-501)
* Optional Cython deserializer for bytes as bytearray (PYTHON-503)
* Add Session.default_serial_consistency_level (github #510)
* cqlengine: Expose prior state information via cqlengine LWTException (github #343, PYTHON-336)
* cqlengine: Collection datatype "contains" operators support (Cassandra 2.1) #278 (PYTHON-258)
* cqlengine: Add DISTINCT query operator (PYTHON-266)
* cqlengine: Tuple cqlengine api (PYTHON-306)
* cqlengine: Add support for UPDATE/DELETE ... IF EXISTS statements (PYTHON-432)
* cqlengine: Allow nested container types (PYTHON-478)
* cqlengine: Add ability to set query's fetch_size and limit (PYTHON-323)
* cqlengine: Internalize default keyspace from successive set_session (PYTHON-486)
* cqlengine: Warn when Model.create() on Counters (to be deprecated) (PYTHON-333)

Bug Fixes
---------
* Bus error (alignment issues) when running cython on some ARM platforms (PYTHON-450)
* Overflow when decoding large collections (cython) (PYTHON-459)
* Timer heap comparison issue with Python 3 (github #466)
* Cython deserializer date overflow at 2^31 - 1 (PYTHON-452)
* Decode error encountered when cython deserializing large map results (PYTHON-459)
* Don't require Cython for build if compiler or Python header not present (PYTHON-471)
* Unorderable types in task scheduling with Python 3 (h(PYTHON-473)
* cqlengine: Fix crash when updating a UDT column with a None value (github #467)
* cqlengine: Race condition in ..connection.execute with lazy_connect (PYTHON-310)
* cqlengine: doesn't support case sensitive column family names (PYTHON-337)
* cqlengine: UserDefinedType mandatory in create or update (PYTHON-344)
* cqlengine: db_field breaks UserType (PYTHON-346)
* cqlengine: UDT badly quoted (PYTHON-347)
* cqlengine: Use of db_field on primary key prevents querying except while tracing. (PYTHON-351)
* cqlengine: DateType.deserialize being called with one argument vs two (PYTHON-354)
* cqlengine: Querying without setting up connection now throws AttributeError and not CQLEngineException (PYTHON-395)
* cqlengine: BatchQuery multiple time executing execute statements. (PYTHON-445)
* cqlengine: Better error for management functions when no connection set (PYTHON-451)
* cqlengine: Handle None values for UDT attributes in cqlengine (PYTHON-470)
* cqlengine: Fix inserting None for model save (PYTHON-475)
* cqlengine: EQ doesn't map to a QueryOperator (setup race condition) (PYTHON-476)
* cqlengine: class.MultipleObjectsReturned has DoesNotExist as base class (PYTHON-489)
* cqlengine: Typo in cqlengine UserType __len__ breaks attribute assignment (PYTHON-502)


Other
-----

* cqlengine: a major improvement on queryset has been introduced. It
  is a lot more efficient to iterate large datasets: the rows are
  now fetched on demand using the driver pagination.

* cqlengine: the queryset len() and count() behaviors have changed. It
  now executes a "SELECT COUNT(*)" of the query rather than returning
  the size of the internal result_cache (loaded rows). On large
  queryset, you might want to avoid using them due to the performance
  cost. Note that trying to access objects using list index/slicing
  with negative indices also requires a count to be
  executed.



3.0.0
=====
November 24, 2015

Features
--------
* Support datetime.date objects as a DateType (PYTHON-212)
* Add Cluster.update_view_metadata (PYTHON-407)
* QueryTrace option to populate partial trace sessions (PYTHON-438)
* Attach column names to ResultSet (PYTHON-439)
* Change default consistency level to LOCAL_ONE

Bug Fixes
---------
* Properly SerDes nested collections when protocol_version < 3 (PYTHON-215)
* Evict UDTs from UserType cache on change (PYTHON-226)
* Make sure query strings are always encoded UTF-8 (PYTHON-334)
* Track previous value of columns at instantiation in CQLengine (PYTHON-348)
* UDT CQL encoding does not work for unicode values (PYTHON-353)
* NetworkTopologyStrategy#make_token_replica_map does not account for multiple racks in a DC (PYTHON-378)
* Cython integer overflow on decimal type deserialization (PYTHON-433)
* Query trace: if session hasn't been logged, query trace can throw exception (PYTHON-442)

3.0.0rc1
========
November 9, 2015

Features
--------
* Process Modernized Schema Tables for Cassandra 3.0 (PYTHON-276, PYTHON-408, PYTHON-400, PYTHON-422)
* Remove deprecated features (PYTHON-292)
* Don't assign trace data to Statements (PYTHON-318)
* Normalize results return (PYTHON-368)
* Process Materialized View Metadata/Events (PYTHON-371)
* Remove blist as soft dependency (PYTHON-385)
* Change default consistency level to LOCAL_QUORUM (PYTHON-416)
* Normalize CQL query/export in metadata model (PYTHON-405)

Bug Fixes
---------
* Implementation of named arguments bind is non-pythonic (PYTHON-178)
* CQL encoding is incorrect for NaN and Infinity floats (PYTHON-282)
* Protocol downgrade issue with C* 2.0.x, 2.1.x, and python3, with non-default logging (PYTHON-409)
* ValueError when accessing usertype with non-alphanumeric field names (PYTHON-413)
* NumpyProtocolHandler does not play well with PagedResult (PYTHON-430)

2.7.2
=====
September 14, 2015

Bug Fixes
---------
* Resolve CQL export error for UDF with zero parameters (PYTHON-392)
* Remove futures dep. for Python 3 (PYTHON-393)
* Avoid Python closure in cdef (supports earlier Cython compiler) (PYTHON-396)
* Unit test runtime issues (PYTHON-397,398)

2.7.1
=====
August 25, 2015

Bug Fixes
---------
* Explicitly include extension source files in Manifest

2.7.0
=====
August 25, 2015

Cython is introduced, providing compiled extensions for core modules, and
extensions for optimized results deserialization.

Features
--------
* General Performance Improvements for Throughput (PYTHON-283)
* Improve synchronous request performance with Timers (PYTHON-108)
* Enable C Extensions for PyPy Runtime (PYTHON-357)
* Refactor SerDes functionality for pluggable interface (PYTHON-313)
* Cython SerDes Extension (PYTHON-377)
* Accept iterators/generators for execute_concurrent() (PYTHON-123)
* cythonize existing modules (PYTHON-342)
* Pure Python murmur3 implementation (PYTHON-363)
* Make driver tolerant of inconsistent metadata (PYTHON-370)

Bug Fixes
---------
* Drop Events out-of-order Cause KeyError on Processing (PYTHON-358)
* DowngradingConsistencyRetryPolicy doesn't check response count on write timeouts (PYTHON-338)
* Blocking connect does not use connect_timeout (PYTHON-381)
* Properly protect partition key in CQL export (PYTHON-375)
* Trigger error callbacks on timeout (PYTHON-294)

2.6.0
=====
July 20, 2015

Bug Fixes
---------
* Output proper CQL for compact tables with no clustering columns (PYTHON-360)

2.6.0c2
=======
June 24, 2015

Features
--------
* Automatic Protocol Version Downgrade (PYTHON-240)
* cqlengine Python 2.6 compatibility (PYTHON-288)
* Double-dollar string quote UDF body (PYTHON-345)
* Set models.DEFAULT_KEYSPACE when calling set_session (github #352)

Bug Fixes
---------
* Avoid stall while connecting to mixed version cluster (PYTHON-303)
* Make SSL work with AsyncoreConnection in python 2.6.9 (PYTHON-322)
* Fix Murmur3Token.from_key() on Windows (PYTHON-331)
* Fix cqlengine TimeUUID rounding error for Windows (PYTHON-341)
* Avoid invalid compaction options in CQL export for non-SizeTiered (PYTHON-352)

2.6.0c1
=======
June 4, 2015

This release adds support for Cassandra 2.2 features, including version
4 of the native protocol.

Features
--------
* Default load balancing policy to TokenAware(DCAware) (PYTHON-160)
* Configuration option for connection timeout (PYTHON-206)
* Support User Defined Function and Aggregate metadata in C* 2.2 (PYTHON-211)
* Surface request client in QueryTrace for C* 2.2+ (PYTHON-235)
* Implement new request failure messages in protocol v4+ (PYTHON-238)
* Metadata model now maps index meta by index name (PYTHON-241)
* Support new types in C* 2.2: date, time, smallint, tinyint (PYTHON-245, 295)
* cqle: add Double column type and remove Float overload (PYTHON-246)
* Use partition key column information in prepared response for protocol v4+ (PYTHON-277)
* Support message custom payloads in protocol v4+ (PYTHON-280, PYTHON-329)
* Deprecate refresh_schema and replace with functions for specific entities (PYTHON-291)
* Save trace id even when trace complete times out (PYTHON-302)
* Warn when registering client UDT class for protocol < v3 (PYTHON-305)
* Support client warnings returned with messages in protocol v4+ (PYTHON-315)
* Ability to distinguish between NULL and UNSET values in protocol v4+ (PYTHON-317)
* Expose CQL keywords in API (PYTHON-324)

Bug Fixes
---------
* IPv6 address support on Windows (PYTHON-20)
* Convert exceptions during automatic re-preparation to nice exceptions (PYTHON-207)
* cqle: Quote keywords properly in table management functions (PYTHON-244)
* Don't default to GeventConnection when gevent is loaded, but not monkey-patched (PYTHON-289)
* Pass dynamic host from SaslAuthProvider to SaslAuthenticator (PYTHON-300)
* Make protocol read_inet work for Windows (PYTHON-309)
* cqle: Correct encoding for nested types (PYTHON-311)
* Update list of CQL keywords used quoting identifiers (PYTHON-319)
* Make ConstantReconnectionPolicy work with infinite retries (github #327, PYTHON-325)
* Accept UUIDs with uppercase hex as valid in cqlengine (github #335)

2.5.1
=====
April 23, 2015

Bug Fixes
---------
* Fix thread safety in DC-aware load balancing policy (PYTHON-297)
* Fix race condition in node/token rebuild (PYTHON-298)
* Set and send serial consistency parameter (PYTHON-299)

2.5.0
=====
March 30, 2015

Features
--------
* Integrated cqlengine object mapping package
* Utility functions for converting timeuuids and datetime (PYTHON-99)
* Schema metadata fetch window randomized, config options added (PYTHON-202)
* Support for new Date and Time Cassandra types (PYTHON-190)

Bug Fixes
---------
* Fix index target for collection indexes (full(), keys()) (PYTHON-222)
* Thread exception during GIL cleanup (PYTHON-229)
* Workaround for rounding anomaly in datetime.utcfromtime (Python 3.4) (PYTHON-230)
* Normalize text serialization for lookup in OrderedMap (PYTHON-231)
* Support reading CompositeType data (PYTHON-234)
* Preserve float precision in CQL encoding (PYTHON-243)

2.1.4
=====
January 26, 2015

Features
--------
* SaslAuthenticator for Kerberos support (PYTHON-109)
* Heartbeat for network device keepalive and detecting failures on idle connections (PYTHON-197)
* Support nested, frozen collections for Cassandra 2.1.3+ (PYTHON-186)
* Schema agreement wait bypass config, new call for synchronous schema refresh (PYTHON-205)
* Add eventlet connection support (PYTHON-194)

Bug Fixes
---------
* Schema meta fix for complex thrift tables (PYTHON-191)
* Support for 'unknown' replica placement strategies in schema meta (PYTHON-192)
* Resolve stream ID leak on set_keyspace (PYTHON-195)
* Remove implicit timestamp scaling on serialization of numeric timestamps (PYTHON-204)
* Resolve stream id collision when using SASL auth (PYTHON-210)
* Correct unhexlify usage for user defined type meta in Python3 (PYTHON-208)

2.1.3
=====
December 16, 2014

Features
--------
* INFO-level log confirmation that a connection was opened to a node that was marked up (PYTHON-116)
* Avoid connecting to peer with incomplete metadata (PYTHON-163)
* Add SSL support to gevent reactor (PYTHON-174)
* Use control connection timeout in wait for schema agreement (PYTHON-175)
* Better consistency level representation in unavailable+timeout exceptions (PYTHON-180)
* Update schema metadata processing to accommodate coming schema modernization (PYTHON-185)

Bug Fixes
---------
* Support large negative timestamps on Windows (PYTHON-119)
* Fix schema agreement for clusters with peer rpc_addres 0.0.0.0 (PYTHON-166)
* Retain table metadata following keyspace meta refresh (PYTHON-173)
* Use a timeout when preparing a statement for all nodes (PYTHON-179)
* Make TokenAware routing tolerant of statements with no keyspace (PYTHON-181)
* Update add_collback to store/invoke multiple callbacks (PYTHON-182)
* Correct routing key encoding for composite keys (PYTHON-184)
* Include compression option in schema export string when disabled (PYTHON-187)

2.1.2
=====
October 16, 2014

Features
--------
* Allow DCAwareRoundRobinPolicy to be constructed without a local_dc, defaulting
  instead to the DC of a contact_point (PYTHON-126)
* Set routing key in BatchStatement.add() if none specified in batch (PYTHON-148)
* Improved feedback on ValueError using named_tuple_factory with invalid column names (PYTHON-122)

Bug Fixes
---------
* Make execute_concurrent compatible with Python 2.6 (PYTHON-159)
* Handle Unauthorized message on schema_triggers query (PYTHON-155)
* Pure Python sorted set in support of UDTs nested in collections (PYTON-167)
* Support CUSTOM index metadata and string export (PYTHON-165)

2.1.1
=====
September 11, 2014

Features
--------
* Detect triggers and include them in CQL queries generated to recreate
  the schema (github-189)
* Support IPv6 addresses (PYTHON-144) (note: basic functionality added; Windows
  platform not addressed (PYTHON-20))

Bug Fixes
---------
* Fix NetworkTopologyStrategy.export_for_schema (PYTHON-120)
* Keep timeout for paged results (PYTHON-150)

Other
-----
* Add frozen<> type modifier to UDTs and tuples to handle CASSANDRA-7857

2.1.0
=====
August 7, 2014

Bug Fixes
---------
* Correctly serialize and deserialize null values in tuples and
  user-defined types (PYTHON-110)
* Include additional header and lib dirs, allowing libevwrapper to build
  against Homebrew and Mac Ports installs of libev (PYTHON-112 and 804dea3)

2.1.0c1
=======
July 25, 2014

Bug Fixes
---------
* Properly specify UDTs for columns in CREATE TABLE statements
* Avoid moving retries to a new host when using request ID zero (PYTHON-88)
* Don't ignore fetch_size arguments to Statement constructors (github-151)
* Allow disabling automatic paging on a per-statement basis when it's
  enabled by default for the session (PYTHON-93)
* Raise ValueError when tuple query parameters for prepared statements
  have extra items (PYTHON-98)
* Correctly encode nested tuples and UDTs for non-prepared statements (PYTHON-100)
* Raise TypeError when a string is used for contact_points (github #164)
* Include User Defined Types in KeyspaceMetadata.export_as_string() (PYTHON-96)

Other
-----
* Return list collection columns as python lists instead of tuples
  now that tuples are a specific Cassandra type

2.1.0b1
=======
July 11, 2014

This release adds support for Cassandra 2.1 features, including version
3 of the native protocol.

Features
--------
* When using the v3 protocol, only one connection is opened per-host, and
  throughput is improved due to reduced pooling overhead and lock contention.
* Support for user-defined types (Cassandra 2.1+)
* Support for tuple type in (limited usage Cassandra 2.0.9, full usage
  in Cassandra 2.1)
* Protocol-level client-side timestamps (see Session.use_client_timestamp)
* Overridable type encoding for non-prepared statements (see Session.encoders)
* Configurable serial consistency levels for batch statements
* Use io.BytesIO for reduced CPU consumption (github #143)
* Support Twisted as a reactor. Note that a Twisted-compatible
  API is not exposed (so no Deferreds), this is just a reactor
  implementation. (github #135, PYTHON-8)

Bug Fixes
---------
* Fix references to xrange that do not go through "six" in libevreactor and
  geventreactor (github #138)
* Make BoundStatements inherit fetch_size from their parent
  PreparedStatement (PYTHON-80)
* Clear reactor state in child process after forking to prevent errors with
  multiprocessing when the parent process has connected a Cluster before
  forking (github #141)
* Don't share prepared statement lock across Cluster instances
* Format CompositeType and DynamicCompositeType columns correctly in
  CREATE TABLE statements.
* Fix cassandra.concurrent behavior when dealing with automatic paging
  (PYTHON-81)
* Properly defunct connections after protocol errors
* Avoid UnicodeDecodeError when query string is unicode (PYTHON-76)
* Correctly capture dclocal_read_repair_chance for tables and
  use it when generating CREATE TABLE statements (PYTHON-84)
* Avoid race condition with AsyncoreConnection that may cause messages
  to fail to be written until a new message is pushed
* Make sure cluster.metadata.partitioner and cluster.metadata.token_map
  are populated when all nodes in the cluster are included in the
  contact points (PYTHON-90)
* Make Murmur3 hash match Cassandra's hash for all values (PYTHON-89,
  github #147)
* Don't attempt to reconnect to hosts that should be ignored (according
  to the load balancing policy) when a notification is received that the
  host is down.
* Add CAS WriteType, avoiding KeyError on CAS write timeout (PYTHON-91)

2.0.2
=====
June 10, 2014

Bug Fixes
---------
* Add six to requirements.txt
* Avoid KeyError during schema refresh when a keyspace is dropped
  and TokenAwarePolicy is not in use
* Avoid registering multiple atexit cleanup functions when the
  asyncore event loop is restarted multiple times
* Delay initialization of reactors in order to avoid problems
  with shared state when using multiprocessing (PYTHON-60)
* Add python-six to debian dependencies, move python-blist to recommends
* Fix memory leak when libev connections are created and
  destroyed (github #93)
* Ensure token map is rebuilt when hosts are removed from the cluster

2.0.1
=====
May 28, 2014

Bug Fixes
---------
* Fix check for Cluster.is_shutdown in in @run_in_executor
  decorator

2.0.0
=====
May 28, 2014

Features
--------
* Make libev C extension Python3-compatible (PYTHON-70)
* Support v2 protocol authentication (PYTHON-73, github #125)

Bug Fixes
---------
* Fix murmur3 C extension compilation under Python3.4 (github #124)

Merged From 1.x
---------------

Features
^^^^^^^^
* Add Session.default_consistency_level (PYTHON-14)

Bug Fixes
^^^^^^^^^
* Don't strip trailing underscores from column names when using the
  named_tuple_factory (PYTHON-56)
* Ensure replication factors are ints for NetworkTopologyStrategy
  to avoid TypeErrors (github #120)
* Pass WriteType instance to RetryPolicy.on_write_timeout() instead
  of the string name of the write type. This caused write timeout
  errors to always be rethrown instead of retrying. (github #123)
* Avoid submitting tasks to the ThreadPoolExecutor after shutdown. With
  retries enabled, this could cause Cluster.shutdown() to hang under
  some circumstances.
* Fix unintended rebuild of token replica map when keyspaces are
  discovered (on startup), added, or updated and TokenAwarePolicy is not
  in use.
* Avoid rebuilding token metadata when cluster topology has not
  actually changed
* Avoid preparing queries for hosts that should be ignored (such as
  remote hosts when using the DCAwareRoundRobinPolicy) (PYTHON-75)

Other
^^^^^
* Add 1 second timeout to join() call on event loop thread during
  interpreter shutdown.  This can help to prevent the process from
  hanging during shutdown.

2.0.0b1
=======
May 6, 2014

Upgrading from 1.x
------------------
Cluster.shutdown() should always be called when you are done with a
Cluster instance.  If it is not called, there are no guarantees that the
driver will not hang.  However, if you *do* have a reproduceable case
where Cluster.shutdown() is not called and the driver hangs, please
report it so that we can attempt to fix it.

If you're using the 2.0 driver against Cassandra 1.2, you will need
to set your protocol version to 1.  For example:

    cluster = Cluster(..., protocol_version=1)

Features
--------
* Support v2 of Cassandra's native protocol, which includes the following
  new features: automatic query paging support, protocol-level batch statements,
  and lightweight transactions
* Support for Python 3.3 and 3.4
* Allow a default query timeout to be set per-Session

Bug Fixes
---------
* Avoid errors during interpreter shutdown (the driver attempts to cleanup
  daemonized worker threads before interpreter shutdown)

Deprecations
------------
The following functions have moved from cassandra.decoder to cassandra.query.
The original functions have been left in place with a DeprecationWarning for
now:

* cassandra.decoder.tuple_factory has moved to cassandra.query.tuple_factory
* cassandra.decoder.named_tuple_factory has moved to cassandra.query.named_tuple_factory
* cassandra.decoder.dict_factory has moved to cassandra.query.dict_factory
* cassandra.decoder.ordered_dict_factory has moved to cassandra.query.ordered_dict_factory

Exceptions that were in cassandra.decoder have been moved to cassandra.protocol. If
you handle any of these exceptions, you must adjust the code accordingly.

1.1.2
=====
May 8, 2014

Features
--------
* Allow a specific compression type to be requested for communications with
  Cassandra and prefer lz4 if available

Bug Fixes
---------
* Update token metadata (for TokenAware calculations) when a node is removed
  from the ring
* Fix file handle leak with gevent reactor due to blocking Greenlet kills when
  closing excess connections
* Avoid handling a node coming up multiple times due to a reconnection attempt
  succeeding close to the same time that an UP notification is pushed
* Fix duplicate node-up handling, which could result in multiple reconnectors
  being started as well as the executor threads becoming deadlocked, preventing
  future node up or node down handling from being executed.
* Handle exhausted ReconnectionPolicy schedule correctly

Other
-----
* Don't log at ERROR when a connection is closed during the startup
  communications
* Mke scales, blist optional dependencies

1.1.1
=====
April 16, 2014

Bug Fixes
---------
* Fix unconditional import of nose in setup.py (github #111)

1.1.0
=====
April 16, 2014

Features
--------
* Gevent is now supported through monkey-patching the stdlib (PYTHON-7,
  github issue #46)
* Support static columns in schemas, which are available starting in
  Cassandra 2.1. (github issue #91)
* Add debian packaging (github issue #101)
* Add utility methods for easy concurrent execution of statements. See
  the new cassandra.concurrent module. (github issue #7)

Bug Fixes
---------
* Correctly supply compaction and compression parameters in CREATE statements
  for tables when working with Cassandra 2.0+
* Lowercase boolean literals when generating schemas
* Ignore SSL_ERROR_WANT_READ and SSL_ERROR_WANT_WRITE socket errors.  Previously,
  these resulted in the connection being defuncted, but they can safely be
  ignored by the driver.
* Don't reconnect the control connection every time Cluster.connect() is
  called
* Avoid race condition that could leave ResponseFuture callbacks uncalled
  if the callback was added outside of the event loop thread (github issue #95)
* Properly escape keyspace name in Session.set_keyspace().  Previously, the
  keyspace name was quoted, but any quotes in the string were not escaped.
* Avoid adding hosts to the load balancing policy before their datacenter
  and rack information has been set, if possible.
* Avoid KeyError when updating metadata after droping a table (github issues
  #97, #98)
* Use tuples instead of sets for DCAwareLoadBalancingPolicy to ensure equal
  distribution of requests

Other
-----
* Don't ignore column names when parsing typestrings.  This is needed for
  user-defined type support.  (github issue #90)
* Better error message when libevwrapper is not found
* Only try to import scales when metrics are enabled (github issue #92)
* Cut down on the number of queries executing when a new Cluster
  connects and when the control connection has to reconnect (github issue #104,
  PYTHON-59)
* Issue warning log when schema versions do not match

1.0.2
=====
March 4, 2014

Bug Fixes
---------
* With asyncorereactor, correctly handle EAGAIN/EWOULDBLOCK when the message from
  Cassandra is a multiple of the read buffer size.  Previously, if no more data
  became available to read on the socket, the message would never be processed,
  resulting in an OperationTimedOut error.
* Double quote keyspace, table and column names that require them (those using
  uppercase characters or keywords) when generating CREATE statements through
  KeyspaceMetadata and TableMetadata.
* Decode TimestampType as DateType.  (Cassandra replaced DateType with
  TimestampType to fix sorting of pre-unix epoch dates in CASSANDRA-5723.)
* Handle latest table options when parsing the schema and generating
  CREATE statements.
* Avoid 'Set changed size during iteration' during query plan generation
  when hosts go up or down

Other
-----
* Remove ignored ``tracing_enabled`` parameter for ``SimpleStatement``.  The
  correct way to trace a query is by setting the ``trace`` argument to ``True``
  in ``Session.execute()`` and ``Session.execute_async()``.
* Raise TypeError instead of cassandra.query.InvalidParameterTypeError when
  a parameter for a prepared statement has the wrong type; remove
  cassandra.query.InvalidParameterTypeError.
* More consistent type checking for query parameters
* Add option to a return special object for empty string values for non-string
  columns

1.0.1
=====
Feb 19, 2014

Bug Fixes
---------
* Include table indexes in ``KeyspaceMetadata.export_as_string()``
* Fix broken token awareness on ByteOrderedPartitioner
* Always close socket when defuncting error'ed connections to avoid a potential
  file descriptor leak
* Handle "custom" types (such as the replaced DateType) correctly
* With libevreactor, correctly handle EAGAIN/EWOULDBLOCK when the message from
  Cassandra is a multiple of the read buffer size.  Previously, if no more data
  became available to read on the socket, the message would never be processed,
  resulting in an OperationTimedOut error.
* Don't break tracing when a Session's row_factory is not the default
  namedtuple_factory.
* Handle data that is already utf8-encoded for UTF8Type values
* Fix token-aware routing for tokens that fall before the first node token in
  the ring and tokens that exactly match a node's token
* Tolerate null source_elapsed values for Trace events.  These may not be
  set when events complete after the main operation has already completed.

Other
-----
* Skip sending OPTIONS message on connection creation if compression is
  disabled or not available and a CQL version has not been explicitly
  set
* Add details about errors and the last queried host to ``OperationTimedOut``

1.0.0 Final
===========
Jan 29, 2014

Bug Fixes
---------
* Prevent leak of Scheduler thread (even with proper shutdown)
* Correctly handle ignored hosts, which are common with the
  DCAwareRoundRobinPolicy
* Hold strong reference to prepared statement while executing it to avoid
  garbage collection
* Add NullHandler logging handler to the cassandra package to avoid
  warnings about there being no configured logger
* Fix bad handling of nodes that have been removed from the cluster
* Properly escape string types within cql collections
* Handle setting the same keyspace twice in a row
* Avoid race condition during schema agreement checks that could result
  in schema update queries returning before all nodes had seen the change
* Preserve millisecond-level precision in datetimes when performing inserts
  with simple (non-prepared) statements
* Properly defunct connections when libev reports an error by setting
  errno instead of simply logging the error
* Fix endless hanging of some requests when using the libev reactor
* Always start a reconnection process when we fail to connect to
  a newly bootstrapped node
* Generators map to CQL lists, not key sequences
* Always defunct connections when an internal operation fails
* Correctly break from handle_write() if nothing was sent (asyncore
  reactor only)
* Avoid potential double-erroring of callbacks when a connection
  becomes defunct

Features
--------
* Add default query timeout to ``Session``
* Add timeout parameter to ``Session.execute()``
* Add ``WhiteListRoundRobinPolicy`` as a load balancing policy option
* Support for consistency level ``LOCAL_ONE``
* Make the backoff for fetching traces exponentially increasing and
  configurable

Other
-----
* Raise Exception if ``TokenAwarePolicy`` is used against a cluster using the
  ``Murmur3Partitioner`` if the murmur3 C extension has not been compiled
* Add encoder mapping for ``OrderedDict``
* Use timeouts on all control connection queries
* Benchmark improvements, including command line options and eay
  multithreading support
* Reduced lock contention when using the asyncore reactor
* Warn when non-datetimes are used for 'timestamp' column values in
  prepared statements
* Add requirements.txt and test-requirements.txt
* TravisCI integration for running unit tests against Python 2.6,
  Python 2.7, and PyPy

1.0.0b7
=======
Nov 12, 2013

This release makes many stability improvements, especially around
prepared statements and node failure handling.  In particular,
several cases where a request would never be completed (and as a
result, leave the application hanging) have been resolved.

Features
--------
* Add `timeout` kwarg to ``ResponseFuture.result()``
* Create connection pools to all hosts in parallel when initializing
  new Sesssions.

Bug Fixes
---------
* Properly set exception on ResponseFuture when a query fails
  against all hosts
* Improved cleanup and reconnection efforts when reconnection fails
  on a node that has recently come up
* Use correct consistency level when retrying failed operations
  against a different host. (An invalid consistency level was being
  used, causing the retry to fail.)
* Better error messages for failed ``Session.prepare()`` opertaions
* Prepare new statements against all hosts in parallel (formerly
  sequential)
* Fix failure to save the new current keyspace on connections. (This
  could cause problems for prepared statements and lead to extra
  operations to continuously re-set the keyspace.)
* Avoid sharing ``LoadBalancingPolicies`` across ``Cluster`` instances. (When
  a second ``Cluster`` was connected, it effectively mark nodes down for the
  first ``Cluster``.)
* Better handling of failures during the re-preparation sequence for
  unrecognized prepared statements
* Throttle trashing of underutilized connections to avoid trashing newly
  created connections
* Fix race condition which could result in trashed connections being closed
  before the last operations had completed
* Avoid preparing statements on the event loop thread (which could lead to
  deadlock)
* Correctly mark up non-contact point nodes discovered by the control
  connection. (This lead to prepared statements not being prepared
  against those hosts, generating extra traffic later when the
  statements were executed and unrecognized.)
* Correctly handle large messages through libev
* Add timeout to schema agreement check queries
* More complete (and less contended) locking around manipulation of the
  pending message deque for libev connections

Other
-----
* Prepare statements in batches of 10. (When many prepared statements
  are in use, this allows the driver to start utilizing nodes that
  were restarted more quickly.)
* Better debug logging around connection management
* Don't retain unreferenced prepared statements in the local cache.
  (If many different prepared statements were created, this would
  increase memory usage and greatly increase the amount of time
  required to begin utilizing a node that was added or marked
  up.)

1.0.0b6
=======
Oct 22, 2013

Bug Fixes
---------
* Use lazy string formatting when logging
* Avoid several deadlock scenarios, especially when nodes go down
* Avoid trashing newly created connections due to insufficient traffic
* Gracefully handle un-handled Exceptions when erroring callbacks

Other
-----
* Node state listeners (which are called when a node is added, removed,
  goes down, or comes up) should now be registered through
  Cluster.register_listener() instead of through a host's HealthMonitor
  (which has been removed)


1.0.0b5
========
Oct 10, 2013

Features
--------
* SSL support

Bug Fixes
---------
* Avoid KeyError when building replica map for NetworkTopologyStrategy
* Work around python bug which causes deadlock when a thread imports
  the utf8 module
* Handle no blist library, which is not compatible with pypy
* Avoid deadlock triggered by a keyspace being set on a connection (which
  may happen automatically for new connections)

Other
-----
* Switch packaging from Distribute to setuptools, improved C extension
  support
* Use PEP 386 compliant beta and post-release versions

1.0.0-beta4
===========
Sep 24, 2013

Features
--------
* Handle new blob syntax in Cassandra 2.0 by accepting bytearray
  objects for blob values
* Add cql_version kwarg to Cluster.__init__

Bug Fixes
---------
* Fix KeyError when building token map with NetworkTopologyStrategy
  keyspaces (this prevented a Cluster from successfully connecting
  at all).
* Don't lose default consitency level from parent PreparedStatement
  when creating BoundStatements

1.0.0-beta3
===========
Sep 20, 2013

Features
--------
* Support for LZ4 compression (Cassandra 2.0+)
* Token-aware routing will now utilize all replicas for a query instead
  of just the first replica

Bug Fixes
---------
* Fix libev include path for CentOS
* Fix varint packing of the value 0
* Correctly pack unicode values
* Don't attempt to return failed connections to the pool when a final result
  is set
* Fix bad iteration of connection credentials
* Use blist's orderedset for set collections and OrderedDict for map
  collections so that Cassandra's ordering is preserved
* Fix connection failure on Windows due to unavailability of inet_pton
  and inet_ntop.  (Note that IPv6 inet_address values are still not
  supported on Windows.)
* Boolean constants shouldn't be surrounded by single quotes
* Avoid a potential loss of precision on float constants due to string
  formatting
* Actually utilize non-standard ports set on Cluster objects
* Fix export of schema as a set of CQL queries

Other
-----
* Use cStringIO for connection buffer for better performance
* Add __repr__ method for Statement classes
* Raise InvalidTypeParameterError when parameters of the wrong
  type are used with statements
* Make all tests compatible with Python 2.6
* Add 1s timeout for opening new connections

1.0.0-beta2
===========
Aug 19, 2013

Bug Fixes
---------
* Fix pip packaging

1.0.0-beta
==========
Aug 16, 2013

Initial release
