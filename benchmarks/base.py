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

from cProfile import Profile
import logging
import os.path
import sys
from threading import Thread
import time
from optparse import OptionParser
import uuid

from greplin import scales

dirname = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dirname)
sys.path.append(os.path.join(dirname, '..'))

import cassandra
from cassandra.cluster import Cluster
from cassandra.io.asyncorereactor import AsyncoreConnection

log = logging.getLogger()
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

logging.getLogger('cassandra').setLevel(logging.WARN)

_log_levels = {
    'CRITICAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'WARN': logging.WARNING,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'NOTSET': logging.NOTSET,
}

have_libev = False
supported_reactors = [AsyncoreConnection]
try:
    from cassandra.io.libevreactor import LibevConnection
    have_libev = True
    supported_reactors.append(LibevConnection)
except ImportError as exc:
    pass

have_asyncio = False
try:
    from cassandra.io.asyncioreactor import AsyncioConnection
    have_asyncio = True
    supported_reactors.append(AsyncioConnection)
except (ImportError, SyntaxError):
    pass

have_twisted = False
try:
    from cassandra.io.twistedreactor import TwistedConnection
    have_twisted = True
    supported_reactors.append(TwistedConnection)
except ImportError as exc:
    log.exception("Error importing twisted")
    pass

KEYSPACE = "testkeyspace" + str(int(time.time()))
TABLE = "testtable"

COLUMN_VALUES = {
    'int': 42,
    'text': "'42'",
    'float': 42.0,
    'uuid': uuid.uuid4(),
    'timestamp': "'2016-02-03 04:05+0000'"
}


def setup(options):
    log.info("Using 'cassandra' package from %s", cassandra.__path__)

    cluster = Cluster(options.hosts, schema_metadata_enabled=False, token_metadata_enabled=False)
    try:
        session = cluster.connect()

        log.debug("Creating keyspace...")
        try:
            session.execute("""
                CREATE KEYSPACE %s
                WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
                """ % options.keyspace)

            log.debug("Setting keyspace...")
        except cassandra.AlreadyExists:
            log.debug("Keyspace already exists")

        session.set_keyspace(options.keyspace)

        log.debug("Creating table...")
        create_table_query = """
            CREATE TABLE {0} (
                thekey text,
        """
        for i in range(options.num_columns):
            create_table_query += "col{0} {1},\n".format(i, options.column_type)
        create_table_query += "PRIMARY KEY (thekey))"

        try:
            session.execute(create_table_query.format(TABLE))
        except cassandra.AlreadyExists:
            log.debug("Table already exists.")

    finally:
        cluster.shutdown()


def teardown(options):
    cluster = Cluster(options.hosts, schema_metadata_enabled=False, token_metadata_enabled=False)
    session = cluster.connect()
    if not options.keep_data:
        session.execute("DROP KEYSPACE " + options.keyspace)
    cluster.shutdown()


def benchmark(thread_class):
    options, args = parse_options()
    for conn_class in options.supported_reactors:
        setup(options)
        log.info("==== %s ====" % (conn_class.__name__,))

        kwargs = {'metrics_enabled': options.enable_metrics,
                  'connection_class': conn_class}
        if options.protocol_version:
            kwargs['protocol_version'] = options.protocol_version
        cluster = Cluster(options.hosts, **kwargs)
        session = cluster.connect(options.keyspace)

        log.debug("Sleeping for two seconds...")
        time.sleep(2.0)


        # Generate the query
        if options.read:
            query = "SELECT * FROM {0}  WHERE thekey = '{{key}}'".format(TABLE)
        else:
            query = "INSERT INTO {0} (thekey".format(TABLE)
            for i in range(options.num_columns):
                query += ", col{0}".format(i)

            query += ") VALUES ('{key}'"
            for i in range(options.num_columns):
                query += ", {0}".format(COLUMN_VALUES[options.column_type])
            query += ")"

        values = None  # we don't use that anymore. Keeping it in case we go back to prepared statements.
        per_thread = options.num_ops // options.threads
        threads = []

        log.debug("Beginning {0}...".format('reads' if options.read else 'inserts'))
        start = time.time()
        try:
            for i in range(options.threads):
                thread = thread_class(
                    i, session, query, values, per_thread,
                    cluster.protocol_version, options.profile)
                thread.daemon = True
                threads.append(thread)

            for thread in threads:
                thread.start()

            for thread in threads:
                while thread.is_alive():
                    thread.join(timeout=0.5)

            end = time.time()
        finally:
            cluster.shutdown()
            teardown(options)

        total = end - start
        log.info("Total time: %0.2fs" % total)
        log.info("Average throughput: %0.2f/sec" % (options.num_ops / total))
        if options.enable_metrics:
            stats = scales.getStats()['cassandra']
            log.info("Connection errors: %d", stats['connection_errors'])
            log.info("Write timeouts: %d", stats['write_timeouts'])
            log.info("Read timeouts: %d", stats['read_timeouts'])
            log.info("Unavailables: %d", stats['unavailables'])
            log.info("Other errors: %d", stats['other_errors'])
            log.info("Retries: %d", stats['retries'])

            request_timer = stats['request_timer']
            log.info("Request latencies:")
            log.info("  min: %0.4fs", request_timer['min'])
            log.info("  max: %0.4fs", request_timer['max'])
            log.info("  mean: %0.4fs", request_timer['mean'])
            log.info("  stddev: %0.4fs", request_timer['stddev'])
            log.info("  median: %0.4fs", request_timer['median'])
            log.info("  75th: %0.4fs", request_timer['75percentile'])
            log.info("  95th: %0.4fs", request_timer['95percentile'])
            log.info("  98th: %0.4fs", request_timer['98percentile'])
            log.info("  99th: %0.4fs", request_timer['99percentile'])
            log.info("  99.9th: %0.4fs", request_timer['999percentile'])


def parse_options():
    parser = OptionParser()
    parser.add_option('-H', '--hosts', default='127.0.0.1',
                      help='cassandra hosts to connect to (comma-separated list) [default: %default]')
    parser.add_option('-t', '--threads', type='int', default=1,
                      help='number of threads [default: %default]')
    parser.add_option('-n', '--num-ops', type='int', default=10000,
                      help='number of operations [default: %default]')
    parser.add_option('--asyncore-only', action='store_true', dest='asyncore_only',
                      help='only benchmark with asyncore connections')
    parser.add_option('--asyncio-only', action='store_true', dest='asyncio_only',
                      help='only benchmark with asyncio connections')
    parser.add_option('--libev-only', action='store_true', dest='libev_only',
                      help='only benchmark with libev connections')
    parser.add_option('--twisted-only', action='store_true', dest='twisted_only',
                      help='only benchmark with Twisted connections')
    parser.add_option('-m', '--metrics', action='store_true', dest='enable_metrics',
                      help='enable and print metrics for operations')
    parser.add_option('-l', '--log-level', default='info',
                      help='logging level: debug, info, warning, or error')
    parser.add_option('-p', '--profile', action='store_true', dest='profile',
                      help='Profile the run')
    parser.add_option('--protocol-version', type='int', dest='protocol_version', default=4,
                      help='Native protocol version to use')
    parser.add_option('-c', '--num-columns', type='int', dest='num_columns', default=2,
                      help='Specify the number of columns for the schema')
    parser.add_option('-k', '--keyspace', type='str', dest='keyspace', default=KEYSPACE,
                      help='Specify the keyspace name for the schema')
    parser.add_option('--keep-data', action='store_true', dest='keep_data', default=False,
                      help='Keep the data after the benchmark')
    parser.add_option('--column-type', type='str', dest='column_type', default='text',
                      help='Specify the column type for the schema (supported: int, text, float, uuid, timestamp)')
    parser.add_option('--read', action='store_true', dest='read', default=False,
                      help='Read mode')


    options, args = parser.parse_args()

    options.hosts = options.hosts.split(',')

    level = options.log_level.upper()
    try:
        log.setLevel(_log_levels[level])
    except KeyError:
        log.warning("Unknown log level specified: %s; specify one of %s", options.log_level, _log_levels.keys())

    if options.asyncore_only:
        options.supported_reactors = [AsyncoreConnection]
    elif options.asyncio_only:
        options.supported_reactors = [AsyncioConnection]
    elif options.libev_only:
        if not have_libev:
            log.error("libev is not available")
            sys.exit(1)
        options.supported_reactors = [LibevConnection]
    elif options.twisted_only:
        if not have_twisted:
            log.error("Twisted is not available")
            sys.exit(1)
        options.supported_reactors = [TwistedConnection]
    else:
        options.supported_reactors = supported_reactors
        if not have_libev:
            log.warning("Not benchmarking libev reactor because libev is not available")

    return options, args


class BenchmarkThread(Thread):

    def __init__(self, thread_num, session, query, values, num_queries, protocol_version, profile):
        Thread.__init__(self)
        self.thread_num = thread_num
        self.session = session
        self.query = query
        self.values = values
        self.num_queries = num_queries
        self.protocol_version = protocol_version
        self.profiler = Profile() if profile else None

    def start_profile(self):
        if self.profiler:
            self.profiler.enable()

    def run_query(self, key, **kwargs):
        return self.session.execute_async(self.query.format(key=key), **kwargs)

    def finish_profile(self):
        if self.profiler:
            self.profiler.disable()
            self.profiler.dump_stats('profile-%d' % self.thread_num)
