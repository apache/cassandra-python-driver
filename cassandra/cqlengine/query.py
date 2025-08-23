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

import copy
from datetime import datetime, timedelta
from functools import partial
import time
from warnings import warn

from cassandra.query import SimpleStatement, BatchType as CBatchType, BatchStatement
from cassandra.cqlengine import columns, CQLEngineException, ValidationError, UnicodeMixin
from cassandra.cqlengine import connection as conn
from cassandra.cqlengine.functions import Token, BaseQueryFunction, QueryValue
from cassandra.cqlengine.operators import (InOperator, EqualsOperator, GreaterThanOperator,
                                           GreaterThanOrEqualOperator, LessThanOperator,
                                           LessThanOrEqualOperator, ContainsOperator, BaseWhereOperator)
from cassandra.cqlengine.statements import (WhereClause, SelectStatement, DeleteStatement,
                                            UpdateStatement, InsertStatement,
                                            BaseCQLStatement, MapDeleteClause, ConditionalClause)


class QueryException(CQLEngineException):
    pass


class IfNotExistsWithCounterColumn(CQLEngineException):
    pass


class IfExistsWithCounterColumn(CQLEngineException):
    pass


class LWTException(CQLEngineException):
    """Lightweight conditional exception.

    This exception will be raised when a write using an `IF` clause could not be
    applied due to existing data violating the condition. The existing data is
    available through the ``existing`` attribute.

    :param existing: The current state of the data which prevented the write.
    """
    def __init__(self, existing):
        super(LWTException, self).__init__("LWT Query was not applied")
        self.existing = existing


class DoesNotExist(QueryException):
    pass


class MultipleObjectsReturned(QueryException):
    pass


def check_applied(result):
    """
    Raises LWTException if it looks like a failed LWT request. A LWTException
    won't be raised in the special case in which there are several failed LWT
    in a  :class:`~cqlengine.query.BatchQuery`.
    """
    try:
        applied = result.was_applied
    except Exception:
        applied = True  # result was not LWT form
    if not applied:
        raise LWTException(result.one())


class AbstractQueryableColumn(UnicodeMixin):
    """
    exposes cql query operators through pythons
    builtin comparator symbols
    """

    def _get_column(self):
        raise NotImplementedError

    def __unicode__(self):
        raise NotImplementedError

    def _to_database(self, val):
        if isinstance(val, QueryValue):
            return val
        else:
            return self._get_column().to_database(val)

    def in_(self, item):
        """
        Returns an in operator

        used where you'd typically want to use python's `in` operator
        """
        return WhereClause(str(self), InOperator(), item)

    def contains_(self, item):
        """
        Returns a CONTAINS operator
        """
        return WhereClause(str(self), ContainsOperator(), item)


    def __eq__(self, other):
        return WhereClause(str(self), EqualsOperator(), self._to_database(other))

    def __gt__(self, other):
        return WhereClause(str(self), GreaterThanOperator(), self._to_database(other))

    def __ge__(self, other):
        return WhereClause(str(self), GreaterThanOrEqualOperator(), self._to_database(other))

    def __lt__(self, other):
        return WhereClause(str(self), LessThanOperator(), self._to_database(other))

    def __le__(self, other):
        return WhereClause(str(self), LessThanOrEqualOperator(), self._to_database(other))


class BatchType(object):
    Unlogged = 'UNLOGGED'
    Counter = 'COUNTER'


class BatchQuery(object):
    """
    Handles the batching of queries

    http://docs.datastax.com/en/cql/3.0/cql/cql_reference/batch_r.html

    See :doc:`/cqlengine/batches` for more details.
    """
    warn_multiple_exec = True

    _consistency = None

    _connection = None
    _connection_explicit = False


    def __init__(self, batch_type=None, timestamp=None, consistency=None, execute_on_exception=False,
                 timeout=conn.NOT_SET, connection=None):
        """
        :param batch_type: (optional) One of batch type values available through BatchType enum
        :type batch_type: BatchType, str or None
        :param timestamp: (optional) A datetime or timedelta object with desired timestamp to be applied
            to the batch conditional.
        :type timestamp: datetime or timedelta or None
        :param consistency: (optional) One of consistency values ("ANY", "ONE", "QUORUM" etc)
        :type consistency: The :class:`.ConsistencyLevel` to be used for the batch query, or None.
        :param execute_on_exception: (Defaults to False) Indicates that when the BatchQuery instance is used
            as a context manager the queries accumulated within the context must be executed despite
            encountering an error within the context. By default, any exception raised from within
            the context scope will cause the batched queries not to be executed.
        :type execute_on_exception: bool
        :param timeout: (optional) Timeout for the entire batch (in seconds), if not specified fallback
            to default session timeout
        :type timeout: float or None
        :param str connection: Connection name to use for the batch execution
        """
        self.queries = []
        self.batch_type = batch_type
        if timestamp is not None and not isinstance(timestamp, (datetime, timedelta)):
            raise CQLEngineException('timestamp object must be an instance of datetime')
        self.timestamp = timestamp
        self._consistency = consistency
        self._execute_on_exception = execute_on_exception
        self._timeout = timeout
        self._callbacks = []
        self._executed = False
        self._context_entered = False
        self._connection = connection
        if connection:
            self._connection_explicit = True

    def add_query(self, query):
        if not isinstance(query, BaseCQLStatement):
            raise CQLEngineException('only BaseCQLStatements can be added to a batch query')
        self.queries.append(query)

    def consistency(self, consistency):
        self._consistency = consistency

    def _execute_callbacks(self):
        for callback, args, kwargs in self._callbacks:
            callback(*args, **kwargs)

    def add_callback(self, fn, *args, **kwargs):
        """Add a function and arguments to be passed to it to be executed after the batch executes.

        A batch can support multiple callbacks.

        Note, that if the batch does not execute, the callbacks are not executed.
        A callback, thus, is an "on batch success" handler.

        :param fn: Callable object
        :type fn: callable
        :param args: Positional arguments to be passed to the callback at the time of execution
        :param kwargs: Named arguments to be passed to the callback at the time of execution
        """
        if not callable(fn):
            raise ValueError("Value for argument 'fn' is {0} and is not a callable object.".format(type(fn)))
        self._callbacks.append((fn, args, kwargs))

    def execute(self):
        if self._executed and self.warn_multiple_exec:
            msg = "Batch executed multiple times."
            if self._context_entered:
                msg += " If using the batch as a context manager, there is no need to call execute directly."
            warn(msg)
        self._executed = True

        if len(self.queries) == 0:
            # Empty batch is a no-op
            # except for callbacks
            self._execute_callbacks()
            return

        batch_type = None if self.batch_type is CBatchType.LOGGED else self.batch_type
        opener = 'BEGIN ' + (str(batch_type) + ' ' if batch_type else '') + ' BATCH'
        if self.timestamp:

            if isinstance(self.timestamp, int):
                ts = self.timestamp
            elif isinstance(self.timestamp, (datetime, timedelta)):
                ts = self.timestamp
                if isinstance(self.timestamp, timedelta):
                    ts += datetime.now()  # Apply timedelta
                ts = int(time.mktime(ts.timetuple()) * 1e+6 + ts.microsecond)
            else:
                raise ValueError("Batch expects a long, a timedelta, or a datetime")

            opener += ' USING TIMESTAMP {0}'.format(ts)

        query_list = [opener]
        parameters = {}
        ctx_counter = 0
        for query in self.queries:
            query.update_context_id(ctx_counter)
            ctx = query.get_context()
            ctx_counter += len(ctx)
            query_list.append('  ' + str(query))
            parameters.update(ctx)

        query_list.append('APPLY BATCH;')

        tmp = conn.execute('\n'.join(query_list), parameters, self._consistency, self._timeout, connection=self._connection)
        check_applied(tmp)

        self.queries = []
        self._execute_callbacks()

    def __enter__(self):
        self._context_entered = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # don't execute if there was an exception by default
        if exc_type is not None and not self._execute_on_exception:
            return
        self.execute()


class ContextQuery(object):
    """
    A Context manager to allow a Model to switch context easily. Presently, the context only
    specifies a keyspace for model IO.

    :param args: One or more models. A model should be a class type, not an instance.
    :param kwargs: (optional) Context parameters: can be *keyspace* or *connection*

    For example:

    .. code-block:: python

            with ContextQuery(Automobile, keyspace='test2') as A:
                A.objects.create(manufacturer='honda', year=2008, model='civic')
                print(len(A.objects.all()))  # 1 result

            with ContextQuery(Automobile, keyspace='test4') as A:
                print(len(A.objects.all()))  # 0 result

            # Multiple models
            with ContextQuery(Automobile, Automobile2, connection='cluster2') as (A, A2):
                print(len(A.objects.all()))
                print(len(A2.objects.all()))

    """

    def __init__(self, *args, **kwargs):
        from cassandra.cqlengine import models

        self.models = []

        if len(args) < 1:
            raise ValueError("No model provided.")

        keyspace = kwargs.pop('keyspace', None)
        connection = kwargs.pop('connection', None)

        if kwargs:
            raise ValueError("Unknown keyword argument(s): {0}".format(
                ','.join(kwargs.keys())))

        for model in args:
            try:
                issubclass(model, models.Model)
            except TypeError:
                raise ValueError("Models must be derived from base Model.")

            m = models._clone_model_class(model, {})

            if keyspace:
                m.__keyspace__ = keyspace
            if connection:
                m.__connection__ = connection

            self.models.append(m)

    def __enter__(self):
        if len(self.models) > 1:
            return tuple(self.models)
        return self.models[0]

    def __exit__(self, exc_type, exc_val, exc_tb):
        return


class AbstractQuerySet(object):

    def __init__(self, model):
        super(AbstractQuerySet, self).__init__()
        self.model = model

        # Where clause filters
        self._where = []

        # Conditional clause filters
        self._conditional = []

        # ordering arguments
        self._order = []

        self._allow_filtering = False

        # CQL has a default limit of 10000, it's defined here
        # because explicit is better than implicit
        self._limit = 10000

        # We store the fields for which we use the Equal operator
        # in a query, so we don't select it from the DB. _defer_fields
        # will contain the names of the fields in the DB, not the names
        # of the variables used by the mapper
        self._defer_fields = set()
        self._deferred_values = {}

        # This variable will hold the names in the database of the fields
        # for which we want to query
        self._only_fields = []

        self._values_list = False
        self._flat_values_list = False

        # results cache
        self._result_cache = None
        self._result_idx = None
        self._result_generator = None
        self._materialize_results = True

        self._distinct_fields = None

        self._count = None

        self._batch = None
        self._ttl =  None
        self._consistency = None
        self._timestamp = None
        self._if_not_exists = False
        self._timeout = conn.NOT_SET
        self._if_exists = False
        self._fetch_size = None
        self._connection = None

    @property
    def column_family_name(self):
        return self.model.column_family_name()

    def _execute(self, statement):
        if self._batch:
            return self._batch.add_query(statement)
        else:
            connection = self._connection or self.model._get_connection()
            result = _execute_statement(self.model, statement, self._consistency, self._timeout, connection=connection)
            if self._if_not_exists or self._if_exists or self._conditional:
                check_applied(result)
            return result

    def __unicode__(self):
        return str(self._select_query())

    def __str__(self):
        return str(self.__unicode__())

    def __call__(self, *args, **kwargs):
        return self.filter(*args, **kwargs)

    def __deepcopy__(self, memo):
        clone = self.__class__(self.model)
        for k, v in self.__dict__.items():
            if k in ['_con', '_cur', '_result_cache', '_result_idx', '_result_generator', '_construct_result']:  # don't clone these, which are per-request-execution
                clone.__dict__[k] = None
            elif k == '_batch':
                # we need to keep the same batch instance across
                # all queryset clones, otherwise the batched queries
                # fly off into other batch instances which are never
                # executed, thx @dokai
                clone.__dict__[k] = self._batch
            elif k == '_timeout':
                clone.__dict__[k] = self._timeout
            else:
                clone.__dict__[k] = copy.deepcopy(v, memo)

        return clone

    def __len__(self):
        self._execute_query()
        return self.count()

    # ----query generation / execution----

    def _select_fields(self):
        """ returns the fields to select """
        return []

    def _validate_select_where(self):
        """ put select query validation here """

    def _select_query(self):
        """
        Returns a select clause based on the given filter args
        """
        if self._where:
            self._validate_select_where()
        return SelectStatement(
            self.column_family_name,
            fields=self._select_fields(),
            where=self._where,
            order_by=self._order,
            limit=self._limit,
            allow_filtering=self._allow_filtering,
            distinct_fields=self._distinct_fields,
            fetch_size=self._fetch_size
        )

    # ----Reads------

    def _execute_query(self):
        if self._batch:
            raise CQLEngineException("Only inserts, updates, and deletes are available in batch mode")
        if self._result_cache is None:
            self._result_generator = (i for i in self._execute(self._select_query()))
            self._result_cache = []
            self._construct_result = self._maybe_inject_deferred(self._get_result_constructor())

            # "DISTINCT COUNT()" is not supported in C* < 2.2, so we need to materialize all results to get
            # len() and count() working with DISTINCT queries
            if self._materialize_results or self._distinct_fields:
                self._fill_result_cache()

    def _fill_result_cache(self):
        """
        Fill the result cache with all results.
        """

        idx = 0
        try:
            while True:
                idx += 1000
                self._fill_result_cache_to_idx(idx)
        except StopIteration:
            pass

        self._count = len(self._result_cache)

    def _fill_result_cache_to_idx(self, idx):
        self._execute_query()
        if self._result_idx is None:
            self._result_idx = -1

        qty = idx - self._result_idx
        if qty < 1:
            return
        else:
            for idx in range(qty):
                self._result_idx += 1
                while True:
                    try:
                        self._result_cache[self._result_idx] = self._construct_result(self._result_cache[self._result_idx])
                        break
                    except IndexError:
                        self._result_cache.append(next(self._result_generator))

    def __iter__(self):
        self._execute_query()

        idx = 0
        while True:
            if len(self._result_cache) <= idx:
                try:
                    self._result_cache.append(next(self._result_generator))
                except StopIteration:
                    break

            instance = self._result_cache[idx]
            if isinstance(instance, dict):
                self._fill_result_cache_to_idx(idx)
            yield self._result_cache[idx]

            idx += 1

    def __getitem__(self, s):
        self._execute_query()

        if isinstance(s, slice):
            start = s.start if s.start else 0

            if start < 0 or (s.stop is not None and s.stop < 0):
                warn("ModelQuerySet slicing with negative indices support will be removed in 4.0.",
                     DeprecationWarning)

            # calculate the amount of results that need to be loaded
            end = s.stop
            if start < 0 or s.stop is None or s.stop < 0:
                end = self.count()

            try:
                self._fill_result_cache_to_idx(end)
            except StopIteration:
                pass

            return self._result_cache[start:s.stop:s.step]
        else:
            try:
                s = int(s)
            except (ValueError, TypeError):
                raise TypeError('QuerySet indices must be integers')

            if s < 0:
                warn("ModelQuerySet indexing with negative indices support will be removed in 4.0.",
                     DeprecationWarning)

            # Using negative indexing is costly since we have to execute a count()
            if s < 0:
                num_results = self.count()
                s += num_results

            try:
                self._fill_result_cache_to_idx(s)
            except StopIteration:
                raise IndexError

            return self._result_cache[s]

    def _get_result_constructor(self):
        """
        Returns a function that will be used to instantiate query results
        """
        raise NotImplementedError

    @staticmethod
    def _construct_with_deferred(f, deferred, row):
        row.update(deferred)
        return f(row)

    def _maybe_inject_deferred(self, constructor):
        return partial(self._construct_with_deferred, constructor, self._deferred_values)\
            if self._deferred_values else constructor

    def batch(self, batch_obj):
        """
        Set a batch object to run the query on.

        Note: running a select query with a batch object will raise an exception
        """
        if self._connection:
            raise CQLEngineException("Cannot specify the connection on model in batch mode.")

        if batch_obj is not None and not isinstance(batch_obj, BatchQuery):
            raise CQLEngineException('batch_obj must be a BatchQuery instance or None')
        clone = copy.deepcopy(self)
        clone._batch = batch_obj
        return clone

    def first(self):
        try:
            return next(iter(self))
        except StopIteration:
            return None

    def all(self):
        """
        Returns a queryset matching all rows

        .. code-block:: python

            for user in User.objects().all():
                print(user)
        """
        return copy.deepcopy(self)

    def consistency(self, consistency):
        """
        Sets the consistency level for the operation. See :class:`.ConsistencyLevel`.

        .. code-block:: python

            for user in User.objects(id=3).consistency(CL.ONE):
                print(user)
        """
        clone = copy.deepcopy(self)
        clone._consistency = consistency
        return clone

    def _parse_filter_arg(self, arg):
        """
        Parses a filter arg in the format:
        <colname>__<op>
        :returns: colname, op tuple
        """
        statement = arg.rsplit('__', 1)
        if len(statement) == 1:
            return arg, None
        elif len(statement) == 2:
            return (statement[0], statement[1]) if arg != 'pk__token' else (arg, None)
        else:
            raise QueryException("Can't parse '{0}'".format(arg))

    def iff(self, *args, **kwargs):
        """Adds IF statements to queryset"""
        if len([x for x in kwargs.values() if x is None]):
            raise CQLEngineException("None values on iff are not allowed")

        clone = copy.deepcopy(self)
        for operator in args:
            if not isinstance(operator, ConditionalClause):
                raise QueryException('{0} is not a valid query operator'.format(operator))
            clone._conditional.append(operator)

        for arg, val in kwargs.items():
            if isinstance(val, Token):
                raise QueryException("Token() values are not valid in conditionals")

            col_name, col_op = self._parse_filter_arg(arg)
            try:
                column = self.model._get_column(col_name)
            except KeyError:
                raise QueryException("Can't resolve column name: '{0}'".format(col_name))

            if isinstance(val, BaseQueryFunction):
                query_val = val
            else:
                query_val = column.to_database(val)

            operator_class = BaseWhereOperator.get_operator(col_op or 'EQ')
            operator = operator_class()
            clone._conditional.append(WhereClause(column.db_field_name, operator, query_val))

        return clone

    def filter(self, *args, **kwargs):
        """
        Adds WHERE arguments to the queryset, returning a new queryset

        See :ref:`retrieving-objects-with-filters`

        Returns a QuerySet filtered on the keyword arguments
        """
        # add arguments to the where clause filters
        if len([x for x in kwargs.values() if x is None]):
            raise CQLEngineException("None values on filter are not allowed")

        clone = copy.deepcopy(self)
        for operator in args:
            if not isinstance(operator, WhereClause):
                raise QueryException('{0} is not a valid query operator'.format(operator))
            clone._where.append(operator)

        for arg, val in kwargs.items():
            col_name, col_op = self._parse_filter_arg(arg)
            quote_field = True

            if not isinstance(val, Token):
                try:
                    column = self.model._get_column(col_name)
                except KeyError:
                    raise QueryException("Can't resolve column name: '{0}'".format(col_name))
            else:
                if col_name != 'pk__token':
                    raise QueryException("Token() values may only be compared to the 'pk__token' virtual column")

                column = columns._PartitionKeysToken(self.model)
                quote_field = False

                partition_columns = column.partition_columns
                if len(partition_columns) != len(val.value):
                    raise QueryException(
                        'Token() received {0} arguments but model has {1} partition keys'.format(
                            len(val.value), len(partition_columns)))
                val.set_columns(partition_columns)

            # get query operator, or use equals if not supplied
            operator_class = BaseWhereOperator.get_operator(col_op or 'EQ')
            operator = operator_class()

            if isinstance(operator, InOperator):
                if not isinstance(val, (list, tuple)):
                    raise QueryException('IN queries must use a list/tuple value')
                query_val = [column.to_database(v) for v in val]
            elif isinstance(val, BaseQueryFunction):
                query_val = val
            elif (isinstance(operator, ContainsOperator) and
                  isinstance(column, (columns.List, columns.Set, columns.Map))):
                # For ContainsOperator and collections, we query using the value, not the container
                query_val = val
            else:
                query_val = column.to_database(val)
                if not col_op:  # only equal values should be deferred
                    clone._defer_fields.add(column.db_field_name)
                    clone._deferred_values[column.db_field_name] = val  # map by db field name for substitution in results

            clone._where.append(WhereClause(column.db_field_name, operator, query_val, quote_field=quote_field))

        return clone

    def get(self, *args, **kwargs):
        """
        Returns a single instance matching this query, optionally with additional filter kwargs.

        See :ref:`retrieving-objects-with-filters`

        Returns a single object matching the QuerySet.

        .. code-block:: python

            user = User.get(id=1)

        If no objects are matched, a :class:`~.DoesNotExist` exception is raised.

        If more than one object is found, a :class:`~.MultipleObjectsReturned` exception is raised.
        """
        if args or kwargs:
            return self.filter(*args, **kwargs).get()

        self._execute_query()

        # Check that the resultset only contains one element, avoiding sending a COUNT query
        try:
            self[1]
            raise self.model.MultipleObjectsReturned('Multiple objects found')
        except IndexError:
            pass

        try:
            obj = self[0]
        except IndexError:
            raise self.model.DoesNotExist

        return obj

    def _get_ordering_condition(self, colname):
        order_type = 'DESC' if colname.startswith('-') else 'ASC'
        colname = colname.replace('-', '')

        return colname, order_type

    def order_by(self, *colnames):
        """
        Sets the column(s) to be used for ordering

        Default order is ascending, prepend a '-' to any column name for descending

        *Note: column names must be a clustering key*

        .. code-block:: python

            from uuid import uuid1,uuid4

            class Comment(Model):
                photo_id = UUID(primary_key=True)
                comment_id = TimeUUID(primary_key=True, default=uuid1) # second primary key component is a clustering key
                comment = Text()

            sync_table(Comment)

            u = uuid4()
            for x in range(5):
                Comment.create(photo_id=u, comment="test %d" % x)

            print("Normal")
            for comment in Comment.objects(photo_id=u):
                print(comment.comment_id)

            print("Reversed")
            for comment in Comment.objects(photo_id=u).order_by("-comment_id"):
                print(comment.comment_id)
        """
        if len(colnames) == 0:
            clone = copy.deepcopy(self)
            clone._order = []
            return clone

        conditions = []
        for colname in colnames:
            conditions.append('"{0}" {1}'.format(*self._get_ordering_condition(colname)))

        clone = copy.deepcopy(self)
        clone._order.extend(conditions)
        return clone

    def count(self):
        """
        Returns the number of rows matched by this query.

        *Note: This function executes a SELECT COUNT() and has a performance cost on large datasets*
        """
        if self._batch:
            raise CQLEngineException("Only inserts, updates, and deletes are available in batch mode")

        if self._count is None:
            query = self._select_query()
            query.count = True
            result = self._execute(query)
            count_row = result.one().popitem()
            self._count = count_row[1]
        return self._count

    def distinct(self, distinct_fields=None):
        """
        Returns the DISTINCT rows matched by this query.

        distinct_fields default to the partition key fields if not specified.

        *Note: distinct_fields must be a partition key or a static column*

        .. code-block:: python

            class Automobile(Model):
                manufacturer = columns.Text(partition_key=True)
                year = columns.Integer(primary_key=True)
                model = columns.Text(primary_key=True)
                price = columns.Decimal()

            sync_table(Automobile)

            # create rows

            Automobile.objects.distinct()

            # or

            Automobile.objects.distinct(['manufacturer'])

        """

        clone = copy.deepcopy(self)
        if distinct_fields:
            clone._distinct_fields = distinct_fields
        else:
            clone._distinct_fields = [x.column_name for x in self.model._partition_keys.values()]

        return clone

    def limit(self, v):
        """
        Limits the number of results returned by Cassandra. Use *0* or *None* to disable.

        *Note that CQL's default limit is 10,000, so all queries without a limit set explicitly will have an implicit limit of 10,000*

        .. code-block:: python

            # Fetch 100 users
            for user in User.objects().limit(100):
                print(user)

            # Fetch all users
            for user in User.objects().limit(None):
                print(user)
        """

        if v is None:
            v = 0

        if not isinstance(v, int):
            raise TypeError
        if v == self._limit:
            return self

        if v < 0:
            raise QueryException("Negative limit is not allowed")

        clone = copy.deepcopy(self)
        clone._limit = v
        return clone

    def fetch_size(self, v):
        """
        Sets the number of rows that are fetched at a time.

        *Note that driver's default fetch size is 5000.*

        .. code-block:: python

            for user in User.objects().fetch_size(500):
                print(user)
        """

        if not isinstance(v, int):
            raise TypeError
        if v == self._fetch_size:
            return self

        if v < 1:
            raise QueryException("fetch size less than 1 is not allowed")

        clone = copy.deepcopy(self)
        clone._fetch_size = v
        return clone

    def allow_filtering(self):
        """
        Enables the (usually) unwise practice of querying on a clustering key without also defining a partition key
        """
        clone = copy.deepcopy(self)
        clone._allow_filtering = True
        return clone

    def _only_or_defer(self, action, fields):
        if action == 'only' and self._only_fields:
            raise QueryException("QuerySet already has 'only' fields defined")

        clone = copy.deepcopy(self)

        # check for strange fields
        missing_fields = [f for f in fields if f not in self.model._columns.keys()]
        if missing_fields:
            raise QueryException(
                "Can't resolve fields {0} in {1}".format(
                    ', '.join(missing_fields), self.model.__name__))

        fields = [self.model._columns[field].db_field_name for field in fields]

        if action == 'defer':
            clone._defer_fields.update(fields)
        elif action == 'only':
            clone._only_fields = fields
        else:
            raise ValueError

        return clone

    def only(self, fields):
        """ Load only these fields for the returned query """
        return self._only_or_defer('only', fields)

    def defer(self, fields):
        """ Don't load these fields for the returned query """
        return self._only_or_defer('defer', fields)

    def create(self, **kwargs):
        return self.model(**kwargs) \
            .batch(self._batch) \
            .ttl(self._ttl) \
            .consistency(self._consistency) \
            .if_not_exists(self._if_not_exists) \
            .timestamp(self._timestamp) \
            .if_exists(self._if_exists) \
            .using(connection=self._connection) \
            .save()

    def delete(self):
        """
        Deletes the contents of a query
        """
        # validate where clause
        partition_keys = set(x.db_field_name for x in self.model._partition_keys.values())
        if partition_keys - set(c.field for c in self._where):
            raise QueryException("The partition key must be defined on delete queries")

        dq = DeleteStatement(
            self.column_family_name,
            where=self._where,
            timestamp=self._timestamp,
            conditionals=self._conditional,
            if_exists=self._if_exists
        )
        self._execute(dq)

    def __eq__(self, q):
        if len(self._where) == len(q._where):
            return all([w in q._where for w in self._where])
        return False

    def __ne__(self, q):
        return not (self != q)

    def timeout(self, timeout):
        """
        :param timeout: Timeout for the query (in seconds)
        :type timeout: float or None
        """
        clone = copy.deepcopy(self)
        clone._timeout = timeout
        return clone

    def using(self, keyspace=None, connection=None):
        """
        Change the context on-the-fly of the Model class (keyspace, connection)
        """

        if connection and self._batch:
            raise CQLEngineException("Cannot specify a connection on model in batch mode.")

        clone = copy.deepcopy(self)
        if keyspace:
            from cassandra.cqlengine.models import _clone_model_class
            clone.model = _clone_model_class(self.model, {'__keyspace__': keyspace})

        if connection:
            clone._connection = connection

        return clone


class ResultObject(dict):
    """
    adds attribute access to a dictionary
    """

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError


class SimpleQuerySet(AbstractQuerySet):
    """
    Overrides _get_result_constructor for querysets that do not define a model (e.g. NamedTable queries)
    """

    def _get_result_constructor(self):
        """
        Returns a function that will be used to instantiate query results
        """
        return ResultObject


class ModelQuerySet(AbstractQuerySet):
    """
    """
    def _validate_select_where(self):
        """ Checks that a filterset will not create invalid select statement """
        # check that there's either a =, a IN or a CONTAINS (collection)
        # relationship with a primary key or indexed field. We also allow
        # custom indexes to be queried with any operator (a difference
        # between a secondary index)
        equal_ops = [self.model._get_column_by_db_name(w.field) \
                     for w in self._where if not isinstance(w.value, Token)
                     and (isinstance(w.operator, EqualsOperator)
                          or self.model._get_column_by_db_name(w.field).custom_index)]
        token_comparison = any([w for w in self._where if isinstance(w.value, Token)])
        if not any(w.primary_key or w.has_index for w in equal_ops) and not token_comparison and not self._allow_filtering:
            raise QueryException(
                ('Where clauses require either  =, a IN or a CONTAINS '
                 '(collection) comparison with either a primary key or '
                 'indexed field. You might want to consider setting '
                 'custom_index on fields that you manage index outside '
                 'cqlengine.'))

        if not self._allow_filtering:
            # if the query is not on an indexed field
            if not any(w.has_index for w in equal_ops):
                if not any([w.partition_key for w in equal_ops]) and not token_comparison:
                    raise QueryException(
                        ('Filtering on a clustering key without a partition '
                         'key is not allowed unless allow_filtering() is '
                         'called on the queryset. You might want to consider '
                         'setting custom_index on fields that you manage '
                         'index outside cqlengine.'))

    def _select_fields(self):
        if self._defer_fields or self._only_fields:
            fields = [columns.db_field_name for columns in self.model._columns.values()]
            if self._defer_fields:
                fields = [f for f in fields if f not in self._defer_fields]
                # select the partition keys if all model fields are set defer
                if not fields:
                    fields = [columns.db_field_name for columns in self.model._partition_keys.values()]
            if self._only_fields:
                fields = [f for f in fields if f in self._only_fields]
            if not fields:
                raise QueryException('No fields in select query. Only fields: "{0}", defer fields: "{1}"'.format(
                    ','.join(self._only_fields), ','.join(self._defer_fields)))
            return fields
        return super(ModelQuerySet, self)._select_fields()

    def _get_result_constructor(self):
        """ Returns a function that will be used to instantiate query results """
        if not self._values_list:  # we want models
            return self.model._construct_instance
        elif self._flat_values_list:  # the user has requested flattened list (1 value per row)
            key = self._only_fields[0]
            return lambda row: row[key]
        else:
            return lambda row: [row[f] for f in self._only_fields]

    def _get_ordering_condition(self, colname):
        colname, order_type = super(ModelQuerySet, self)._get_ordering_condition(colname)

        column = self.model._columns.get(colname)
        if column is None:
            raise QueryException("Can't resolve the column name: '{0}'".format(colname))

        # validate the column selection
        if not column.primary_key:
            raise QueryException(
                "Can't order on '{0}', can only order on (clustered) primary keys".format(colname))

        pks = [v for k, v in self.model._columns.items() if v.primary_key]
        if column == pks[0]:
            raise QueryException(
                "Can't order by the first primary key (partition key), clustering (secondary) keys only")

        return column.db_field_name, order_type

    def values_list(self, *fields, **kwargs):
        """ Instructs the query set to return tuples, not model instance """
        flat = kwargs.pop('flat', False)
        if kwargs:
            raise TypeError('Unexpected keyword arguments to values_list: %s'
                            % (kwargs.keys(),))
        if flat and len(fields) > 1:
            raise TypeError("'flat' is not valid when values_list is called with more than one field.")
        clone = self.only(fields)
        clone._values_list = True
        clone._flat_values_list = flat
        return clone

    def ttl(self, ttl):
        """
        Sets the ttl (in seconds) for modified data.

        *Note that running a select query with a ttl value will raise an exception*
        """
        clone = copy.deepcopy(self)
        clone._ttl = ttl
        return clone

    def timestamp(self, timestamp):
        """
        Allows for custom timestamps to be saved with the record.
        """
        clone = copy.deepcopy(self)
        clone._timestamp = timestamp
        return clone

    def if_not_exists(self):
        """
        Check the existence of an object before insertion.

        If the insertion isn't applied, a LWTException is raised.
        """
        if self.model._has_counter:
            raise IfNotExistsWithCounterColumn('if_not_exists cannot be used with tables containing counter columns')
        clone = copy.deepcopy(self)
        clone._if_not_exists = True
        return clone

    def if_exists(self):
        """
        Check the existence of an object before an update or delete.

        If the update or delete isn't applied, a LWTException is raised.
        """
        if self.model._has_counter:
            raise IfExistsWithCounterColumn('if_exists cannot be used with tables containing counter columns')
        clone = copy.deepcopy(self)
        clone._if_exists = True
        return clone

    def update(self, **values):
        """
        Performs an update on the row selected by the queryset. Include values to update in the
        update like so:

        .. code-block:: python

            Model.objects(key=n).update(value='x')

        Passing in updates for columns which are not part of the model will raise a ValidationError.

        Per column validation will be performed, but instance level validation will not
        (i.e., `Model.validate` is not called).  This is sometimes referred to as a blind update.

        For example:

        .. code-block:: python

            class User(Model):
                id = Integer(primary_key=True)
                name = Text()

            setup(["localhost"], "test")
            sync_table(User)

            u = User.create(id=1, name="jon")

            User.objects(id=1).update(name="Steve")

            # sets name to null
            User.objects(id=1).update(name=None)


        Also supported is blindly adding and removing elements from container columns,
        without loading a model instance from Cassandra.

        Using the syntax `.update(column_name={x, y, z})` will overwrite the contents of the container, like updating a
        non container column. However, adding `__<operation>` to the end of the keyword arg, makes the update call add
        or remove items from the collection, without overwriting then entire column.

        Given the model below, here are the operations that can be performed on the different container columns:

        .. code-block:: python

            class Row(Model):
                row_id      = columns.Integer(primary_key=True)
                set_column  = columns.Set(Integer)
                list_column = columns.List(Integer)
                map_column  = columns.Map(Integer, Integer)

        :class:`~cqlengine.columns.Set`

        - `add`: adds the elements of the given set to the column
        - `remove`: removes the elements of the given set to the column


        .. code-block:: python

            # add elements to a set
            Row.objects(row_id=5).update(set_column__add={6})

            # remove elements to a set
            Row.objects(row_id=5).update(set_column__remove={4})

        :class:`~cqlengine.columns.List`

        - `append`: appends the elements of the given list to the end of the column
        - `prepend`: prepends the elements of the given list to the beginning of the column

        .. code-block:: python

            # append items to a list
            Row.objects(row_id=5).update(list_column__append=[6, 7])

            # prepend items to a list
            Row.objects(row_id=5).update(list_column__prepend=[1, 2])


        :class:`~cqlengine.columns.Map`

        - `update`: adds the given keys/values to the columns, creating new entries if they didn't exist, and overwriting old ones if they did

        .. code-block:: python

            # add items to a map
            Row.objects(row_id=5).update(map_column__update={1: 2, 3: 4})

            # remove items from a map
            Row.objects(row_id=5).update(map_column__remove={1, 2})
        """
        if not values:
            return

        nulled_columns = set()
        updated_columns = set()
        us = UpdateStatement(self.column_family_name, where=self._where, ttl=self._ttl,
                             timestamp=self._timestamp, conditionals=self._conditional, if_exists=self._if_exists)
        for name, val in values.items():
            col_name, col_op = self._parse_filter_arg(name)
            col = self.model._columns.get(col_name)
            # check for nonexistant columns
            if col is None:
                raise ValidationError("{0}.{1} has no column named: {2}".format(self.__module__, self.model.__name__, col_name))
            # check for primary key update attempts
            if col.is_primary_key:
                raise ValidationError("Cannot apply update to primary key '{0}' for {1}.{2}".format(col_name, self.__module__, self.model.__name__))

            if col_op == 'remove' and isinstance(col, columns.Map):
                if not isinstance(val, set):
                    raise ValidationError(
                        "Cannot apply update operation '{0}' on column '{1}' with value '{2}'. A set is required.".format(col_op, col_name, val))
                val = {v: None for v in val}
            else:
                # we should not provide default values in this use case.
                val = col.validate(val)

            if val is None:
                nulled_columns.add(col_name)
                continue

            us.add_update(col, val, operation=col_op)
            updated_columns.add(col_name)

        if us.assignments:
            self._execute(us)

        if nulled_columns:
            delete_conditional = [condition for condition in self._conditional
                                  if condition.field not in updated_columns] if self._conditional else None
            ds = DeleteStatement(self.column_family_name, fields=nulled_columns,
                                 where=self._where, conditionals=delete_conditional, if_exists=self._if_exists)
            self._execute(ds)


class DMLQuery(object):
    """
    A query object used for queries performing inserts, updates, or deletes

    this is usually instantiated by the model instance to be modified

    unlike the read query object, this is mutable
    """
    _ttl = None
    _consistency = None
    _timestamp = None
    _if_not_exists = False
    _if_exists = False

    def __init__(self, model, instance=None, batch=None, ttl=None, consistency=None, timestamp=None,
                 if_not_exists=False, conditional=None, timeout=conn.NOT_SET, if_exists=False):
        self.model = model
        self.column_family_name = self.model.column_family_name()
        self.instance = instance
        self._batch = batch
        self._ttl = ttl
        self._consistency = consistency
        self._timestamp = timestamp
        self._if_not_exists = if_not_exists
        self._if_exists = if_exists
        self._conditional = conditional
        self._timeout = timeout

    def _execute(self, statement):
        connection = self.instance._get_connection() if self.instance else self.model._get_connection()
        if self._batch:
            if self._batch._connection:
                if not self._batch._connection_explicit and connection and \
                        connection != self._batch._connection:
                            raise CQLEngineException('BatchQuery queries must be executed on the same connection')
            else:
                # set the BatchQuery connection from the model
                self._batch._connection = connection
            return self._batch.add_query(statement)
        else:
            results = _execute_statement(self.model, statement, self._consistency, self._timeout, connection=connection)
            if self._if_not_exists or self._if_exists or self._conditional:
                check_applied(results)
            return results

    def batch(self, batch_obj):
        if batch_obj is not None and not isinstance(batch_obj, BatchQuery):
            raise CQLEngineException('batch_obj must be a BatchQuery instance or None')
        self._batch = batch_obj
        return self

    def _delete_null_columns(self, conditionals=None):
        """
        executes a delete query to remove columns that have changed to null
        """
        ds = DeleteStatement(self.column_family_name, conditionals=conditionals, if_exists=self._if_exists)
        deleted_fields = False
        static_only = True
        for _, v in self.instance._values.items():
            col = v.column
            if v.deleted:
                ds.add_field(col.db_field_name)
                deleted_fields = True
                static_only &= col.static
            elif isinstance(col, columns.Map):
                uc = MapDeleteClause(col.db_field_name, v.value, v.previous_value)
                if uc.get_context_size() > 0:
                    ds.add_field(uc)
                    deleted_fields = True
                    static_only |= col.static

        if deleted_fields:
            keys = self.model._partition_keys if static_only else self.model._primary_keys
            for name, col in keys.items():
                ds.add_where(col, EqualsOperator(), getattr(self.instance, name))
            self._execute(ds)

    def update(self):
        """
        updates a row.
        This is a blind update call.
        All validation and cleaning needs to happen
        prior to calling this.
        """
        if self.instance is None:
            raise CQLEngineException("DML Query instance attribute is None")
        assert type(self.instance) == self.model
        null_clustering_key = False if len(self.instance._clustering_keys) == 0 else True
        static_changed_only = True
        statement = UpdateStatement(self.column_family_name, ttl=self._ttl, timestamp=self._timestamp,
                                    conditionals=self._conditional, if_exists=self._if_exists)
        for name, col in self.instance._clustering_keys.items():
            null_clustering_key = null_clustering_key and col._val_is_null(getattr(self.instance, name, None))

        updated_columns = set()
        # get defined fields and their column names
        for name, col in self.model._columns.items():
            # if clustering key is null, don't include non-static columns
            if null_clustering_key and not col.static and not col.partition_key:
                continue
            if not col.is_primary_key:
                val = getattr(self.instance, name, None)
                val_mgr = self.instance._values[name]

                if val is None:
                    continue

                if not val_mgr.changed and not isinstance(col, columns.Counter):
                    continue

                static_changed_only = static_changed_only and col.static
                statement.add_update(col, val, previous=val_mgr.previous_value)
                updated_columns.add(col.db_field_name)

        if statement.assignments:
            for name, col in self.model._primary_keys.items():
                # only include clustering key if clustering key is not null, and non-static columns are changed to avoid cql error
                if (null_clustering_key or static_changed_only) and (not col.partition_key):
                    continue
                statement.add_where(col, EqualsOperator(), getattr(self.instance, name))
            self._execute(statement)

        if not null_clustering_key:
            # remove conditions on fields that have been updated
            delete_conditionals = [condition for condition in self._conditional
                                   if condition.field not in updated_columns] if self._conditional else None
            self._delete_null_columns(delete_conditionals)

    def save(self):
        """
        Creates / updates a row.
        This is a blind insert call.
        All validation and cleaning needs to happen
        prior to calling this.
        """
        if self.instance is None:
            raise CQLEngineException("DML Query instance attribute is None")
        assert type(self.instance) == self.model

        nulled_fields = set()
        if self.instance._has_counter or self.instance._can_update():
            if self.instance._has_counter:
                warn("'create' and 'save' actions on Counters are deprecated. It will be disallowed in 4.0. "
                    "Use the 'update' mechanism instead.", DeprecationWarning)
            return self.update()
        else:
            insert = InsertStatement(self.column_family_name, ttl=self._ttl, timestamp=self._timestamp, if_not_exists=self._if_not_exists)
            static_save_only = False if len(self.instance._clustering_keys) == 0 else True
            for name, col in self.instance._clustering_keys.items():
                static_save_only = static_save_only and col._val_is_null(getattr(self.instance, name, None))
            for name, col in self.instance._columns.items():
                if static_save_only and not col.static and not col.partition_key:
                    continue
                val = getattr(self.instance, name, None)
                if col._val_is_null(val):
                    if self.instance._values[name].changed:
                        nulled_fields.add(col.db_field_name)
                    continue
                if col.has_default and not self.instance._values[name].changed:
                    # Ensure default columns included in a save() are marked as explicit, to get them *persisted* properly
                    self.instance._values[name].explicit = True
                insert.add_assignment(col, getattr(self.instance, name, None))

        # skip query execution if it's empty
        # caused by pointless update queries
        if not insert.is_empty:
            self._execute(insert)
        # delete any nulled columns
        if not static_save_only:
            self._delete_null_columns()

    def delete(self):
        """ Deletes one instance """
        if self.instance is None:
            raise CQLEngineException("DML Query instance attribute is None")

        ds = DeleteStatement(self.column_family_name, timestamp=self._timestamp, conditionals=self._conditional, if_exists=self._if_exists)
        for name, col in self.model._primary_keys.items():
            val = getattr(self.instance, name)
            if val is None and not col.partition_key:
                continue
            ds.add_where(col, EqualsOperator(), val)
        self._execute(ds)


def _execute_statement(model, statement, consistency_level, timeout, connection=None):
    params = statement.get_context()
    s = SimpleStatement(str(statement), consistency_level=consistency_level, fetch_size=statement.fetch_size)
    if model._partition_key_index:
        key_values = statement.partition_key_values(model._partition_key_index)
        if not any(v is None for v in key_values):
            parts = model._routing_key_from_values(key_values, conn.get_cluster(connection).protocol_version)
            s.routing_key = parts
            s.keyspace = model._get_keyspace()
    connection = connection or model._get_connection()
    return conn.execute(s, params, timeout=timeout, connection=connection)
