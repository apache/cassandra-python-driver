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

from datetime import datetime, timedelta
import time

from cassandra.query import FETCH_SIZE_UNSET
from cassandra.cqlengine import columns
from cassandra.cqlengine import UnicodeMixin
from cassandra.cqlengine.functions import QueryValue
from cassandra.cqlengine.operators import BaseWhereOperator, InOperator, EqualsOperator, IsNotNullOperator


class StatementException(Exception):
    pass


class ValueQuoter(UnicodeMixin):

    def __init__(self, value):
        self.value = value

    def __unicode__(self):
        from cassandra.encoder import cql_quote
        if isinstance(self.value, (list, tuple)):
            return '[' + ', '.join([cql_quote(v) for v in self.value]) + ']'
        elif isinstance(self.value, dict):
            return '{' + ', '.join([cql_quote(k) + ':' + cql_quote(v) for k, v in self.value.items()]) + '}'
        elif isinstance(self.value, set):
            return '{' + ', '.join([cql_quote(v) for v in self.value]) + '}'
        return cql_quote(self.value)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.value == other.value
        return False


class InQuoter(ValueQuoter):

    def __unicode__(self):
        from cassandra.encoder import cql_quote
        return '(' + ', '.join([cql_quote(v) for v in self.value]) + ')'


class BaseClause(UnicodeMixin):

    def __init__(self, field, value):
        self.field = field
        self.value = value
        self.context_id = None

    def __unicode__(self):
        raise NotImplementedError

    def __hash__(self):
        return hash(self.field) ^ hash(self.value)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.field == other.field and self.value == other.value
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def get_context_size(self):
        """ returns the number of entries this clause will add to the query context """
        return 1

    def set_context_id(self, i):
        """ sets the value placeholder that will be used in the query """
        self.context_id = i

    def update_context(self, ctx):
        """ updates the query context with this clauses values """
        assert isinstance(ctx, dict)
        ctx[str(self.context_id)] = self.value


class WhereClause(BaseClause):
    """ a single where statement used in queries """

    def __init__(self, field, operator, value, quote_field=True):
        """

        :param field:
        :param operator:
        :param value:
        :param quote_field: hack to get the token function rendering properly
        :return:
        """
        if not isinstance(operator, BaseWhereOperator):
            raise StatementException(
                "operator must be of type {0}, got {1}".format(BaseWhereOperator, type(operator))
            )
        super(WhereClause, self).__init__(field, value)
        self.operator = operator
        self.query_value = self.value if isinstance(self.value, QueryValue) else QueryValue(self.value)
        self.quote_field = quote_field

    def __unicode__(self):
        field = ('"{0}"' if self.quote_field else '{0}').format(self.field)
        return u'{0} {1} {2}'.format(field, self.operator, str(self.query_value))

    def __hash__(self):
        return super(WhereClause, self).__hash__() ^ hash(self.operator)

    def __eq__(self, other):
        if super(WhereClause, self).__eq__(other):
            return self.operator.__class__ == other.operator.__class__
        return False

    def get_context_size(self):
        return self.query_value.get_context_size()

    def set_context_id(self, i):
        super(WhereClause, self).set_context_id(i)
        self.query_value.set_context_id(i)

    def update_context(self, ctx):
        if isinstance(self.operator, InOperator):
            ctx[str(self.context_id)] = InQuoter(self.value)
        else:
            self.query_value.update_context(ctx)


class IsNotNullClause(WhereClause):
    def __init__(self, field):
        super(IsNotNullClause, self).__init__(field, IsNotNullOperator(), '')

    def __unicode__(self):
        field = ('"{0}"' if self.quote_field else '{0}').format(self.field)
        return u'{0} {1}'.format(field, self.operator)

    def update_context(self, ctx):
        pass

    def get_context_size(self):
        return 0

# alias for convenience
IsNotNull = IsNotNullClause


class AssignmentClause(BaseClause):
    """ a single variable st statement """

    def __unicode__(self):
        return u'"{0}" = %({1})s'.format(self.field, self.context_id)

    def insert_tuple(self):
        return self.field, self.context_id


class ConditionalClause(BaseClause):
    """ A single variable iff statement """

    def __unicode__(self):
        return u'"{0}" = %({1})s'.format(self.field, self.context_id)

    def insert_tuple(self):
        return self.field, self.context_id


class ContainerUpdateTypeMapMeta(type):

    def __init__(cls, name, bases, dct):
        if not hasattr(cls, 'type_map'):
            cls.type_map = {}
        else:
            cls.type_map[cls.col_type] = cls
        super(ContainerUpdateTypeMapMeta, cls).__init__(name, bases, dct)


class ContainerUpdateClause(AssignmentClause, metaclass=ContainerUpdateTypeMapMeta):

    def __init__(self, field, value, operation=None, previous=None):
        super(ContainerUpdateClause, self).__init__(field, value)
        self.previous = previous
        self._assignments = None
        self._operation = operation
        self._analyzed = False

    def _analyze(self):
        raise NotImplementedError

    def get_context_size(self):
        raise NotImplementedError

    def update_context(self, ctx):
        raise NotImplementedError


class SetUpdateClause(ContainerUpdateClause):
    """ updates a set collection """

    col_type = columns.Set

    _additions = None
    _removals = None

    def __unicode__(self):
        qs = []
        ctx_id = self.context_id
        if (self.previous is None and
                self._assignments is None and
                self._additions is None and
                self._removals is None):
            qs += ['"{0}" = %({1})s'.format(self.field, ctx_id)]
        if self._assignments is not None:
            qs += ['"{0}" = %({1})s'.format(self.field, ctx_id)]
            ctx_id += 1
        if self._additions is not None:
            qs += ['"{0}" = "{0}" + %({1})s'.format(self.field, ctx_id)]
            ctx_id += 1
        if self._removals is not None:
            qs += ['"{0}" = "{0}" - %({1})s'.format(self.field, ctx_id)]

        return ', '.join(qs)

    def _analyze(self):
        """ works out the updates to be performed """
        if self.value is None or self.value == self.previous:
            pass
        elif self._operation == "add":
            self._additions = self.value
        elif self._operation == "remove":
            self._removals = self.value
        elif self.previous is None:
            self._assignments = self.value
        else:
            # partial update time
            self._additions = (self.value - self.previous) or None
            self._removals = (self.previous - self.value) or None
        self._analyzed = True

    def get_context_size(self):
        if not self._analyzed:
            self._analyze()
        if (self.previous is None and
                not self._assignments and
                self._additions is None and
                self._removals is None):
            return 1
        return int(bool(self._assignments)) + int(bool(self._additions)) + int(bool(self._removals))

    def update_context(self, ctx):
        if not self._analyzed:
            self._analyze()
        ctx_id = self.context_id
        if (self.previous is None and
                self._assignments is None and
                self._additions is None and
                self._removals is None):
            ctx[str(ctx_id)] = set()
        if self._assignments is not None:
            ctx[str(ctx_id)] = self._assignments
            ctx_id += 1
        if self._additions is not None:
            ctx[str(ctx_id)] = self._additions
            ctx_id += 1
        if self._removals is not None:
            ctx[str(ctx_id)] = self._removals


class ListUpdateClause(ContainerUpdateClause):
    """ updates a list collection """

    col_type = columns.List

    _append = None
    _prepend = None

    def __unicode__(self):
        if not self._analyzed:
            self._analyze()
        qs = []
        ctx_id = self.context_id
        if self._assignments is not None:
            qs += ['"{0}" = %({1})s'.format(self.field, ctx_id)]
            ctx_id += 1

        if self._prepend is not None:
            qs += ['"{0}" = %({1})s + "{0}"'.format(self.field, ctx_id)]
            ctx_id += 1

        if self._append is not None:
            qs += ['"{0}" = "{0}" + %({1})s'.format(self.field, ctx_id)]

        return ', '.join(qs)

    def get_context_size(self):
        if not self._analyzed:
            self._analyze()
        return int(self._assignments is not None) + int(bool(self._append)) + int(bool(self._prepend))

    def update_context(self, ctx):
        if not self._analyzed:
            self._analyze()
        ctx_id = self.context_id
        if self._assignments is not None:
            ctx[str(ctx_id)] = self._assignments
            ctx_id += 1
        if self._prepend is not None:
            ctx[str(ctx_id)] = self._prepend
            ctx_id += 1
        if self._append is not None:
            ctx[str(ctx_id)] = self._append

    def _analyze(self):
        """ works out the updates to be performed """
        if self.value is None or self.value == self.previous:
            pass

        elif self._operation == "append":
            self._append = self.value

        elif self._operation == "prepend":
            self._prepend = self.value

        elif self.previous is None:
            self._assignments = self.value

        elif len(self.value) < len(self.previous):
            # if elements have been removed,
            # rewrite the whole list
            self._assignments = self.value

        elif len(self.previous) == 0:
            # if we're updating from an empty
            # list, do a complete insert
            self._assignments = self.value
        else:

            # the max start idx we want to compare
            search_space = len(self.value) - max(0, len(self.previous) - 1)

            # the size of the sub lists we want to look at
            search_size = len(self.previous)

            for i in range(search_space):
                # slice boundary
                j = i + search_size
                sub = self.value[i:j]
                idx_cmp = lambda idx: self.previous[idx] == sub[idx]
                if idx_cmp(0) and idx_cmp(-1) and self.previous == sub:
                    self._prepend = self.value[:i] or None
                    self._append = self.value[j:] or None
                    break

            # if both append and prepend are still None after looking
            # at both lists, an insert statement will be created
            if self._prepend is self._append is None:
                self._assignments = self.value

        self._analyzed = True


class MapUpdateClause(ContainerUpdateClause):
    """ updates a map collection """

    col_type = columns.Map

    _updates = None
    _removals = None

    def _analyze(self):
        if self._operation == "update":
            self._updates = self.value.keys()
        elif self._operation == "remove":
            self._removals = {v for v in self.value.keys()}
        else:
            if self.previous is None:
                self._updates = sorted([k for k, v in self.value.items()])
            else:
                self._updates = sorted([k for k, v in self.value.items() if v != self.previous.get(k)]) or None
        self._analyzed = True

    def get_context_size(self):
        if self.is_assignment:
            return 1
        return int((len(self._updates or []) * 2) + int(bool(self._removals)))

    def update_context(self, ctx):
        ctx_id = self.context_id
        if self.is_assignment:
            ctx[str(ctx_id)] = {}
        elif self._removals is not None:
            ctx[str(ctx_id)] = self._removals
        else:
            for key in self._updates or []:
                val = self.value.get(key)
                ctx[str(ctx_id)] = key
                ctx[str(ctx_id + 1)] = val
                ctx_id += 2

    @property
    def is_assignment(self):
        if not self._analyzed:
            self._analyze()
        return self.previous is None and not self._updates and not self._removals

    def __unicode__(self):
        qs = []

        ctx_id = self.context_id
        if self.is_assignment:
            qs += ['"{0}" = %({1})s'.format(self.field, ctx_id)]
        elif self._removals is not None:
            qs += ['"{0}" = "{0}" - %({1})s'.format(self.field, ctx_id)]
            ctx_id += 1
        else:
            for _ in self._updates or []:
                qs += ['"{0}"[%({1})s] = %({2})s'.format(self.field, ctx_id, ctx_id + 1)]
                ctx_id += 2

        return ', '.join(qs)


class CounterUpdateClause(AssignmentClause):

    col_type = columns.Counter

    def __init__(self, field, value, previous=None):
        super(CounterUpdateClause, self).__init__(field, value)
        self.previous = previous or 0

    def get_context_size(self):
        return 1

    def update_context(self, ctx):
        ctx[str(self.context_id)] = abs(self.value - self.previous)

    def __unicode__(self):
        delta = self.value - self.previous
        sign = '-' if delta < 0 else '+'
        return '"{0}" = "{0}" {1} %({2})s'.format(self.field, sign, self.context_id)


class BaseDeleteClause(BaseClause):
    pass


class FieldDeleteClause(BaseDeleteClause):
    """ deletes a field from a row """

    def __init__(self, field):
        super(FieldDeleteClause, self).__init__(field, None)

    def __unicode__(self):
        return '"{0}"'.format(self.field)

    def update_context(self, ctx):
        pass

    def get_context_size(self):
        return 0


class MapDeleteClause(BaseDeleteClause):
    """ removes keys from a map """

    def __init__(self, field, value, previous=None):
        super(MapDeleteClause, self).__init__(field, value)
        self.value = self.value or {}
        self.previous = previous or {}
        self._analyzed = False
        self._removals = None

    def _analyze(self):
        self._removals = sorted([k for k in self.previous if k not in self.value])
        self._analyzed = True

    def update_context(self, ctx):
        if not self._analyzed:
            self._analyze()
        for idx, key in enumerate(self._removals):
            ctx[str(self.context_id + idx)] = key

    def get_context_size(self):
        if not self._analyzed:
            self._analyze()
        return len(self._removals)

    def __unicode__(self):
        if not self._analyzed:
            self._analyze()
        return ', '.join(['"{0}"[%({1})s]'.format(self.field, self.context_id + i) for i in range(len(self._removals))])


class BaseCQLStatement(UnicodeMixin):
    """ The base cql statement class """

    def __init__(self, table, timestamp=None, where=None, fetch_size=None, conditionals=None):
        super(BaseCQLStatement, self).__init__()
        self.table = table
        self.context_id = 0
        self.context_counter = self.context_id
        self.timestamp = timestamp
        self.fetch_size = fetch_size if fetch_size else FETCH_SIZE_UNSET

        self.where_clauses = []
        for clause in where or []:
            self._add_where_clause(clause)

        self.conditionals = []
        for conditional in conditionals or []:
            self.add_conditional_clause(conditional)

    def _update_part_key_values(self, field_index_map, clauses, parts):
        for clause in filter(lambda c: c.field in field_index_map, clauses):
            parts[field_index_map[clause.field]] = clause.value

    def partition_key_values(self, field_index_map):
        parts = [None] * len(field_index_map)
        self._update_part_key_values(field_index_map, (w for w in self.where_clauses if w.operator.__class__ == EqualsOperator), parts)
        return parts

    def add_where(self, column, operator, value, quote_field=True):
        value = column.to_database(value)
        clause = WhereClause(column.db_field_name, operator, value, quote_field)
        self._add_where_clause(clause)

    def _add_where_clause(self, clause):
        clause.set_context_id(self.context_counter)
        self.context_counter += clause.get_context_size()
        self.where_clauses.append(clause)

    def get_context(self):
        """
        returns the context dict for this statement
        :rtype: dict
        """
        ctx = {}
        for clause in self.where_clauses or []:
            clause.update_context(ctx)
        return ctx

    def add_conditional_clause(self, clause):
        """
        Adds an iff clause to this statement

        :param clause: The clause that will be added to the iff statement
        :type clause: ConditionalClause
        """
        clause.set_context_id(self.context_counter)
        self.context_counter += clause.get_context_size()
        self.conditionals.append(clause)

    def _get_conditionals(self):
        return 'IF {0}'.format(' AND '.join([str(c) for c in self.conditionals]))

    def get_context_size(self):
        return len(self.get_context())

    def update_context_id(self, i):
        self.context_id = i
        self.context_counter = self.context_id
        for clause in self.where_clauses:
            clause.set_context_id(self.context_counter)
            self.context_counter += clause.get_context_size()

    @property
    def timestamp_normalized(self):
        """
        We're expecting self.timestamp to be either a long, int, a datetime, or a timedelta
        :return:
        """
        if not self.timestamp:
            return None

        if isinstance(self.timestamp, int):
            return self.timestamp

        if isinstance(self.timestamp, timedelta):
            tmp = datetime.now() + self.timestamp
        else:
            tmp = self.timestamp

        return int(time.mktime(tmp.timetuple()) * 1e+6 + tmp.microsecond)

    def __unicode__(self):
        raise NotImplementedError

    def __repr__(self):
        return self.__unicode__()

    @property
    def _where(self):
        return 'WHERE {0}'.format(' AND '.join([str(c) for c in self.where_clauses]))


class SelectStatement(BaseCQLStatement):
    """ a cql select statement """

    def __init__(self,
                 table,
                 fields=None,
                 count=False,
                 where=None,
                 order_by=None,
                 limit=None,
                 allow_filtering=False,
                 distinct_fields=None,
                 fetch_size=None):

        """
        :param where
        :type where list of cqlengine.statements.WhereClause
        """
        super(SelectStatement, self).__init__(
            table,
            where=where,
            fetch_size=fetch_size
        )

        self.fields = [fields] if isinstance(fields, str) else (fields or [])
        self.distinct_fields = distinct_fields
        self.count = count
        self.order_by = [order_by] if isinstance(order_by, str) else order_by
        self.limit = limit
        self.allow_filtering = allow_filtering

    def __unicode__(self):
        qs = ['SELECT']
        if self.distinct_fields:
            if self.count:
                qs += ['DISTINCT COUNT({0})'.format(', '.join(['"{0}"'.format(f) for f in self.distinct_fields]))]
            else:
                qs += ['DISTINCT {0}'.format(', '.join(['"{0}"'.format(f) for f in self.distinct_fields]))]
        elif self.count:
            qs += ['COUNT(*)']
        else:
            qs += [', '.join(['"{0}"'.format(f) for f in self.fields]) if self.fields else '*']
        qs += ['FROM', self.table]

        if self.where_clauses:
            qs += [self._where]

        if self.order_by and not self.count:
            qs += ['ORDER BY {0}'.format(', '.join(str(o) for o in self.order_by))]

        if self.limit:
            qs += ['LIMIT {0}'.format(self.limit)]

        if self.allow_filtering:
            qs += ['ALLOW FILTERING']

        return ' '.join(qs)


class AssignmentStatement(BaseCQLStatement):
    """ value assignment statements """

    def __init__(self,
                 table,
                 assignments=None,
                 where=None,
                 ttl=None,
                 timestamp=None,
                 conditionals=None):
        super(AssignmentStatement, self).__init__(
            table,
            where=where,
            conditionals=conditionals
        )
        self.ttl = ttl
        self.timestamp = timestamp

        # add assignments
        self.assignments = []
        for assignment in assignments or []:
            self._add_assignment_clause(assignment)

    def update_context_id(self, i):
        super(AssignmentStatement, self).update_context_id(i)
        for assignment in self.assignments:
            assignment.set_context_id(self.context_counter)
            self.context_counter += assignment.get_context_size()

    def partition_key_values(self, field_index_map):
        parts = super(AssignmentStatement, self).partition_key_values(field_index_map)
        self._update_part_key_values(field_index_map, self.assignments, parts)
        return parts

    def add_assignment(self, column, value):
        value = column.to_database(value)
        clause = AssignmentClause(column.db_field_name, value)
        self._add_assignment_clause(clause)

    def _add_assignment_clause(self, clause):
        clause.set_context_id(self.context_counter)
        self.context_counter += clause.get_context_size()
        self.assignments.append(clause)

    @property
    def is_empty(self):
        return len(self.assignments) == 0

    def get_context(self):
        ctx = super(AssignmentStatement, self).get_context()
        for clause in self.assignments:
            clause.update_context(ctx)
        return ctx


class InsertStatement(AssignmentStatement):
    """ an cql insert statement """

    def __init__(self,
                 table,
                 assignments=None,
                 where=None,
                 ttl=None,
                 timestamp=None,
                 if_not_exists=False):
        super(InsertStatement, self).__init__(table,
                                              assignments=assignments,
                                              where=where,
                                              ttl=ttl,
                                              timestamp=timestamp)

        self.if_not_exists = if_not_exists

    def __unicode__(self):
        qs = ['INSERT INTO {0}'.format(self.table)]

        # get column names and context placeholders
        fields = [a.insert_tuple() for a in self.assignments]
        columns, values = zip(*fields)

        qs += ["({0})".format(', '.join(['"{0}"'.format(c) for c in columns]))]
        qs += ['VALUES']
        qs += ["({0})".format(', '.join(['%({0})s'.format(v) for v in values]))]

        if self.if_not_exists:
            qs += ["IF NOT EXISTS"]

        using_options = []
        if self.ttl:
            using_options += ["TTL {}".format(self.ttl)]

        if self.timestamp:
            using_options += ["TIMESTAMP {}".format(self.timestamp_normalized)]

        if using_options:
            qs += ["USING {}".format(" AND ".join(using_options))]
        return ' '.join(qs)


class UpdateStatement(AssignmentStatement):
    """ an cql update select statement """

    def __init__(self,
                 table,
                 assignments=None,
                 where=None,
                 ttl=None,
                 timestamp=None,
                 conditionals=None,
                 if_exists=False):
        super(UpdateStatement, self). __init__(table,
                                               assignments=assignments,
                                               where=where,
                                               ttl=ttl,
                                               timestamp=timestamp,
                                               conditionals=conditionals)

        self.if_exists = if_exists

    def __unicode__(self):
        qs = ['UPDATE', self.table]

        using_options = []

        if self.ttl:
            using_options += ["TTL {0}".format(self.ttl)]

        if self.timestamp:
            using_options += ["TIMESTAMP {0}".format(self.timestamp_normalized)]

        if using_options:
            qs += ["USING {0}".format(" AND ".join(using_options))]

        qs += ['SET']
        qs += [', '.join([str(c) for c in self.assignments])]

        if self.where_clauses:
            qs += [self._where]

        if len(self.conditionals) > 0:
            qs += [self._get_conditionals()]

        if self.if_exists:
            qs += ["IF EXISTS"]

        return ' '.join(qs)

    def get_context(self):
        ctx = super(UpdateStatement, self).get_context()
        for clause in self.conditionals:
            clause.update_context(ctx)
        return ctx

    def update_context_id(self, i):
        super(UpdateStatement, self).update_context_id(i)
        for conditional in self.conditionals:
            conditional.set_context_id(self.context_counter)
            self.context_counter += conditional.get_context_size()

    def add_update(self, column, value, operation=None, previous=None):
        value = column.to_database(value)
        col_type = type(column)
        container_update_type = ContainerUpdateClause.type_map.get(col_type)
        if container_update_type:
            previous = column.to_database(previous)
            clause = container_update_type(column.db_field_name, value, operation, previous)
        elif col_type == columns.Counter:
            clause = CounterUpdateClause(column.db_field_name, value, previous)
        else:
            clause = AssignmentClause(column.db_field_name, value)
        if clause.get_context_size():  # this is to exclude map removals from updates. Can go away if we drop support for C* < 1.2.4 and remove two-phase updates
            self._add_assignment_clause(clause)


class DeleteStatement(BaseCQLStatement):
    """ a cql delete statement """

    def __init__(self, table, fields=None, where=None, timestamp=None, conditionals=None, if_exists=False):
        super(DeleteStatement, self).__init__(
            table,
            where=where,
            timestamp=timestamp,
            conditionals=conditionals
        )
        self.fields = []
        if isinstance(fields, str):
            fields = [fields]
        for field in fields or []:
            self.add_field(field)

        self.if_exists = if_exists

    def update_context_id(self, i):
        super(DeleteStatement, self).update_context_id(i)
        for field in self.fields:
            field.set_context_id(self.context_counter)
            self.context_counter += field.get_context_size()
        for t in self.conditionals:
            t.set_context_id(self.context_counter)
            self.context_counter += t.get_context_size()

    def get_context(self):
        ctx = super(DeleteStatement, self).get_context()
        for field in self.fields:
            field.update_context(ctx)
        for clause in self.conditionals:
            clause.update_context(ctx)
        return ctx

    def add_field(self, field):
        if isinstance(field, str):
            field = FieldDeleteClause(field)
        if not isinstance(field, BaseClause):
            raise StatementException("only instances of AssignmentClause can be added to statements")
        field.set_context_id(self.context_counter)
        self.context_counter += field.get_context_size()
        self.fields.append(field)

    def __unicode__(self):
        qs = ['DELETE']
        if self.fields:
            qs += [', '.join(['{0}'.format(f) for f in self.fields])]
        qs += ['FROM', self.table]

        delete_option = []

        if self.timestamp:
            delete_option += ["TIMESTAMP {0}".format(self.timestamp_normalized)]

        if delete_option:
            qs += [" USING {0} ".format(" AND ".join(delete_option))]

        if self.where_clauses:
            qs += [self._where]

        if self.conditionals:
            qs += [self._get_conditionals()]

        if self.if_exists:
            qs += ["IF EXISTS"]

        return ' '.join(qs)
