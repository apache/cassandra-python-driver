#!/usr/bin/env python

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

# silence warnings just for demo -- applications would typically not do this
import os
os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = '1'

import logging

log = logging.getLogger()
log.setLevel('INFO')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from uuid import uuid4

from cassandra.cqlengine import columns
from cassandra.cqlengine import connection
from cassandra.cqlengine import management
from cassandra.cqlengine import ValidationError
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.query import BatchQuery, LWTException

KEYSPACE = "testkeyspace"


class FamilyMembers(Model):
    __keyspace__ = KEYSPACE
    id = columns.UUID(primary_key=True, default=uuid4)
    surname = columns.Text(primary_key=True)
    name = columns.Text(primary_key=True)
    birth_year = columns.Integer()
    sex = columns.Text(min_length=1, max_length=1)

    def validate(self):
        super(FamilyMembers, self).validate()
        if self.sex and self.sex not in 'mf':
            raise ValidationError("FamilyMember.sex must be one of ['m', 'f']")

        if self.birth_year and self.sex == 'f':
            raise ValidationError("FamilyMember.birth_year is set, and 'a lady never tells'")


def main():
    connection.default()

    # Management functions would normally be used in development, and possibly for deployments.
    # They are typically not part of a core application.
    log.info("### creating keyspace...")
    management.create_keyspace_simple(KEYSPACE, 1)
    log.info("### syncing model...")
    management.sync_table(FamilyMembers)

    # default uuid is assigned
    simmons = FamilyMembers.create(surname='Simmons', name='Gene', birth_year=1949, sex='m')

    # add members to his family later
    FamilyMembers.create(id=simmons.id, surname='Simmons', name='Nick', birth_year=1989, sex='m')
    sophie = FamilyMembers.create(id=simmons.id, surname='Simmons', name='Sophie', sex='f')

    nick = FamilyMembers.objects(id=simmons.id, surname='Simmons', name='Nick')
    try:
        nick.iff(birth_year=1988).update(birth_year=1989)
    except LWTException:
        print("precondition not met")

    log.info("### setting individual column to NULL by updating it to None")
    nick.update(birth_year=None)

    # showing validation
    try:
        FamilyMembers.create(id=simmons.id, surname='Tweed', name='Shannon', birth_year=1957, sex='f')
    except ValidationError:
        log.exception('INTENTIONAL VALIDATION EXCEPTION; Failed creating instance:')
        FamilyMembers.create(id=simmons.id, surname='Tweed', name='Shannon', sex='f')

    log.info("### add multiple as part of a batch")
    # If creating many at one time, can use a batch to minimize round-trips
    hogan_id = uuid4()
    with BatchQuery() as b:
        FamilyMembers.batch(b).create(id=hogan_id, surname='Hogan', name='Hulk', sex='m')
        FamilyMembers.batch(b).create(id=hogan_id, surname='Hogan', name='Linda', sex='f')
        FamilyMembers.batch(b).create(id=hogan_id, surname='Hogan', name='Nick', sex='m')
        FamilyMembers.batch(b).create(id=hogan_id, surname='Hogan', name='Brooke', sex='f')

    log.info("### All members")
    for m in FamilyMembers.all():
        print(m, m.birth_year, m.sex)

    log.info("### Select by partition key")
    for m in FamilyMembers.objects(id=simmons.id):
        print(m, m.birth_year, m.sex)

    log.info("### Constrain on clustering key")
    for m in FamilyMembers.objects(id=simmons.id, surname=simmons.surname):
        print(m, m.birth_year, m.sex)

    log.info("### Constrain on clustering key")
    kids = FamilyMembers.objects(id=simmons.id, surname=simmons.surname, name__in=['Nick', 'Sophie'])

    log.info("### Delete a record")
    FamilyMembers(id=hogan_id, surname='Hogan', name='Linda').delete()
    for m in FamilyMembers.objects(id=hogan_id):
        print(m, m.birth_year, m.sex)

    management.drop_keyspace(KEYSPACE)

if __name__ == "__main__":
    main()
