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
from unittest.mock import patch

from cassandra.cqlengine import columns
from cassandra.cqlengine.management import drop_table, sync_table, _get_table_metadata, _update_options
from cassandra.cqlengine.models import Model

from tests.integration.cqlengine.base import BaseCassEngTestCase


class LeveledCompactionTestTable(Model):

    __options__ = {'compaction': {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy',
                                  'sstable_size_in_mb': '64'}}

    user_id = columns.UUID(primary_key=True)
    name = columns.Text()


class AlterTableTest(BaseCassEngTestCase):

    def test_alter_is_called_table(self):
        drop_table(LeveledCompactionTestTable)
        sync_table(LeveledCompactionTestTable)
        with patch('cassandra.cqlengine.management._update_options') as mock:
            sync_table(LeveledCompactionTestTable)
        assert mock.called == 1

    def test_compaction_not_altered_without_changes_leveled(self):

        class LeveledCompactionChangesDetectionTest(Model):

            __options__ = {'compaction': {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy',
                                          'sstable_size_in_mb': '160',
                                          'tombstone_threshold': '0.125',
                                          'tombstone_compaction_interval': '3600'}}
            pk = columns.Integer(primary_key=True)

        drop_table(LeveledCompactionChangesDetectionTest)
        sync_table(LeveledCompactionChangesDetectionTest)

        self.assertFalse(_update_options(LeveledCompactionChangesDetectionTest))

    def test_compaction_not_altered_without_changes_sizetiered(self):
        class SizeTieredCompactionChangesDetectionTest(Model):

            __options__ = {'compaction': {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
                                          'bucket_high': '20',
                                          'bucket_low': '10',
                                          'max_threshold': '200',
                                          'min_threshold': '100',
                                          'min_sstable_size': '1000',
                                          'tombstone_threshold': '0.125',
                                          'tombstone_compaction_interval': '3600'}}
            pk = columns.Integer(primary_key=True)

        drop_table(SizeTieredCompactionChangesDetectionTest)
        sync_table(SizeTieredCompactionChangesDetectionTest)

        self.assertFalse(_update_options(SizeTieredCompactionChangesDetectionTest))

    def test_alter_actually_alters(self):
        tmp = copy.deepcopy(LeveledCompactionTestTable)
        drop_table(tmp)
        sync_table(tmp)
        tmp.__options__ = {'compaction': {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}}
        sync_table(tmp)

        table_meta = _get_table_metadata(tmp)

        self.assertRegex(table_meta.export_as_string(), '.*SizeTieredCompactionStrategy.*')

    def test_alter_options(self):

        class AlterTable(Model):

            __options__ = {'compaction': {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy',
                                          'sstable_size_in_mb': '64'}}
            user_id = columns.UUID(primary_key=True)
            name = columns.Text()

        drop_table(AlterTable)
        sync_table(AlterTable)
        table_meta = _get_table_metadata(AlterTable)
        self.assertRegex(table_meta.export_as_string(), ".*'sstable_size_in_mb': '64'.*")
        AlterTable.__options__['compaction']['sstable_size_in_mb'] = '128'
        sync_table(AlterTable)
        table_meta = _get_table_metadata(AlterTable)
        self.assertRegex(table_meta.export_as_string(), ".*'sstable_size_in_mb': '128'.*")


class OptionsTest(BaseCassEngTestCase):

    def _verify_options(self, table_meta, expected_options):
        cql = table_meta.export_as_string()

        for name, value in expected_options.items():
            if isinstance(value, str):
                self.assertIn("%s = '%s'" % (name, value), cql)
            else:
                start = cql.find("%s = {" % (name,))
                end = cql.find('}', start)
                for subname, subvalue in value.items():
                    attr = "'%s': '%s'" % (subname, subvalue)
                    found_at = cql.find(attr, start)
                    self.assertTrue(found_at > start)
                    self.assertTrue(found_at < end)

    def test_all_size_tiered_options(self):
        class AllSizeTieredOptionsModel(Model):
            __options__ = {'compaction': {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
                                          'bucket_low': '.3',
                                          'bucket_high': '2',
                                          'min_threshold': '2',
                                          'max_threshold': '64',
                                          'tombstone_compaction_interval': '86400'}}

            cid = columns.UUID(primary_key=True)
            name = columns.Text()

        drop_table(AllSizeTieredOptionsModel)
        sync_table(AllSizeTieredOptionsModel)

        table_meta = _get_table_metadata(AllSizeTieredOptionsModel)
        self._verify_options(table_meta, AllSizeTieredOptionsModel.__options__)

    def test_all_leveled_options(self):

        class AllLeveledOptionsModel(Model):
            __options__ = {'compaction': {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy',
                                          'sstable_size_in_mb': '64'}}

            cid = columns.UUID(primary_key=True)
            name = columns.Text()

        drop_table(AllLeveledOptionsModel)
        sync_table(AllLeveledOptionsModel)

        table_meta = _get_table_metadata(AllLeveledOptionsModel)
        self._verify_options(table_meta, AllLeveledOptionsModel.__options__)
