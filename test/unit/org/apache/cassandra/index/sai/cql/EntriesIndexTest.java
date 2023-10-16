/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.cql;

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

public class EntriesIndexTest extends SAITester
{
    @Test
    public void createEntriesIndexEqualityTest()
    {
        createTable("CREATE TABLE %s (partition int primary key, item_cost map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(item_cost)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (partition, item_cost) VALUES (1, {'apple': 1, 'orange': -2})");
        flush();
        execute("INSERT INTO %s (partition, item_cost) VALUES (2, {'apple': 2, 'orange': 1})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (3, {'apple': 1, 'orange': 3})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (4, {'apple': 10, 'orange': -7})");

        // Test equality over both sstable and memtable
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] = 1"), row(1), row(3));
        // Test sstable read
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['orange'] = -2"), row(1));
        // Test memtable read
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['orange'] = -7"), row(4));
        // Test miss
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['orange'] = -3"));
    }

    @Test
    public void createEntriesIndexRangeTest()
    {
        createTable("CREATE TABLE %s (partition int primary key, item_cost map<text, int>)");
        createIndex("CREATE CUSTOM INDEX ON %s(entries(item_cost)) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (partition, item_cost) VALUES (1, {'apple': 1, 'orange': 2})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (4, {'apple': 3, 'orange': 2})");
        flush();
        execute("INSERT INTO %s (partition, item_cost) VALUES (2, {'apple': 2, 'orange': 1})");
        execute("INSERT INTO %s (partition, item_cost) VALUES (3, {'apple': 1, 'orange': 3})");

        // GT cases with all, some, and no results
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] > 0"),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] > 1"), row(2), row(4));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] > 2"), row(4));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] > 3"));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] > " + Integer.MAX_VALUE));

        // GTE cases with all, some, and no results
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] >= 0"),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] >= 1"),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] >= 2"), row(2), row(4));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] >= 3"),
                   row(4));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] >= " + Integer.MAX_VALUE));

        // LT cases with all, some, and no results
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < " + Integer.MAX_VALUE),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 4"),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 2"),
                   row(1), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 1"));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] < 0"));

        // LTE cases with all, some, and no results
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] <= " + Integer.MAX_VALUE),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] <= 4"),
                   row(1), row(2), row(4), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] <= 2"),
                   row(1), row(2), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] <= 1"),
                   row(1), row(3));
        assertRows(execute("SELECT partition FROM %s WHERE item_cost['apple'] <= 0"));
    }
}
