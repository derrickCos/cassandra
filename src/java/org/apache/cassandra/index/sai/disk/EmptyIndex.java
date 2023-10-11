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

package org.apache.cassandra.index.sai.disk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public class EmptyIndex implements SearchableIndex
{
    public EmptyIndex()
    {
    }

    @Override
    public long indexFileCacheSize()
    {
        return 0;
    }

    @Override
    public long getRowCount()
    {
        return 0;
    }

    @Override
    public long minSSTableRowId()
    {
        return -1;
    }

    @Override
    public long maxSSTableRowId()
    {
        return -1;
    }

    @Override
    public ByteBuffer minTerm()
    {
        return null;
    }

    @Override
    public ByteBuffer maxTerm()
    {
        return null;
    }

    @Override
    public DecoratedKey minKey()
    {
        return null;
    }

    @Override
    public DecoratedKey maxKey()
    {
        return null;
    }

    @Override
    public List<RangeIterator> search(Expression expression,
                                      AbstractBounds<PartitionPosition> keyRange,
                                      QueryContext context,
                                      boolean defer,
                                      int limit) throws IOException
    {
        return Collections.emptyList();
    }

    @Override
    public void populateSystemView(SimpleDataSet dataSet, SSTableReader sstable)
    {

    }

    @Override
    public void close() throws IOException
    {

    }

    @Override
    public RangeIterator limitToTopResults(QueryContext context, List<PrimaryKey> keys, Expression exp, int limit) throws IOException
    {
        return RangeIterator.empty();
    }
}
