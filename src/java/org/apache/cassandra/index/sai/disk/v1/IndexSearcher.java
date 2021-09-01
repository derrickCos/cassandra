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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;

/**
 * Abstract reader for individual segments of an on-disk index.
 *
 * Accepts shared resources (token/offset file readers), and uses them to perform lookups against on-disk data
 * structures.
 */
public abstract class IndexSearcher implements Closeable
{
    private final LongArray.Factory rowIdToTokenFactory;
    private final LongArray.Factory rowIdToOffsetFactory;
    private final V1SSTableContext.KeyFetcher keyFetcher;

    final PerIndexFiles indexFiles;
    final SegmentMetadata metadata;
    final IndexContext indexContext;

    IndexSearcher(Segment segment, IndexContext indexContext)
    {
        this.rowIdToTokenFactory = segment.segmentRowIdToTokenFactory;
        this.rowIdToOffsetFactory = segment.segmentRowIdToOffsetFactory;
        this.keyFetcher = segment.keyFetcher;
        this.indexFiles = segment.indexFiles;
        this.metadata = segment.metadata;
        this.indexContext = indexContext;
    }

    public static IndexSearcher open(Segment segment, IndexContext columnContext) throws IOException
    {
        return columnContext.isLiteral() ? new InvertedIndexSearcher(segment, columnContext)
                                         : new KDTreeIndexSearcher(segment, columnContext);
    }

    /**
     * @return number of per-index open files attached to a sstable
     */
    public static int openPerIndexFiles(AbstractType<?> columnType)
    {
        return TypeUtil.isLiteral(columnType) ? InvertedIndexSearcher.openPerIndexFiles() : KDTreeIndexSearcher.openPerIndexFiles();
    }

    /**
     * @return memory usage of underlying on-disk data structure
     */
    public abstract long indexFileCacheSize();

    /**
     * Search on-disk index synchronously.
     *
     * @param expression to filter on disk index
     * @param queryContext to track per sstable cache and per query metrics
     * @param defer create the iterator in a deferred state
     *
     * @return {@link RangeIterator} that matches given expression
     */
    public abstract RangeIterator search(Expression expression, SSTableQueryContext queryContext, boolean defer);

    RangeIterator toIterator(PostingList postingList, SSTableQueryContext queryContext, boolean defer)
    {
        if (postingList == null)
            return RangeIterator.empty();

        SearcherContext searcherContext = defer ? new DeferredSearcherContext(queryContext, postingList.peekable())
                                                : new DirectSearcherContext(queryContext, postingList.peekable());

        if (searcherContext.noOverlap)
            return RangeIterator.empty();

        RangeIterator iterator = new PostingListRangeIterator(indexContext, searcherContext, keyFetcher);

        return iterator;
    }

    public abstract class SearcherContext
    {
        long minToken;
        long maxToken;
        long maxPartitionOffset;
        boolean noOverlap;
        final LongArray segmentRowIdToToken;
        final LongArray segmentRowIdToOffset;
        final SSTableQueryContext context;
        final PostingList.PeekablePostingList postingList;

        SearcherContext(SSTableQueryContext context, PostingList.PeekablePostingList postingList)
        {
            this.context = context;
            this.postingList = postingList;

            // startingIndex of 0 means `findTokenRowId` should search all tokens in the segment.
            this.segmentRowIdToToken = new LongArray.DeferredLongArray(() -> rowIdToTokenFactory.openTokenReader(0, context));
            this.segmentRowIdToOffset = new LongArray.DeferredLongArray(() -> rowIdToOffsetFactory.open());

            minToken = calculateMinimumToken();

            // use segment's metadata for the range iterator, may not be accurate, but should not matter to performance.
            maxToken = metadata.maxKey.isMinimum()
                       ? toLongToken(DatabaseDescriptor.getPartitioner().getMaximumToken())
                       : toLongToken(metadata.maxKey);

            maxPartitionOffset = Long.MAX_VALUE;
        }

        long minToken()
        {
            return minToken;
        }

        long maxToken()
        {
            return maxToken;
        }

        abstract long calculateMinimumToken();

        abstract long count();

    }

    public class DirectSearcherContext extends SearcherContext
    {
        DirectSearcherContext(SSTableQueryContext context, PostingList.PeekablePostingList postingList)
        {
            super(context, postingList);
        }

        @Override
        long calculateMinimumToken()
        {
            // Use the first row id's token as min
            return this.segmentRowIdToToken.get(postingList.peek());
        }

        @Override
        long count()
        {
            return postingList.size();
        }
    }

    public class DeferredSearcherContext extends SearcherContext
    {
        DeferredSearcherContext(SSTableQueryContext context, PostingList.PeekablePostingList postingList)
        {
            super(context, postingList);
        }

        @Override
        long calculateMinimumToken()
        {
            // Use the segments min key min
            return toLongToken(metadata.minKey);
        }

        @Override
        long count()
        {
            return metadata.numRows;
        }
    }

    private static long toLongToken(DecoratedKey key)
    {
        return toLongToken(key.getToken());
    }

    private static long toLongToken(ByteBuffer key)
    {
        return toLongToken(DatabaseDescriptor.getPartitioner().getToken(key));
    }

    private static long toLongToken(Token token)
    {
        return (long) token.getTokenValue();
    }
}
