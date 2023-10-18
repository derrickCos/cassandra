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
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.postings.ScanningPostingsReader;
import org.apache.cassandra.index.sai.disk.v1.postings.TraversingPostingsReader;
import org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryReader;
import org.apache.cassandra.index.sai.disk.v2.sortedterms.TrieRangeIterator;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.store.IndexInput;

import static org.apache.cassandra.index.sai.utils.SAICodecUtils.validate;

/**
 * Synchronous reader of terms dictionary and postings lists to produce a {@link PostingList} with matching row ids.
 *
 * {@link #exactMatch(ByteComparable, QueryEventListener.TrieIndexEventListener, QueryContext)} does:
 * <ul>
 * <li>{@link TermQuery#lookupTermDictionary(ByteComparable)}: does term dictionary lookup to find the posting list file
 * position</li>
 * <li>{@link TermQuery#getPostingReader(long)}: reads posting list block summary and initializes posting read which
 * reads the first block of the posting list into memory</li>
 * </ul>
 */
public class TermsReader implements Closeable
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final IndexContext indexContext;
    private final FileHandle termDictionaryFile;
    private final FileHandle postingsFile;
    private final long termDictionaryRoot;

    public TermsReader(IndexContext indexContext,
                       FileHandle termsData,
                       FileHandle postingLists,
                       long root,
                       long termsFooterPointer) throws IOException
    {
        this.indexContext = indexContext;
        termDictionaryFile = termsData;
        postingsFile = postingLists;
        termDictionaryRoot = root;

        try (final IndexInput indexInput = IndexFileUtils.instance.openInput(termDictionaryFile))
        {
            // if the pointer is -1 then this is a previous version of the index
            // use the old way to validate the footer
            // the footer pointer is used due to encrypted indexes padding extra bytes
            if (termsFooterPointer == -1)
            {
                validate(indexInput);
            }
            else
            {
                validate(indexInput, termsFooterPointer);
            }
        }

        try (final IndexInput indexInput = IndexFileUtils.instance.openInput(postingsFile))
        {
            validate(indexInput);
        }
    }

    @Override
    public void close()
    {
        try
        {
            termDictionaryFile.close();
        }
        finally
        {
            postingsFile.close();
        }
    }

    public TermsIterator allTerms(long segmentOffset)
    {
        // blocking, since we use it only for segment merging for now
        return new TermsScanner(segmentOffset);
    }

    public PostingList exactMatch(ByteComparable term, QueryEventListener.TrieIndexEventListener perQueryEventListener, QueryContext context)
    {
        perQueryEventListener.onSegmentHit();
        return new TermQuery(term, perQueryEventListener, context).execute();
    }

    public PostingList rangeMatch(Expression exp, QueryEventListener.TrieIndexEventListener perQueryEventListener, QueryContext context)
    {
        perQueryEventListener.onSegmentHit();
        return new RangeQuery(exp, perQueryEventListener, context).execute();
    }

    @VisibleForTesting
    public class TermQuery
    {
        private final IndexInput postingsInput;
        private final IndexInput postingsSummaryInput;
        private final QueryEventListener.TrieIndexEventListener listener;
        private final long lookupStartTime;
        private final QueryContext context;

        private ByteComparable term;

        TermQuery(ByteComparable term, QueryEventListener.TrieIndexEventListener listener, QueryContext context)
        {
            this.listener = listener;
            postingsInput = IndexFileUtils.instance.openInput(postingsFile);
            postingsSummaryInput = IndexFileUtils.instance.openInput(postingsFile);
            this.term = term;
            lookupStartTime = System.nanoTime();
            this.context = context;
        }

        public PostingList execute()
        {
            try
            {
                long postingOffset = lookupTermDictionary(term);
                if (postingOffset == PostingList.OFFSET_NOT_FOUND)
                {
                    FileUtils.closeQuietly(postingsInput);
                    FileUtils.closeQuietly(postingsSummaryInput);
                    return null;
                }

                context.checkpoint();

                // when posting is found, resources will be closed when posting reader is closed.
                return getPostingReader(postingOffset);
            }
            catch (Throwable e)
            {
                //TODO Is there an equivalent of AOE in OS?
                if (!(e instanceof AbortedOperationException))
                    logger.error(indexContext.logMessage("Failed to execute term query"), e);

                closeOnException();
                throw Throwables.cleaned(e);
            }
        }

        private void closeOnException()
        {
            FileUtils.closeQuietly(postingsInput);
            FileUtils.closeQuietly(postingsSummaryInput);
        }

        public long lookupTermDictionary(ByteComparable term)
        {
            try (TrieTermsDictionaryReader reader = new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(), termDictionaryRoot))
            {
                final long offset = reader.exactMatch(term);

                listener.onTraversalComplete(System.nanoTime() - lookupStartTime, TimeUnit.NANOSECONDS);

                if (offset == TrieTermsDictionaryReader.NOT_FOUND)
                    return PostingList.OFFSET_NOT_FOUND;

                return offset;
            }
        }

        public PostingsReader getPostingReader(long offset) throws IOException
        {
            PostingsReader.BlocksSummary header = new PostingsReader.BlocksSummary(postingsSummaryInput, offset);

            return new PostingsReader(postingsInput, header, listener.postingListEventListener());
        }
    }

    @VisibleForTesting
    public class RangeQuery
    {
        private final IndexInput postingsInput;
        private final IndexInput postingsSummaryInput;
        private final QueryEventListener.TrieIndexEventListener listener;
        private final long lookupStartTime;
        private final QueryContext context;

        private Expression exp;

        RangeQuery(Expression exp, QueryEventListener.TrieIndexEventListener listener, QueryContext context)
        {
            this.listener = listener;
            postingsInput = IndexFileUtils.instance.openInput(postingsFile);
            postingsSummaryInput = IndexFileUtils.instance.openInput(postingsFile);
            this.exp = exp;
            lookupStartTime = System.nanoTime();
            this.context = context;
        }

        private ByteComparable getByteComparable(ByteBuffer byteBuffer, boolean isLower)
        {
            return ByteComparable.fixedLength(CompositeType.extractFirstComponentAsTrieSearchPrefix(byteBuffer, isLower));
        }

        public PostingList execute()
        {
            final ByteComparable lower = exp.lower != null ? getByteComparable(exp.lower.value.encoded, true) : null;
            final ByteComparable upper = exp.upper != null ? getByteComparable(exp.upper.value.encoded, false) : null;
            try (TrieRangeIterator reader = new TrieRangeIterator(termDictionaryFile.instantiateRebufferer(),
                                                                  termDictionaryRoot,
                                                                  lower,
                                                                  upper,
                                                                  exp.lowerInclusive,
                                                                  exp.upperInclusive))
            {
                var iter = Iterators.peekingIterator(reader.iterator());
                if (!iter.hasNext())
                {
                    FileUtils.closeQuietly(postingsInput);
                    FileUtils.closeQuietly(postingsSummaryInput);
                    return PostingList.EMPTY;
                }

                context.checkpoint();

                return new TraversingPostingsReader(exp, postingsInput, iter, listener.postingListEventListener());
            }
            catch (Throwable e)
            {
                //TODO Is there an equivalent of AOE in OS?
                if (!(e instanceof AbortedOperationException))
                    logger.error(indexContext.logMessage("Failed to execute term query"), e);

                closeOnException();
                throw Throwables.cleaned(e);
            }
        }

        private void closeOnException()
        {
            FileUtils.closeQuietly(postingsInput);
            FileUtils.closeQuietly(postingsSummaryInput);
        }

    }

    // currently only used for testing
    private class TermsScanner implements TermsIterator
    {
        private final long segmentOffset;
        private final TrieTermsDictionaryReader termsDictionaryReader;
        private final Iterator<Pair<ByteComparable, Long>> iterator;
        private final ByteBuffer minTerm, maxTerm;
        private Pair<ByteComparable, Long> entry;

        private TermsScanner(long segmentOffset)
        {
            this.termsDictionaryReader = new TrieTermsDictionaryReader(termDictionaryFile.instantiateRebufferer(), termDictionaryRoot);

            this.minTerm = ByteBuffer.wrap(ByteSourceInverse.readBytes(termsDictionaryReader.getMinTerm().asComparableBytes(ByteComparable.Version.OSS41)));
            this.maxTerm = ByteBuffer.wrap(ByteSourceInverse.readBytes(termsDictionaryReader.getMaxTerm().asComparableBytes(ByteComparable.Version.OSS41)));
            this.iterator = termsDictionaryReader.iterator();
            this.segmentOffset = segmentOffset;
        }

        @Override
        @SuppressWarnings("resource")
        public PostingList postings() throws IOException
        {
            assert entry != null;
            final IndexInput input = IndexFileUtils.instance.openInput(postingsFile);
            return new OffsetPostingList(segmentOffset, new ScanningPostingsReader(input, new PostingsReader.BlocksSummary(input, entry.right)));
        }

        @Override
        public void close()
        {
            termsDictionaryReader.close();
        }

        @Override
        public ByteBuffer getMinTerm()
        {
            return minTerm;
        }

        @Override
        public ByteBuffer getMaxTerm()
        {
            return maxTerm;
        }

        @Override
        public ByteComparable next()
        {
            if (iterator.hasNext())
            {
                entry = iterator.next();
                return entry.left;
            }
            return null;
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }
    }

    private class OffsetPostingList implements PostingList
    {
        private final long offset;
        private final PostingList wrapped;

        OffsetPostingList(long offset, PostingList postingList)
        {
            this.offset = offset;
            this.wrapped = postingList;
        }

        @Override
        public long nextPosting() throws IOException
        {
            long next = wrapped.nextPosting();
            if (next == PostingList.END_OF_STREAM)
                return next;
            return next + offset;
        }

        @Override
        public long size()
        {
            return wrapped.size();
        }

        @Override
        public long advance(long targetRowID) throws IOException
        {
            long next = wrapped.advance(targetRowID);
            if (next == PostingList.END_OF_STREAM)
                return next;
            return next + offset;
        }
    }
}
