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
package org.apache.cassandra.db.partitions;

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.*;

import io.reactivex.Completable;
import io.reactivex.Single;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.rows.publisher.PartitionsPublisher;
import org.apache.cassandra.db.rows.publisher.ReduceCallbacks;
import org.apache.cassandra.db.transform.FilteredPartitions;
import org.apache.cassandra.db.transform.MorePartitions;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Reducer;

/**
 * Static methods to work with partition iterators.
 */
public abstract class UnfilteredPartitionIterators
{
    private static final Versioned<EncodingVersion, Serializer> serializers = EncodingVersion.versioned(Serializer::new);

    private static final Comparator<UnfilteredRowIterator> partitionComparator = Comparator.comparing(BaseRowIterator::partitionKey);

    private UnfilteredPartitionIterators() {}

    public interface MergeListener
    {
        public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions);

        /**
         * Forces merge listener to be called even when there is only
         * a single iterator.
         * <p>
         * This can be useful for listeners that require seeing all row updates.
         */
        public default boolean callOnTrivialMerge()
        {
            return true;
        }

        public void close();

        public static final MergeListener NONE = new MergeListener()
        {
            public org.apache.cassandra.db.rows.UnfilteredRowIterators.MergeListener getRowMergeListener(
                    DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
            {
                return null;
            }

            public boolean callOnTrivialMerge()
            {
                return false;
            }

            public void close()
            {
            }
        };
    }

    @SuppressWarnings("resource") // The created resources are returned right away
    public static UnfilteredRowIterator getOnlyElement(final UnfilteredPartitionIterator iter, SinglePartitionReadCommand command)
    {
        // If the query has no results, we'll get an empty iterator, but we still
        // want a RowIterator out of this method, so we return an empty one.
        UnfilteredRowIterator toReturn = iter.hasNext()
                              ? iter.next()
                              : EmptyIterators.unfilteredRow(command.metadata(),
                                                             command.partitionKey(),
                                                             command.clusteringIndexFilter().isReversed());

        // Note that in general, we should wrap the result so that it's close method actually
        // close the whole UnfilteredPartitionIterator.
        class Close extends Transformation
        {
            public void onPartitionClose()
            {
                // asserting this only now because it bothers Serializer if hasNext() is called before
                // the previously returned iterator hasn't been fully consumed.
                boolean hadNext = iter.hasNext();
                iter.close();
                assert !hadNext;
            }
        }
        return Transformation.apply(toReturn, new Close());
    }

    public static UnfilteredPartitionIterator concat(final List<UnfilteredPartitionIterator> iterators, TableMetadata metadata)
    {
        if (iterators.isEmpty())
            return EmptyIterators.unfilteredPartition(metadata);

        if (iterators.size() == 1)
            return iterators.get(0);

        class Extend implements MorePartitions<UnfilteredPartitionIterator>
        {
            int i = 1;
            public UnfilteredPartitionIterator moreContents()
            {
                if (i >= iterators.size())
                    return null;
                return iterators.get(i++);
            }
        }

        return MorePartitions.extend(iterators.get(0), new Extend());
    }

    public static Single<UnfilteredPartitionIterator> concat(final Iterable<Single<UnfilteredPartitionIterator>> iterators)
    {
        Iterator<Single<UnfilteredPartitionIterator>> it = iterators.iterator();
        assert it.hasNext();
        Single<UnfilteredPartitionIterator> it0 = it.next();
        if (!it.hasNext())
            return it0;

        class Extend implements MorePartitions<UnfilteredPartitionIterator>
        {
            public UnfilteredPartitionIterator moreContents()
            {
                if (!it.hasNext())
                    return null;
                return it.next().blockingGet();
            }
        }
        return it0.map(i -> MorePartitions.extend(i, new Extend()));
    }

    public static PartitionIterator mergeAndFilter(List<UnfilteredPartitionIterator> iterators, int nowInSec, MergeListener listener)
    {
        // TODO: we could have a somewhat faster version if we were to merge the UnfilteredRowIterators directly as RowIterators
        return filter(merge(iterators, nowInSec, listener), nowInSec);
    }

    public static PartitionIterator filter(final UnfilteredPartitionIterator iterator, final int nowInSec)
    {
        return FilteredPartitions.filter(iterator, nowInSec);
    }

    public static UnfilteredPartitionIterator merge(final List<? extends UnfilteredPartitionIterator> iterators, final int nowInSec, final MergeListener listener)
    {
        assert listener != null;
        assert !iterators.isEmpty();

        final TableMetadata metadata = iterators.get(0).metadata();

        // TODO review the merge of this
        UnfilteredMergeReducer reducer = new UnfilteredMergeReducer(metadata, iterators.size(), nowInSec, listener);
        final MergeIterator<UnfilteredRowIterator, UnfilteredRowIterator> merged = MergeIterator.get(iterators, partitionComparator, reducer);

        return new AbstractUnfilteredPartitionIterator()
        {
            public TableMetadata metadata()
            {
                return metadata;
            }

            public boolean hasNext()
            {
                return merged.hasNext();
            }

            public UnfilteredRowIterator next()
            {
                return merged.next();
            }

            @Override
            public void close()
            {
                merged.close();
                listener.close();
            }
        };
    }

    static class UnfilteredMergeReducer extends Reducer<UnfilteredRowIterator, UnfilteredRowIterator>
    {
        private final int numIterators;
        private final int nowInSec;
        private final MergeListener listener;
        private final List<UnfilteredRowIterator> toMerge;
        private final UnfilteredRowIterator emptyIterator;

        private DecoratedKey partitionKey;
        private boolean isReverseOrder;

        UnfilteredMergeReducer(TableMetadata metadata, int numIterators, int nowInSec, final MergeListener listener)
        {
            this.numIterators = numIterators;
            this.nowInSec = nowInSec;
            this.listener = listener;

            emptyIterator = EmptyIterators.unfilteredRow(metadata, null, false);
            toMerge = new ArrayList<>(numIterators);
        }

        public void reduce(int idx, UnfilteredRowIterator current)
        {
            partitionKey = current.partitionKey();
            isReverseOrder = current.isReverseOrder();

            // Note that because the MergeListener cares about it, we want to preserve the index of the iterator.
            // Non-present iterator will thus be set to empty in getReduced.
            toMerge.set(idx, current);
        }

        public UnfilteredRowIterator getReduced()
        {
            UnfilteredRowIterators.MergeListener rowListener = listener.getRowMergeListener(partitionKey, toMerge);

            ((EmptyIterators.EmptyUnfilteredRowIterator)emptyIterator).reuse(partitionKey, isReverseOrder, DeletionTime.LIVE);

            // Replace nulls by empty iterators
            UnfilteredRowIterator nonEmptyRowIterator = null;
            int numNonEmptyRowIterators = 0;

            for (int i = 0, length = toMerge.size(); i < length; i++)
            {
                UnfilteredRowIterator element = toMerge.get(i);
                if (element == null)
                {
                    toMerge.set(i, emptyIterator);
                }
                else
                {
                    numNonEmptyRowIterators++;
                    nonEmptyRowIterator = element;
                }
            }

            return numNonEmptyRowIterators == 1 && !listener.callOnTrivialMerge() ? nonEmptyRowIterator : UnfilteredRowIterators.merge(toMerge, nowInSec, rowListener);
        }

        public void onKeyChange()
        {
            toMerge.clear();
            for (int i = 0, length = numIterators; i < length; i++)
                toMerge.add(null);
        }
    }

    /**
     * Digests the the provided iterator.
     *
     * @param iterator the iterator to digest.
     * @param digest the {@code MessageDigest} to use for the digest.
     * @param version the version to use when producing the digest.
     */
    public static Completable digest(UnfilteredPartitionIterator iterator, MessageDigest digest, DigestVersion version)
    {
        return Completable.fromAction(() ->
          {
              try (UnfilteredPartitionIterator iter = iterator)
              {
                  while (iter.hasNext())
                  {
                      try (UnfilteredRowIterator partition = iter.next())
                      {
                          UnfilteredRowIterators.digest(partition, digest, version);
                      }
                  }
              }
          });
    }

    public static Single<MessageDigest> digest(PartitionsPublisher partitions, MessageDigest digest, DigestVersion version)
    {
        return partitions.reduce(ReduceCallbacks.create(digest,
                                                        (s, partition) -> UnfilteredRowIterators.digestPartition(partition, digest, version),
                                                        (s, unfiltered) -> UnfilteredRowIterators.digestUnfiltered(unfiltered, digest)));
    }

    public static Serializer serializerForIntraNode(EncodingVersion version)
    {
        return serializers.get(version);
    }

    /**
     * Wraps the provided iterator so it logs the returned rows/RT for debugging purposes.
     * <p>
     * Note that this is only meant for debugging as this can log a very large amount of
     * logging at INFO.
     */
    public static UnfilteredPartitionIterator loggingIterator(UnfilteredPartitionIterator iterator, final String id, final boolean fullDetails)
    {
        class Logging extends Transformation<UnfilteredRowIterator>
        {
            public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
            {
                return UnfilteredRowIterators.loggingIterator(partition, id, fullDetails);
            }
        }
        return Transformation.apply(iterator, new Logging());
    }

    /**
     * Serialize each UnfilteredSerializer one after the other, with an initial byte that indicates whether
     * we're done or not.
     */
    public static class Serializer extends VersionDependent<EncodingVersion>
    {
        private Serializer(EncodingVersion version)
        {
            super(version);
        }

        public void serialize(UnfilteredPartitionIterator iter, ColumnFilter selection, DataOutputPlus out) throws IOException
        {
            // Previously, a boolean indicating if this was for a thrift query.
            // Unused since 4.0 but kept on wire for compatibility.
            out.writeBoolean(false);
            while (iter.hasNext())
            {
                out.writeBoolean(true);
                try (UnfilteredRowIterator partition = iter.next())
                {
                    UnfilteredRowIteratorSerializer.serializers.get(version).serialize(partition, selection, out);
                }
            }
            out.writeBoolean(false);
        }

        public Single<ByteBuffer> serialize(PartitionsPublisher partitions, ColumnFilter selection)
        {
            return partitions.reduce(new ReduceCallbacks<DataOutputBuffer, SerializationHeader>(
            () -> {
                final DataOutputBuffer out = new DataOutputBuffer();
                // Previously, a boolean indicating if this was for a thrift query.
                // Unused since 4.0 but kept on wire for compatibility.
                out.writeBoolean(false);
                return out;
            },
            (out, partition) -> {
                out.writeBoolean(true); // next partition
                SerializationHeader header = new SerializationHeader(false,
                                                                     partition.metadata(),
                                                                     partition.columns(),
                                                                     partition.stats());
                UnfilteredRowIteratorSerializer.serializers.get(version)
                                                           .serializeBeginningOfPartition(partition, header, selection, out, -1);
                return header;
            },
            (out, header, unfiltered) -> {
                UnfilteredRowIteratorSerializer.serializers.get(version).serialize(unfiltered, header, out);
                return header;
            },
            (out, header, partition) -> {
                if (!partition.isEmpty())
                    UnfilteredRowIteratorSerializer.serializers.get(version).serializeEndOfPartition(out);
                return out;
            }
            )).map(out -> {
                out.writeBoolean(false); // no more partitions
                ByteBuffer ret = out.buffer();
                out.close();
                return ret;
            });
        }

        public UnfilteredPartitionIterator deserialize(final DataInputPlus in, final TableMetadata metadata, final ColumnFilter selection, final SerializationHelper.Flag flag) throws IOException
        {
            // Skip now unused isForThrift boolean
            in.readBoolean();

            return new AbstractUnfilteredPartitionIterator()
            {
                private UnfilteredRowIterator next;
                private boolean hasNext;
                private boolean nextReturned = true;

                public TableMetadata metadata()
                {
                    return metadata;
                }

                public boolean hasNext()
                {
                    if (!nextReturned)
                        return hasNext;

                    // We can't answer this until the previously returned iterator has been fully consumed,
                    // so complain if that's not the case.
                    if (next != null && next.hasNext())
                        throw new IllegalStateException("Cannot call hasNext() until the previous iterator has been fully consumed");

                    try
                    {
                        hasNext = in.readBoolean();
                        nextReturned = false;
                        return hasNext;
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }

                public UnfilteredRowIterator next()
                {
                    if (nextReturned && !hasNext())
                        throw new NoSuchElementException();

                    try
                    {
                        nextReturned = true;
                        next = UnfilteredRowIteratorSerializer.serializers.get(version).deserialize(in, metadata, selection, flag);
                        return next;
                    }
                    catch (IOException e)
                    {
                        throw new IOError(e);
                    }
                }

                @Override
                public void close()
                {
                    if (next != null)
                        next.close();
                }
            };
        }
    }
}
