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
package org.apache.cassandra.index.sai.utils;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tracing.Tracing;

/**
 * A simple intersection iterator that makes no real attempts at optimising the iteration apart from
 * initially sorting the ranges. This implementation also supports an intersection limit which limits
 * the number of ranges that will be included in the intersection. This currently defaults to 2.
 */
@SuppressWarnings("resource")
public class RangeIntersectionIterator<T extends Comparable<T>> extends RangeIterator<T>
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // The cassandra.sai.intersection.clause.limit (default: 2) controls the maximum number of range iterator that
    // will be used in the final intersection of a query operation.
    private static final int INTERSECTION_CLAUSE_LIMIT = Integer.getInteger("cassandra.sai.intersection.clause.limit", 2);

    static
    {
        logger.info(String.format("Storage attached index intersection clause limit is %d", INTERSECTION_CLAUSE_LIMIT));
    }

    public static boolean shouldDefer(int numberOfExpressions)
    {
        return (INTERSECTION_CLAUSE_LIMIT <= 0) || (numberOfExpressions <= INTERSECTION_CLAUSE_LIMIT);
    }

    private final List<RangeIterator<T>> ranges;

    private RangeIntersectionIterator(Builder.Statistics<T> statistics, List<RangeIterator<T>> ranges)
    {
        super(statistics);
        this.ranges = ranges;
    }

    protected T computeNext()
    {
        // Range iterator that has been advanced in the previous cycle of the outer loop.
        // Initially there hasn't been the previous cycle, so set to null.
        int alreadyAvanced = -1;

        // The highest primary key seen on any range iterator so far.
        // It can become null when we reach the end of the iterator.
        T highestKey = (T) getCurrent();

        outer:
        while (highestKey != null && highestKey.compareTo(getMaximum()) <= 0)
        {
            // Try advance all iterators to the highest key seen so far.
            // Once this inner loop finishes normally, all iterators are guaranteed to be at the same value.
            for (int index = 0; index < ranges.size(); index++)
            {
                if (index != alreadyAvanced)
                {
                    RangeIterator<T> range = ranges.get(index);
                    T nextKey = nextOrNull(range, highestKey);
                    if (nextKey == null || nextKey.compareTo(highestKey) > 0)
                    {
                        // We jumped over the highest key seen so far, so make it the new highest key.
                        highestKey = nextKey;
                        // Remember this iterator to avoid advancing it again, because it is already at the highest key
                        alreadyAvanced = index;
                        // This iterator jumped over, so the other iterators are lagging behind now,
                        // including the ones already advanced in the earlier cycles of the inner loop.
                        // Therefore, restart the inner loop in order to advance
                        // the other iterators except this one to match the new highest key.
                        continue outer;
                    }
                    assert nextKey.compareTo(highestKey) == 0:
                    String.format("skipTo skipped to an item smaller than the target; " +
                                  "iterator: %s, target key: %s, returned key: %s", range, highestKey, nextKey);
                }
            }
            // If we reached here, next() has been called at least once on each range iterator and
            // the last call to next() on each iterator returned a value equal to the highestKey.
            return highestKey;
        }
        return (T) endOfData();
    }

    protected void performSkipTo(T nextToken)
    {
        for (var range : ranges)
            if (range.hasNext())
                range.skipTo(nextToken);
    }

    /**
     * Fetches the next available item from the iterator, such that the item is not lower than the given key.
     * If no such items are available, returns null.
     */
    private T nextOrNull(RangeIterator<T> iterator, T minKey)
    {
        iterator.skipTo(minKey);
        return iterator.hasNext() ? iterator.next() : null;
    }

    public void close() throws IOException
    {
        ranges.forEach(FileUtils::closeQuietly);
    }

    public static <T extends Comparable<T>> Builder<T> builder(List<RangeIterator<T>> ranges, int limit)
    {
        var builder = new Builder<T>(ranges.size(), limit);
        for (var range : ranges)
            builder.add(range);
        return builder;
    }

    public static <T extends Comparable<T>> Builder<T> builder(int size, int limit)
    {
        return new Builder<>(size, limit);
    }

    public static <T extends Comparable<T>> Builder<T> builder(int limit)
    {
        return builder(10, limit);
    }

    public static <T extends Comparable<T>> Builder<T> sizedBuilder(int size)
    {
        return builder(size, INTERSECTION_CLAUSE_LIMIT);
    }

    public static <T extends Comparable<T>> Builder<T> builder()
    {
        return builder(INTERSECTION_CLAUSE_LIMIT);
    }

    public static class Builder<T extends Comparable<T>> extends RangeIterator.Builder<T>
    {
        private final int limit;
        protected List<RangeIterator<T>> rangeIterators;

        private Builder(int size, int limit)
        {
            super(IteratorType.INTERSECTION);
            rangeIterators = new ArrayList<>(size);
            this.limit = limit;
        }

        public RangeIterator.Builder<T> add(RangeIterator<T> range)
        {
            if (range == null)
                return this;

            if (range.getCount() > 0)
                rangeIterators.add(range);
            else
                FileUtils.closeQuietly(range);
            statistics.update(range);

            return this;
        }

        public RangeIterator.Builder<T> add(List<RangeIterator<T>> ranges)
        {
            if (ranges == null || ranges.isEmpty())
                return this;

            ranges.forEach(this::add);
            return this;
        }

        public int rangeCount()
        {
            return rangeIterators.size();
        }

        protected RangeIterator<T> buildIterator()
        {
            rangeIterators.sort(Comparator.comparingLong(RangeIterator::getCount));
            int initialSize = rangeIterators.size();
            // all ranges will be included
            if (limit >= rangeIterators.size() || limit <= 0)
                return buildIterator(statistics, rangeIterators);

            // Apply most selective iterators during intersection, because larger number of iterators will result lots of disk seek.
            Statistics<T> selectiveStatistics = new Statistics<>(IteratorType.INTERSECTION);
            for (int i = rangeIterators.size() - 1; i >= 0 && i >= limit; i--)
                FileUtils.closeQuietly(rangeIterators.remove(i));

            for (var iterator : rangeIterators)
                selectiveStatistics.update(iterator);

            if (Tracing.isTracing())
                Tracing.trace("Selecting {} {} of {} out of {} indexes",
                              rangeIterators.size(),
                              rangeIterators.size() > 1 ? "indexes with cardinalities" : "index with cardinality",
                              rangeIterators.stream().map(RangeIterator::getCount).map(Object::toString).collect(Collectors.joining(", ")),
                              initialSize);

            return buildIterator(selectiveStatistics, rangeIterators);
        }

        private static <T extends Comparable<T>> RangeIterator<T> buildIterator(Statistics<T> statistics, List<RangeIterator<T>> ranges)
        {
            // if the range is disjoint or we have an intersection with an empty set,
            // we can simply return an empty iterator, because it's not going to produce any results.
            if (statistics.isDisjoint())
            {
                // release posting lists
                FileUtils.closeQuietly(ranges);
                return RangeIterator.empty();
            }

            if (ranges.size() == 1)
                return ranges.get(0);

            return new RangeIntersectionIterator<>(statistics, ranges);
        }
    }
}
