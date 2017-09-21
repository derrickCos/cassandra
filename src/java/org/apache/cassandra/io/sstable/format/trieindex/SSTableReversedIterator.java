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
package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import java.util.NoSuchElementException;

import com.carrotsearch.hppc.LongStack;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.AbstractSSTableIterator;
import org.apache.cassandra.io.sstable.format.trieindex.RowIndexReader.IndexInfo;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.Rebufferer;

/**
 *  A Cell Iterator in reversed clustering order over SSTable
 */
class SSTableReversedIterator extends AbstractSSTableIterator
{
    /**
     * The index of the slice being processed.
     */
    private int slice;

    public SSTableReversedIterator(TrieIndexSSTableReader sstable,
                                   FileDataInput file,
                                   DecoratedKey key,
                                   RowIndexEntry indexEntry,
                                   Slices slices,
                                   ColumnFilter columns,
                                   Rebufferer.ReaderConstraint readerConstraint)
    {
        super(sstable, file, key, indexEntry, slices, columns, readerConstraint);
    }

    protected Reader createReaderInternal(RowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile, Rebufferer.ReaderConstraint rc)
    {
        return indexEntry.isIndexed()
             ? new ReverseIndexedReader(indexEntry, file, shouldCloseFile, rc)
             : new ReverseReader(file, shouldCloseFile);
    }

    public boolean isReverseOrder()
    {
        return true;
    }

    protected int nextSliceIndex()
    {
        int next = slice;
        slice++;
        return slices.size() - (next + 1);
    }

    protected int currentSliceIndex()
    {
        assert slice > 0;
        return slices.size() - slice;
    }

    protected boolean hasMoreSlices()
    {
        return slice < slices.size();
    }

    /**
     * Reverse iteration is performed by going through an index block (or the whole partition if not indexed) forwards
     * and storing the positions of each entry that falls within the slice in a stack. Reverse iteration then pops out
     * positions and reads the entries.
     *
     * Note: The earlier version of this was constructing an in-memory view of the block instead, which gives better
     * performance on bigger queries and index blocks (due to not having to read disk again). With the lower
     * granularity of the tries it makes better sense to store as little as possible as the beginning of the block
     * should very rarely be in other page/chunk cache locations. This has the benefit of being able to answer small
     * queries (esp. LIMIT 1) faster and with less GC churn.
     */
    private class ReverseReader extends Reader
    {
        LongStack rowOffsets = new LongStack();
        Long currentOffset = null;
        RangeTombstoneMarker blockOpenMarker, blockCloseMarker;
        Unfiltered next = null;
        boolean foundLessThan;
        long startPos = -1;

        private ReverseReader(FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
        }

        public void setForSlice(Slice slice) throws IOException
        {
            // read full row and filter
            if (startPos == -1)
                startPos = file.getFilePointer();
            else
                seekToPosition(startPos);

            fillOffsets(slice, true, true, Long.MAX_VALUE);
        }

        protected boolean hasNextInternal() throws IOException
        {
            if (next != null)
                return true;
            if (checkHasNext())
                next = computeNext();
            return next != null;
        }

        protected Unfiltered nextInternal() throws IOException
        {
            if (!hasNextInternal())
                throw new NoSuchElementException();

            Unfiltered toReturn = next;
            next = null;
            return toReturn;
        }

        private boolean checkHasNext() throws IOException
        {
            do
            {
                if (blockCloseMarker != null || !rowOffsets.isEmpty())
                    return true;
            }
            while (!foundLessThan && advanceIndexBlock());
            // open marker to be output only as slice is finished 
            return blockOpenMarker != null;
        }

        private Unfiltered computeNext() throws IOException
        {
            Unfiltered toReturn = null;
            if (blockCloseMarker != null)
            {
                toReturn = blockCloseMarker;
                blockCloseMarker = null;
                return toReturn;
            }
            while (!rowOffsets.isEmpty())
            {
                seekToPosition(rowOffsets.peek());
                boolean hasNext = deserializer.hasNext();
                assert hasNext;
                toReturn = deserializer.readNext();
                rowOffsets.pop();
                // We may get empty row for the same reason expressed on UnfilteredSerializer.deserializeOne.
                if (!toReturn.isEmpty())
                    return toReturn;
            }
            if (blockOpenMarker != null)
            {
                toReturn = blockOpenMarker;
                blockOpenMarker = null;
                return toReturn;
            }
            return null;
        }

        protected boolean advanceIndexBlock() throws IOException
        {
            return false;
        }

        void fillOffsets(Slice slice, boolean filterStart, boolean filterEnd, long stopPosition) throws IOException
        {
            filterStart &= !slice.start().equals(ClusteringBound.BOTTOM);
            filterEnd &= !slice.end().equals(ClusteringBound.TOP);
            long currentPosition = -1;

            ClusteringBound start = slice.start();
            currentPosition = file.getFilePointer();
            foundLessThan = false;
            // This is a copy of handlePreSliceData which also checks currentPosition < stopPosition.
            // Not extracted to method as we need both marker and currentPosition.
            if (filterStart)
            {
                while (currentPosition < stopPosition && deserializer.hasNext() && deserializer.compareNextTo(start) <= 0)
                {
                    if (deserializer.nextIsRow())
                        deserializer.skipNext();
                    else
                        updateOpenMarker((RangeTombstoneMarker)deserializer.readNext());

                    currentPosition = file.getFilePointer();
                    foundLessThan = true;
                }
            }

            // We've reached the beginning of our queried slice. If we have an open marker
            // we should return that at the end of the slice to close the deletion.
            if (openMarker != null)
                blockOpenMarker = new RangeTombstoneBoundMarker(start, openMarker);


            // Now deserialize everything until we reach our requested end (if we have one)
            // See SSTableIterator.ForwardRead.computeNext() for why this is a strict inequality below: this is the same
            // reasoning here.
            while (currentPosition < stopPosition && deserializer.hasNext()
                   && (!filterEnd || deserializer.compareNextTo(slice.end()) < 0))
            {
                rowOffsets.push(currentPosition);
                if (deserializer.nextIsRow())
                    deserializer.skipNext();
                else
                    updateOpenMarker((RangeTombstoneMarker)deserializer.readNext());

                currentPosition = file.getFilePointer();
            }

            // If we have an open marker, we should output that first, unless end is not being filtered
            // (i.e. it's either top (where a marker can't be open) or we placed that marker during previous block).
            if (openMarker != null && filterEnd)
            {
                // If we have no end and still an openMarker, this means we're indexed and the marker is closed in a following block.
                blockCloseMarker = new RangeTombstoneBoundMarker(slice.end(), openMarker);
            }
        }
    }

    private class ReverseIndexedReader extends ReverseReader
    {
        private RowIndexReverseIterator indexReader;
        final RowIndexEntry indexEntry;
        long basePosition;
        Slice currentSlice;
        long currentBlockStart;
        IndexInfo currentIndexInfo;
        Rebufferer.ReaderConstraint rc;

        public ReverseIndexedReader(RowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile, Rebufferer.ReaderConstraint rc)
        {
            super(file, shouldCloseFile);
            basePosition = indexEntry.position;
            this.indexEntry = indexEntry;
            this.rc = rc;
        }

        @Override
        public void close() throws IOException
        {
            if (indexReader != null)
                indexReader.close();
            super.close();
        }

        /**
         * This method must be async-read-safe.
         */
        @Override
        public void setForSlice(Slice slice) throws IOException
        {
            if (currentSlice == null || !slice.equals(currentSlice))
            {
                ClusteringComparator comparator = metadata.comparator;
                if (indexReader != null)
                    indexReader.close();
                indexReader = new RowIndexReverseIterator(((TrieIndexSSTableReader) sstable).rowIndexFile,
                                                          indexEntry,
                                                          comparator.asByteComparableSource(slice.end()), rc);
                blockOpenMarker = null;
                currentIndexInfo = indexReader.nextIndexInfo();
                gotoBlock(slice, currentIndexInfo, true, Long.MAX_VALUE);
                currentSlice = slice;
            }
        }

        boolean gotoBlock(Slice slice, IndexInfo indexInfo, boolean filterEnd, long blockEnd) throws IOException
        {
            blockCloseMarker = null;
            rowOffsets.clear();
            if (indexInfo == null)
                return false;
            long tmpCurrentBlockStart = basePosition + indexInfo.offset;
            openMarker = indexInfo.openDeletion;
            seekToPosition(tmpCurrentBlockStart);
            fillOffsets(slice, true, filterEnd, blockEnd);
            currentIndexInfo = null;
            currentBlockStart = tmpCurrentBlockStart;
            return !rowOffsets.isEmpty();
        }

        @Override
        protected boolean advanceIndexBlock() throws IOException
        {
            if (currentIndexInfo == null)
                currentIndexInfo = indexReader.nextIndexInfo();
            return gotoBlock(currentSlice, currentIndexInfo, false, currentBlockStart);
        }
    }
}
