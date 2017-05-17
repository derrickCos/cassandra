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
package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Observable;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.reactivestreams.Subscription;

public abstract class AbstractSSTableIterator implements UnfilteredRowIterator
{
    final static Logger logger = LoggerFactory.getLogger(AbstractSSTableIterator.class);
    protected final SSTableReader sstable;
    // We could use sstable.metadata(), but that can change during execution so it's good hygiene to grab an immutable instance
    protected final TableMetadata metadata;

    protected final DecoratedKey key;
    protected final DeletionTime partitionLevelDeletion;
    protected final ColumnFilter columns;
    protected final SerializationHelper helper;

    protected final Row staticRow;
    protected final Reader reader;

    private boolean isClosed;

    protected final Slices slices;


    protected AbstractSSTableIterator(SSTableReader sstable,
                                      FileDataInput file,
                                      RowIndexEntry indexEntry,
                                      DecoratedKey key,
                                      Slices slices,
                                      ColumnFilter columnFilter,
                                      DeletionTime partitionLevelDeletion,
                                      Row staticRow)
    {
        this.sstable = sstable;
        this.metadata = sstable.metadata();
        this.key = key;
        this.columns = columnFilter;
        this.slices = slices;
        this.helper = new SerializationHelper(metadata, sstable.descriptor.version.encodingVersion(), SerializationHelper.Flag.LOCAL, columnFilter);
        this.partitionLevelDeletion = partitionLevelDeletion;
        this.staticRow = staticRow;
        this.reader = createReader(indexEntry, file, false, Rebufferer.ReaderConstraint.IN_CACHE_ONLY);

        try
        {
            if (reader != null && !slices.isEmpty())
                reader.setForSlice(nextSlice());
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            String filePath = file.getPath();

            throw new CorruptSSTableException(e, filePath);
        }
    }

    @SuppressWarnings("resource") // We need this because the analysis is not able to determine that we do close
                                  // file on every path where we created it.
    protected AbstractSSTableIterator(SSTableReader sstable,
                                      FileDataInput dataFileInput,
                                      DecoratedKey key,
                                      RowIndexEntry indexEntry,
                                      Slices slices,
                                      ColumnFilter columnFilter)
    {
        this.sstable = sstable;
        this.metadata = sstable.metadata();
        this.key = key;
        this.columns = columnFilter;
        this.slices = slices;
        this.helper = new SerializationHelper(metadata, sstable.descriptor.version.encodingVersion(), SerializationHelper.Flag.LOCAL, columnFilter);

        if (indexEntry == null)
        {
            this.partitionLevelDeletion = DeletionTime.LIVE;
            this.reader = null;
            this.staticRow = Rows.EMPTY_STATIC_ROW;
        }
        else
        {
            boolean shouldCloseFile = dataFileInput == null;
            try
            {
                // We seek to the beginning to the partition if either:
                //   - the partition is not indexed; we then have a single block to read anyway
                //     (and we need to read the partition deletion time).
                //   - we're querying static columns.
                boolean needSeekAtPartitionStart = !indexEntry.isIndexed() || !columns.fetchedColumns().statics.isEmpty();

                if (needSeekAtPartitionStart)
                {
                    // Not indexed (or is reading static), set to the beginning of the partition and read partition level deletion there
                    if (dataFileInput == null)
                        dataFileInput = sstable.getFileDataInput(indexEntry.position, Rebufferer.ReaderConstraint.NONE);
                    else
                        dataFileInput.seek(indexEntry.position);

                    ByteBufferUtil.skipShortLength(dataFileInput); // Skip partition key
                    this.partitionLevelDeletion = DeletionTime.serializer.deserialize(dataFileInput);

                    // Note that this needs to be called after file != null and after the partitionDeletion has been set, but before readStaticRow
                    // (since it uses it) so we can't move that up (but we'll be able to simplify as soon as we drop support for the old file format).
                    this.reader = createReader(indexEntry, dataFileInput, shouldCloseFile, Rebufferer.ReaderConstraint.NONE);
                    this.staticRow = readStaticRow(sstable, dataFileInput, helper, columns.fetchedColumns().statics);
                }
                else
                {
                    this.partitionLevelDeletion = indexEntry.deletionTime();
                    this.staticRow = Rows.EMPTY_STATIC_ROW;
                    this.reader = createReader(indexEntry, dataFileInput, shouldCloseFile, Rebufferer.ReaderConstraint.NONE);
                }

                if (!slices.isEmpty())
                    reader.setForSlice(nextSlice());
            }
            catch (IOException e)
            {
                sstable.markSuspect();
                String filePath = dataFileInput.getPath();
                if (shouldCloseFile)
                {
                    try
                    {
                        dataFileInput.close();
                    }
                    catch (IOException suppressed)
                    {
                        e.addSuppressed(suppressed);
                    }
                }
                throw new CorruptSSTableException(e, filePath);
            }
        }
    }

    private Slice nextSlice()
    {
        return slices.get(nextSliceIndex());
    }

    private Slice currentSlice()
    {
        return slices.get(currentSliceIndex());
    }

    /**
     * Returns the index of the next slice to process.
     * @return the index of the next slice to process
     */
    protected abstract int nextSliceIndex();

    protected abstract int currentSliceIndex();

    /**
     * Checks if there are more slice to process.
     * @return {@code true} if there are more slice to process, {@code false} otherwise.
     */
    protected abstract boolean hasMoreSlices();

    private static Row readStaticRow(SSTableReader sstable,
                                     FileDataInput file,
                                     SerializationHelper helper,
                                     Columns statics) throws IOException
    {
        if (!sstable.header.hasStatic())
            return Rows.EMPTY_STATIC_ROW;

        if (statics.isEmpty())
        {
            UnfilteredSerializer.serializers.get(helper.version).skipStaticRow(file, sstable.header, helper);
            return Rows.EMPTY_STATIC_ROW;
        }
        else
        {
            return UnfilteredSerializer.serializers.get(helper.version).deserializeStaticRow(file, sstable.header, helper);
        }
    }

    protected abstract Reader createReaderInternal(RowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile, Rebufferer.ReaderConstraint rc);

    private Reader createReader(RowIndexEntry indexEntry, FileDataInput file, boolean shouldCloseFile, Rebufferer.ReaderConstraint rc)
    {
        return slices.isEmpty() ? new NoRowsReader(file, shouldCloseFile)
                                : createReaderInternal(indexEntry, file, shouldCloseFile, rc);
    }

    public void resetReaderState()
    {
        assert reader != null;
        reader.resetState();
        if (!slices.isEmpty())
        {
            try
            {
                reader.setForSlice(currentSlice());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public TableMetadata metadata()
    {
        return metadata;
    }

    public RegularAndStaticColumns columns()
    {
        return columns.fetchedColumns();
    }

    public DecoratedKey partitionKey()
    {
        return key;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public Row staticRow()
    {
        return staticRow;
    }

    public EncodingStats stats()
    {
        return sstable.stats();
    }

    public boolean hasNext()
    {
        while (true)
        {
            if (reader == null)
                return false;

            if (reader.hasNext())
                return true;

            if (!hasMoreSlices())
                return false;

            slice(nextSlice());
        }
    }

    public Unfiltered next()
    {
        assert reader != null;
        return reader.next();
    }

    private void slice(Slice slice)
    {
        try
        {
            if (reader != null)
                reader.setForSlice(slice);
        }
        catch (IOException e)
        {
            try
            {
                closeInternal();
            }
            catch (IOException suppressed)
            {
                e.addSuppressed(suppressed);
            }
            sstable.markSuspect();
            throw new CorruptSSTableException(e, reader.file.getPath());
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private void closeInternal() throws IOException
    {
        // It's important to make closing idempotent since it would bad to double-close 'file' as its a RandomAccessReader
        // and its close is not idemptotent in the case where we recycle it.
        if (isClosed)
            return;

        if (reader != null)
            reader.close();

        isClosed = true;
    }

    public void close()
    {
        try
        {
            closeInternal();
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, reader.file.getPath());
        }
    }

    public abstract class Reader implements Iterator<Unfiltered>
    {
        private final boolean shouldCloseFile;
        public FileDataInput file;
        protected long filePos = -1;

        public UnfilteredDeserializer deserializer;

        // Records the currently open range tombstone (if any)
        public DeletionTime openMarker = null;
        protected DeletionTime lastOpenMarker = null;

        protected Reader(FileDataInput file, boolean shouldCloseFile)
        {
            this.file = file;
            this.shouldCloseFile = shouldCloseFile;

            if (file != null)
                createDeserializer();
        }

        protected void createDeserializer()
        {
            assert file != null && deserializer == null;
            deserializer = UnfilteredDeserializer.create(metadata, file, sstable.header, helper);
        }

        /**
         * Resets the state to the last known finished item
         * This is needed to handle async retries - due to missing data in the chunk cache.
         * Classes that override this must call the base class too.
         */
         protected void resetState()
         {
             deserializer.clearState();
             openMarker = lastOpenMarker;
         }

        public void seekToPosition(long position) throws IOException
        {
            // This may be the first time we're actually looking into the file
            // Note: file can't be null in rx read path
            if (file == null)
            {
                file = sstable.getFileDataInput(position, Rebufferer.ReaderConstraint.NONE);
                createDeserializer();
            }
            else
            {
                filePos = position;
                file.seek(position);
                deserializer.clearState();
            }
        }

        protected void updateOpenMarker(RangeTombstoneMarker marker)
        {
            lastOpenMarker = null;
            // Note that we always read index blocks in forward order so this method is always called in forward order
            openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
        }

        protected DeletionTime getAndClearOpenMarker()
        {
            DeletionTime toReturn = openMarker;
            lastOpenMarker = openMarker;
            openMarker = null;
            return toReturn;
        }

        public boolean hasNext()
        {
            try
            {
                return hasNextInternal();
            }
            catch (IOException | IndexOutOfBoundsException e)
            {
                try
                {
                    closeInternal();
                }
                catch (IOException suppressed)
                {
                    e.addSuppressed(suppressed);
                }
                sstable.markSuspect();
                throw new CorruptSSTableException(e, reader.file.getPath());
            }
        }

        public Unfiltered next()
        {
            try
            {
                return nextInternal();
            }
            catch (IOException e)
            {
                try
                {
                    closeInternal();
                }
                catch (IOException suppressed)
                {
                    e.addSuppressed(suppressed);
                }
                sstable.markSuspect();
                throw new CorruptSSTableException(e, reader.file.getPath());
            }
        }

        // Set the reader so its hasNext/next methods return values within the provided slice
        public abstract void setForSlice(Slice slice) throws IOException;

        protected abstract boolean hasNextInternal() throws IOException;
        protected abstract Unfiltered nextInternal() throws IOException;

        public void close() throws IOException
        {
            if (shouldCloseFile && file != null)
                file.close();
        }
    }

    // Reader for when we have Slices.NONE but need to read static row or partition level deletion
    private class NoRowsReader extends AbstractSSTableIterator.Reader
    {
        private NoRowsReader(FileDataInput file, boolean shouldCloseFile)
        {
            super(file, shouldCloseFile);
        }

        public void setForSlice(Slice slice) throws IOException
        {
            return;
        }

        protected boolean hasNextInternal() throws IOException
        {
            return false;
        }

        protected Unfiltered nextInternal() throws IOException
        {
            throw new NoSuchElementException();
        }

        protected void resetState()
        {

        }
    }
}
