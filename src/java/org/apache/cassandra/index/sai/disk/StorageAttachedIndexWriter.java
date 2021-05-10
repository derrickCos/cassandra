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
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.BKDReader;
import org.apache.cassandra.index.sai.disk.v1.BKDRowOrdinalsWriter;
import org.apache.cassandra.index.sai.disk.v1.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.SortedPostingsWriter;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.SortedRow;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Writes all on-disk index structures attached to a given SSTable.
 */
public class StorageAttachedIndexWriter implements SSTableFlushObserver
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Descriptor descriptor;
    private final SortedRow.SortedRowFactory sortedRowFactory;
    private final Collection<StorageAttachedIndex> indices;
    private final Collection<ColumnIndexWriter> columnIndexWriters;
    private final SSTableComponentsWriter sstableComponentsWriter;
    private final Stopwatch stopwatch = Stopwatch.createUnstarted();
    private final RowMapping rowMapping;

    private DecoratedKey currentKey;
    private boolean tokenOffsetWriterCompleted = false;
    private boolean aborted = false;

    private long sstableRowId = 0;

    public StorageAttachedIndexWriter(Descriptor descriptor,
                                      Collection<StorageAttachedIndex> indices,
                                      LifecycleNewTracker tracker,
                                      TableMetadata tableMetadata) throws IOException
    {
        this(descriptor, indices, tracker, false, tableMetadata);
    }

    public StorageAttachedIndexWriter(Descriptor descriptor,
                                      Collection<StorageAttachedIndex> indices,
                                      LifecycleNewTracker tracker,
                                      boolean perColumnOnly,
                                      TableMetadata tableMetadata) throws IOException
    {
        this.descriptor = descriptor;
        this.sortedRowFactory = SortedRow.factory(tableMetadata);
        this.indices = indices;
        this.rowMapping = RowMapping.create(tracker.opType());
        this.columnIndexWriters = indices.stream().map(i -> i.newIndexWriter(descriptor, tracker, rowMapping, tableMetadata.params.compression))
                                         .filter(Objects::nonNull) // a null here means the column had no data to flush
                                         .collect(Collectors.toList());

        this.sstableComponentsWriter = perColumnOnly
                                       ? SSTableComponentsWriter.NONE
                                       : new SSTableComponentsWriter.OnDiskSSTableComponentsWriter(descriptor, tableMetadata.params.compression);
    }

    @Override
    public void begin()
    {
        logger.debug(logMessage("Starting partition iteration for storage attached index flush for SSTable {}..."), descriptor);
        stopwatch.start();
    }

    @Override
    public void startPartition(DecoratedKey key, long position)
    {
        if (aborted) return;
        
        currentKey = key;
    }

    @Override
    public void nextUnfilteredCluster(Unfiltered unfiltered, long position)
    {
        if (aborted) return;
        
        try
        {
            // Ignore range tombstones...
            if (unfiltered.isRow())
            {
                SortedRow sortedRow = sortedRowFactory.createKey(currentKey, ((Row)unfiltered).clustering(), sstableRowId++);
                sstableComponentsWriter.nextRow(sortedRow);
                rowMapping.add(sortedRow);

                for (ColumnIndexWriter w : columnIndexWriters)
                {
                    w.addRow(sortedRow, (Row) unfiltered);
                }
            }
        }
        catch (Throwable t)
        {
            logger.error(logMessage("Failed to record a row during an index build"), t);
            abort(t, true);
        }
    }

    @Override
    public void partitionLevelDeletion(DeletionTime deletionTime, long position)
    {
        // Deletions (including partition deletions) are accounted for during reads.
    }

    @Override
    public void staticRow(Row staticRow, long position)
    {
        if (aborted) return;
        
        if (staticRow.isEmpty())
            return;

        try
        {
            SortedRow sortedRow = sortedRowFactory.createKey(currentKey, staticRow.clustering(), sstableRowId++);
            sstableComponentsWriter.nextRow(sortedRow);
            rowMapping.add(sortedRow);

            for (ColumnIndexWriter w : columnIndexWriters)
            {
                w.addRow(sortedRow, staticRow);
            }
        }
        catch (Throwable t)
        {
            logger.error(logMessage("Failed to record a static row during an index build"), t);
            abort(t, true);
        }
    }

    @Override
    public void complete()
    {
        if (aborted) return;
        
        logger.debug(logMessage("Completed partition iteration for index flush for SSTable {}. Elapsed time: {} ms"),
                     descriptor, stopwatch.elapsed(TimeUnit.MILLISECONDS));

        try
        {
            sstableComponentsWriter.complete();
            tokenOffsetWriterCompleted = true;

            logger.debug(logMessage("Flushed tokens and offsets for SSTable {}. Elapsed time: {} ms."),
                         descriptor, stopwatch.elapsed(TimeUnit.MILLISECONDS));

            rowMapping.complete();

            final Map<String,IndexComponents> indexNames = new LinkedHashMap<>();
            final Map<String,SortedRow.SortedRowFactory> sortedRowFactories = new LinkedHashMap<>();

            for (ColumnIndexWriter columnIndexWriter : columnIndexWriters)
            {
                columnIndexWriter.flush();

                if (!columnIndexWriter.isLiteral()) // only add the index name if it's a string
                {
                    String indexName = columnIndexWriter.indexName();
                    SortedRow.SortedRowFactory sortedRowFactory = columnIndexWriter.sortedRowFactory();

                    assert sortedRowFactory != null;

                    sortedRowFactories.put(indexName, sortedRowFactory);
                    indexNames.put(indexName, columnIndexWriter.indexComponents());
                }
            }

            for (Map.Entry<String,IndexComponents> indexName : indexNames.entrySet())
            {
                final IndexComponents indexComponents = indexName.getValue();
                final MetadataSource source = MetadataSource.loadColumnMetadata(indexComponents);

                for (Map.Entry<String,IndexComponents> indexName2 : indexNames.entrySet())
                {
                    final IndexComponents indexComponents2 = indexName2.getValue();

                    SortedRow.SortedRowFactory sortedRowFactory = sortedRowFactories.get(indexName.getKey());

                    assert sortedRowFactory != null;

                    final BlockPackedReader rowIDToPointIDReader = BKDRowOrdinalsWriter.openReader(indexComponents, source, sortedRowFactory);
                    final LongArray rowIDToPointIDMap = rowIDToPointIDReader.open();

                    final FileHandle kdTreeFile2 = indexComponents.createFileHandle(indexComponents2.kdTree);
                    final FileHandle kdTreePostingsFile2 = indexComponents.createFileHandle(indexComponents2.kdTreePostingLists);

                    final MetadataSource source2 = MetadataSource.loadColumnMetadata(indexComponents2);

                    SegmentMetadata metadata2 = SegmentMetadata.load(source2, SortedRow.factory(), null);

                    long kdTreeIndexRoot2 = metadata2.getIndexRoot(indexComponents.kdTree);
                    final long postingsPosition = metadata2.getIndexRoot(indexComponents.kdTreePostingLists);

                    final BKDReader bkdReader2 = new BKDReader(indexComponents2,
                                                               kdTreeFile2,
                                                               kdTreeIndexRoot2,
                                                               kdTreePostingsFile2,
                                                               postingsPosition,
                    null);

                    String suffix = "sorted_"+indexName2.getKey()+"_asc";

                    IndexComponents.IndexComponent kdTreeOrderMapComp = indexComponents.kdTreeOrderMaps.ndiType.newComponent(indexName.getKey(), suffix);
                    IndexComponents.IndexComponent kdTreePostingsComp = indexComponents.kdTreePostingLists.ndiType.newComponent(indexName.getKey(), suffix);

                    IndexOutputWriter sortedOrderMapOut = indexComponents.createOutput(kdTreeOrderMapComp);

                    final SortedPostingsWriter sortedPostingsWriter = new SortedPostingsWriter(bkdReader2);

                    final long postingsIndexFilePointer = sortedPostingsWriter.finish(() -> {
                                                                                          try
                                                                                          {
                                                                                              return indexComponents.createOutput(kdTreePostingsComp);
                                                                                          }
                                                                                          catch (Exception ex)
                                                                                          {
                                                                                              throw new RuntimeException(ex);
                                                                                          }
                                                                                      },
                                                                                      sortedOrderMapOut,
                                                                                      () -> {
                                                                                          FileHandle handle = indexComponents2.createFileHandle(kdTreePostingsComp);
                                                                                          return indexComponents2.openInput(handle);
                                                                                      },
                                                                                      (rowID -> rowIDToPointIDMap.get(rowID)));
                    rowIDToPointIDMap.close();
                    sortedOrderMapOut.close();

                    bkdReader2.close();

                    IndexComponents.IndexComponent metaComp2 = indexComponents.meta.ndiType.newComponent(indexName.getKey(), suffix);
                    MetadataWriter metaWriter = new MetadataWriter(indexComponents.createOutput(metaComp2));

                    MetadataWriter.Builder builder = metaWriter.builder("sortedPostingsMeta");
                    SortedPostingsMeta meta = new SortedPostingsMeta(postingsIndexFilePointer);
                    meta.write(builder);
                    builder.close();
                    metaWriter.close();
                }
            }
        }
        catch (Throwable t)
        {
            logger.error(logMessage("Failed to complete an index build"), t);
            abort(t, true);
        }
    }

    public static class SortedPostingsMeta
    {
        public final long postingsIndexFilePointer;

        public SortedPostingsMeta(IndexInput input) throws IOException
        {
            postingsIndexFilePointer = input.readLong();
        }

        public SortedPostingsMeta(long postingsIndexFilePointer)
        {
            this.postingsIndexFilePointer = postingsIndexFilePointer;
        }

        public void write(IndexOutput out) throws IOException
        {
            out.writeLong(postingsIndexFilePointer);
        }
    }

    /**
     * Aborts all column index writers and, only if they have not yet completed, SSTable-level component writers.
     * 
     * @param accumulator the initial exception thrown from the failed writer
     */
    @Override
    public void abort(Throwable accumulator)
    {
        abort(accumulator, false);
    }

    /**
     *
     * @param accumulator original cause of the abort
     * @param fromIndex true if the cause of the abort was the index itself, false otherwise
     */
    public void abort(Throwable accumulator, boolean fromIndex)
    {
        // Mark the write aborted, so we can short-circuit any further operations on the component writers.
        aborted = true;
        
        // Make any indexes involved in this transaction non-queryable, as they will likely not match the backing table.
        if (fromIndex)
            indices.forEach(StorageAttachedIndex::makeIndexNonQueryable);
        
        for (ColumnIndexWriter writer : columnIndexWriters)
        {
            try
            {
                writer.abort(accumulator);
            }
            catch (Throwable t)
            {
                if (accumulator != null)
                {
                    accumulator.addSuppressed(t);
                }
            }
        }
        
        if (!tokenOffsetWriterCompleted)
        {
            // If the token/offset files have already been written successfully, they can be reused later. 
            sstableComponentsWriter.abort(accumulator);
        }
    }

    /**
     * A helper method for constructing consistent log messages. This method is different to similar helper
     * methods in that log messages generated in this class are not necessarily related to a single index
     * so the log message is decorated as follows:
     *
     * [ks.tb.*] Log message
     *
     * @param message The raw content of a logging message.
     *
     * @return A log message with the proper keyspace and table name prepended to it.
     */
    public String logMessage(String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.*] %s", descriptor.ksname, descriptor.cfname, message);
    }

}
