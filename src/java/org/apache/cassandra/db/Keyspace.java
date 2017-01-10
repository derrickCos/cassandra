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
package org.apache.cassandra.db;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import io.reactivex.*;
import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NettyRxScheduler;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.TPCOpOrder;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.view.ViewManager;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.metrics.KeyspaceMetrics;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * It represents a Keyspace.
 */
public class Keyspace
{
    private static final Logger logger = LoggerFactory.getLogger(Keyspace.class);

    private static final String TEST_FAIL_WRITES_KS = System.getProperty("cassandra.test.fail_writes_ks", "");
    private static final boolean TEST_FAIL_WRITES = !TEST_FAIL_WRITES_KS.isEmpty();
    private static int TEST_FAIL_MV_LOCKS_COUNT = Integer.getInteger("cassandra.test.fail_mv_locks_count", 0);

    public final KeyspaceMetrics metric;

    // It is possible to call Keyspace.open without a running daemon, so it makes sense to ensure
    // proper directories here as well as in CassandraDaemon.
    static
    {
        if (DatabaseDescriptor.isDaemonInitialized() || DatabaseDescriptor.isToolInitialized())
            DatabaseDescriptor.createAllDirectories();
    }

    private volatile KeyspaceMetadata metadata;

    //OpOrder is defined globally since we need to order writes across
    //Keyspaces in the case of Views (batchlog of view mutations)
    public static final OpOrder writeOrder = new OpOrder();

    /* ColumnFamilyStore per column family */
    private final ConcurrentMap<UUID, ColumnFamilyStore> columnFamilyStores = new ConcurrentHashMap<>();
    private volatile AbstractReplicationStrategy replicationStrategy;
    public final ViewManager viewManager;

    public static final Function<String,Keyspace> keyspaceTransformer = new Function<String, Keyspace>()
    {
        public Keyspace apply(String keyspaceName)
        {
            return Keyspace.open(keyspaceName);
        }
    };

    private static volatile boolean initialized = false;

    public static void setInitialized()
    {
        initialized = true;
    }

    public static Keyspace open(String keyspaceName)
    {
        assert initialized || SchemaConstants.isSystemKeyspace(keyspaceName);
        return open(keyspaceName, Schema.instance, true);
    }

    // to only be used by org.apache.cassandra.tools.Standalone* classes
    public static Keyspace openWithoutSSTables(String keyspaceName)
    {
        return open(keyspaceName, Schema.instance, false);
    }

    private static Keyspace open(String keyspaceName, Schema schema, boolean loadSSTables)
    {
        Keyspace keyspaceInstance = schema.getKeyspaceInstance(keyspaceName);

        if (keyspaceInstance == null)
        {
            // instantiate the Keyspace.  we could use putIfAbsent but it's important to making sure it is only done once
            // per keyspace, so we synchronize and re-check before doing it.
            synchronized (Keyspace.class)
            {
                keyspaceInstance = schema.getKeyspaceInstance(keyspaceName);
                if (keyspaceInstance == null)
                {
                    // open and store the keyspace
                    keyspaceInstance = new Keyspace(keyspaceName, loadSSTables);
                    schema.storeKeyspaceInstance(keyspaceInstance);
                }
            }
        }
        return keyspaceInstance;
    }

    public static Keyspace clear(String keyspaceName)
    {
        return clear(keyspaceName, Schema.instance);
    }

    public static Keyspace clear(String keyspaceName, Schema schema)
    {
        synchronized (Keyspace.class)
        {
            Keyspace t = schema.removeKeyspaceInstance(keyspaceName);
            if (t != null)
            {
                for (ColumnFamilyStore cfs : t.getColumnFamilyStores())
                    t.unloadCf(cfs);
                t.metric.release();
            }
            return t;
        }
    }

    public static ColumnFamilyStore openAndGetStore(CFMetaData cfm)
    {
        return open(cfm.ksName).getColumnFamilyStore(cfm.cfId);
    }

    /**
     * Removes every SSTable in the directory from the appropriate Tracker's view.
     * @param directory the unreadable directory, possibly with SSTables in it, but not necessarily.
     */
    public static void removeUnreadableSSTables(File directory)
    {
        for (Keyspace keyspace : Keyspace.all())
        {
            for (ColumnFamilyStore baseCfs : keyspace.getColumnFamilyStores())
            {
                for (ColumnFamilyStore cfs : baseCfs.concatWithIndexes())
                    cfs.maybeRemoveUnreadableSSTables(directory);
            }
        }
    }

    public void setMetadata(KeyspaceMetadata metadata)
    {
        this.metadata = metadata;
        createReplicationStrategy(metadata);
    }

    public KeyspaceMetadata getMetadata()
    {
        return metadata;
    }

    public Collection<ColumnFamilyStore> getColumnFamilyStores()
    {
        return Collections.unmodifiableCollection(columnFamilyStores.values());
    }

    public ColumnFamilyStore getColumnFamilyStore(String cfName)
    {
        UUID id = Schema.instance.getId(getName(), cfName);
        if (id == null)
            throw new IllegalArgumentException(String.format("Unknown keyspace/cf pair (%s.%s)", getName(), cfName));
        return getColumnFamilyStore(id);
    }

    public ColumnFamilyStore getColumnFamilyStore(UUID id)
    {
        ColumnFamilyStore cfs = columnFamilyStores.get(id);
        if (cfs == null)
            throw new IllegalArgumentException("Unknown CF " + id);
        return cfs;
    }

    public boolean hasColumnFamilyStore(UUID id)
    {
        return columnFamilyStores.containsKey(id);
    }

    /**
     * Take a snapshot of the specific column family, or the entire set of column families
     * if columnFamily is null with a given timestamp
     *
     * @param snapshotName     the tag associated with the name of the snapshot.  This value may not be null
     * @param columnFamilyName the column family to snapshot or all on null
     * @param skipFlush Skip blocking flush of memtable
     * @throws IOException if the column family doesn't exist
     */
    public void snapshot(String snapshotName, String columnFamilyName, boolean skipFlush) throws IOException
    {
        assert snapshotName != null;
        boolean tookSnapShot = false;
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
        {
            if (columnFamilyName == null || cfStore.name.equals(columnFamilyName))
            {
                tookSnapShot = true;
                cfStore.snapshot(snapshotName, skipFlush);
            }
        }

        if ((columnFamilyName != null) && !tookSnapShot)
            throw new IOException("Failed taking snapshot. Table " + columnFamilyName + " does not exist.");
    }

    /**
     * Take a snapshot of the specific column family, or the entire set of column families
     * if columnFamily is null with a given timestamp
     *
     * @param snapshotName     the tag associated with the name of the snapshot.  This value may not be null
     * @param columnFamilyName the column family to snapshot or all on null
     * @throws IOException if the column family doesn't exist
     */
    public void snapshot(String snapshotName, String columnFamilyName) throws IOException
    {
        snapshot(snapshotName, columnFamilyName, false);
    }

    /**
     * @param clientSuppliedName may be null.
     * @return the name of the snapshot
     */
    public static String getTimestampedSnapshotName(String clientSuppliedName)
    {
        String snapshotName = Long.toString(System.currentTimeMillis());
        if (clientSuppliedName != null && !clientSuppliedName.equals(""))
        {
            snapshotName = snapshotName + "-" + clientSuppliedName;
        }
        return snapshotName;
    }

    public static String getTimestampedSnapshotNameWithPrefix(String clientSuppliedName, String prefix)
    {
        return prefix + "-" + getTimestampedSnapshotName(clientSuppliedName);
    }

    /**
     * Check whether snapshots already exists for a given name.
     *
     * @param snapshotName the user supplied snapshot name
     * @return true if the snapshot exists
     */
    public boolean snapshotExists(String snapshotName)
    {
        assert snapshotName != null;
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
        {
            if (cfStore.snapshotExists(snapshotName))
                return true;
        }
        return false;
    }

    /**
     * Clear all the snapshots for a given keyspace.
     *
     * @param snapshotName the user supplied snapshot name. It empty or null,
     *                     all the snapshots will be cleaned
     */
    public static void clearSnapshot(String snapshotName, String keyspace)
    {
        List<File> snapshotDirs = Directories.getKSChildDirectories(keyspace, ColumnFamilyStore.getInitialDirectories());
        Directories.clearSnapshot(snapshotName, snapshotDirs);
    }

    /**
     * @return A list of open SSTableReaders
     */
    public List<SSTableReader> getAllSSTables(SSTableSet sstableSet)
    {
        List<SSTableReader> list = new ArrayList<>(columnFamilyStores.size());
        for (ColumnFamilyStore cfStore : columnFamilyStores.values())
            Iterables.addAll(list, cfStore.getSSTables(sstableSet));
        return list;
    }

    private Keyspace(String keyspaceName, boolean loadSSTables)
    {
        metadata = Schema.instance.getKSMetaData(keyspaceName);
        assert metadata != null : "Unknown keyspace " + keyspaceName;
        createReplicationStrategy(metadata);

        this.metric = new KeyspaceMetrics(this);
        this.viewManager = new ViewManager(this);
        for (CFMetaData cfm : metadata.tablesAndViews())
        {
            logger.trace("Initializing {}.{}", getName(), cfm.cfName);
            initCf(cfm, loadSSTables);
        }
        this.viewManager.reload();
    }

    private Keyspace(KeyspaceMetadata metadata)
    {
        this.metadata = metadata;
        createReplicationStrategy(metadata);
        this.metric = new KeyspaceMetrics(this);
        this.viewManager = new ViewManager(this);
    }

    public static Keyspace mockKS(KeyspaceMetadata metadata)
    {
        return new Keyspace(metadata);
    }

    private void createReplicationStrategy(KeyspaceMetadata ksm)
    {
        replicationStrategy = AbstractReplicationStrategy.createReplicationStrategy(ksm.name,
                                                                                    ksm.params.replication.klass,
                                                                                    StorageService.instance.getTokenMetadata(),
                                                                                    DatabaseDescriptor.getEndpointSnitch(),
                                                                                    ksm.params.replication.options);
    }

    // best invoked on the compaction mananger.
    public void dropCf(UUID cfId)
    {
        assert columnFamilyStores.containsKey(cfId);
        ColumnFamilyStore cfs = columnFamilyStores.remove(cfId);
        if (cfs == null)
            return;

        cfs.getCompactionStrategyManager().shutdown();
        CompactionManager.instance.interruptCompactionForCFs(cfs.concatWithIndexes(), true);
        // wait for any outstanding reads/writes that might affect the CFS
        cfs.keyspace.writeOrder.awaitNewBarrier();
        cfs.readOrdering.awaitNewBarrier();

        unloadCf(cfs);
    }

    // disassociate a cfs from this keyspace instance.
    private void unloadCf(ColumnFamilyStore cfs)
    {
        cfs.forceBlockingFlush();
        cfs.invalidate();
    }

    /**
     * Registers a custom cf instance with this keyspace.
     * This is required for offline tools what use non-standard directories.
     */
    public void initCfCustom(ColumnFamilyStore newCfs)
    {
        ColumnFamilyStore cfs = columnFamilyStores.get(newCfs.metadata.cfId);

        if (cfs == null)
        {
            // CFS being created for the first time, either on server startup or new CF being added.
            // We don't worry about races here; startup is safe, and adding multiple idential CFs
            // simultaneously is a "don't do that" scenario.
            ColumnFamilyStore oldCfs = columnFamilyStores.putIfAbsent(newCfs.metadata.cfId, newCfs);
            // CFS mbean instantiation will error out before we hit this, but in case that changes...
            if (oldCfs != null)
                throw new IllegalStateException("added multiple mappings for cf id " + newCfs.metadata.cfId);
        }
        else
        {
            throw new IllegalStateException("CFS is already initialized: " + cfs.name);
        }
    }

    /**
     * adds a cf to internal structures, ends up creating disk files).
     */
    public void initCf(CFMetaData metadata, boolean loadSSTables)
    {
        ColumnFamilyStore cfs = columnFamilyStores.get(metadata.cfId);

        if (cfs == null)
        {
            // CFS being created for the first time, either on server startup or new CF being added.
            // We don't worry about races here; startup is safe, and adding multiple idential CFs
            // simultaneously is a "don't do that" scenario.
            ColumnFamilyStore oldCfs = columnFamilyStores.putIfAbsent(metadata.cfId, ColumnFamilyStore.createColumnFamilyStore(this, metadata, loadSSTables));
            // CFS mbean instantiation will error out before we hit this, but in case that changes...
            if (oldCfs != null)
                throw new IllegalStateException("added multiple mappings for cf id " + metadata.cfId);
        }
        else
        {
            // re-initializing an existing CF.  This will happen if you cleared the schema
            // on this node and it's getting repopulated from the rest of the cluster.
            assert cfs.name.equals(metadata.cfName);
            cfs.reload();
        }
    }

    public Completable apply(final Mutation mutation, final boolean writeCommitLog)
    {
        return apply(mutation, writeCommitLog, true, true);
    }

    // TODO can probably combine this with applyInternal with the same signature
    /**
     * If apply is blocking, apply must not be deferred
     * Otherwise there is a race condition where ALL mutation workers are beeing blocked ending
     * in a complete deadlock of the mutation stage. See CASSANDRA-12689.
     *
     * @param mutation       the row to write.  Must not be modified after calling apply, since commitlog append
     *                       may happen concurrently, depending on the CL Executor type.
     * @param writeCommitLog false to disable commitlog append entirely
     * @param updateIndexes  false to disable index updates (used by CollationController "defragmenting")
     * @param isDroppable    true if this should throw WriteTimeoutException if it does not acquire lock within write_request_timeout_in_ms
     * @throws ExecutionException
     */
    public Completable apply(final Mutation mutation, final boolean writeCommitLog, boolean updateIndexes, boolean isDroppable)
    {
        return applyInternal(mutation, writeCommitLog, updateIndexes, isDroppable);
    }


    /**
     * This method appends a row to the global CommitLog, then updates memtables and indexes.
     *
     * @param mutation       the row to write.  Must not be modified after calling apply, since commitlog append
     *                       may happen concurrently, depending on the CL Executor type.
     * @param writeCommitLog false to disable commitlog append entirely
     * @param updateIndexes  false to disable index updates (used by CollationController "defragmenting")
     * @param isDroppable    true if this should throw WriteTimeoutException if it does not acquire lock within write_request_timeout_in_ms
     */
    private Completable applyInternal(final Mutation mutation, final boolean writeCommitLog, boolean updateIndexes, boolean isDroppable)
    {
        if (TEST_FAIL_WRITES && metadata.name.equals(TEST_FAIL_WRITES_KS))
            return Completable.error(new RuntimeException("Testing write failures"));

        boolean requiresViewUpdate = updateIndexes && viewManager.updatesAffectView(Collections.singleton(mutation), false);

        final Lock[] locks = requiresViewUpdate ? new Lock[mutation.getColumnFamilyIds().size()] : null;
        return Completable.defer(() ->
        {
            if (requiresViewUpdate)
            {
                if (mutation.viewLockAcquireStart == 0)
                    mutation.viewLockAcquireStart = System.currentTimeMillis();

                // the order of lock acquisition doesn't matter (from a deadlock perspective) because we only use tryLock()
                Collection<UUID> columnFamilyIds = mutation.getColumnFamilyIds();
                Iterator<UUID> idIterator = columnFamilyIds.iterator();

                for (int i = 0; i < columnFamilyIds.size(); i++)
                {
                    UUID cfid = idIterator.next();
                    int lockKey = Objects.hash(mutation.key().getKey(), cfid);
                    Lock lock = null;

                    if (TEST_FAIL_MV_LOCKS_COUNT == 0)
                        lock = ViewManager.acquireLockFor(lockKey);
                    else
                        TEST_FAIL_MV_LOCKS_COUNT--;

                    if (lock == null)
                    {
                        //throw WTE only if request is droppable
                        if (isDroppable && (System.currentTimeMillis() - mutation.createdAt) > DatabaseDescriptor.getWriteRpcTimeout())
                        {
                            for (int j = 0; j < i; j++)
                                locks[j].unlock();

                            logger.trace("Could not acquire lock for {} and table {}", ByteBufferUtil.bytesToHex(mutation.key().getKey()), columnFamilyStores.get(cfid).name);
                            Tracing.trace("Could not acquire MV lock");
                            return Completable.error(new WriteTimeoutException(WriteType.VIEW, ConsistencyLevel.LOCAL_ONE, 0, 1));
                        }
                        else
                        {
                            for (int j = 0; j < i; j++)
                                locks[j].unlock();

                            // This view update can't happen right now, so schedule another attempt later.
                            return Completable.defer(() -> applyInternal(mutation, writeCommitLog, true, isDroppable))
                                    .subscribeOn(NettyRxScheduler.instance());
                        }
                    }
                    else
                    {
                        locks[i] = lock;
                    }
                }

                long acquireTime = System.currentTimeMillis() - mutation.viewLockAcquireStart;
                // Metrics are only collected for droppable write operations
                // Bulk non-droppable operations (e.g. commitlog replay, hint delivery) are not measured
                if (isDroppable)
                {
                    for(UUID cfid : columnFamilyIds)
                        columnFamilyStores.get(cfid).metric.viewLockAcquireTime.update(acquireTime, TimeUnit.MILLISECONDS);
                }
            }
            int nowInSec = FBUtilities.nowInSeconds();

            // write the mutation to the commitlog
            Single<CommitLogPosition> commitLogPositionObservable;
            if (writeCommitLog)
            {
                Tracing.trace("Appending to commitlog");
                commitLogPositionObservable = CommitLog.instance.add(mutation);
            }
            else
            {
                commitLogPositionObservable = Single.just(CommitLogPosition.NONE);
            }

            TPCOpOrder.Group opGroup = writeOrder.start();

            return commitLogPositionObservable
                   .flatMapCompletable(commitLogPosition -> {

                        List<Completable> memtablePutCompletables = new ArrayList<>(mutation.getPartitionUpdates().size());

                        for (PartitionUpdate upd : mutation.getPartitionUpdates())
                        {

                            ColumnFamilyStore cfs = columnFamilyStores.get(upd.metadata().cfId);
                            if (cfs == null)
                            {
                                logger.error("Attempting to mutate non-existant table {} ({}.{})", upd.metadata().cfId, upd.metadata().ksName, upd.metadata().cfName);
                                continue;
                            }

                            // TODO this probably doesn't need to be atomic after TPC
                            AtomicLong baseComplete = new AtomicLong(Long.MAX_VALUE);

                            Completable viewUpdateCompletable = null;
                            if (requiresViewUpdate)
                            {
                                Tracing.trace("Creating materialized view mutations from base table replica");
                                viewUpdateCompletable = viewManager.forTable(upd.metadata()).pushViewReplicaUpdates(upd, writeCommitLog, baseComplete);
                                viewUpdateCompletable.doOnError(exc -> {
                                        JVMStabilityInspector.inspectThrowable(exc);
                                        logger.error(String.format("Unknown exception caught while attempting to update MaterializedView! %s.%s",
                                                upd.metadata().ksName, upd.metadata().cfName), exc);
                                });
                            }

                            Tracing.trace("Adding to {} memtable", upd.metadata().cfName);
                            UpdateTransaction indexTransaction = updateIndexes
                                                                 ? cfs.indexManager.newUpdateTransaction(upd, opGroup, nowInSec)
                                                                 : UpdateTransaction.NO_OP;

                            CommitLogPosition pos = commitLogPosition == CommitLogPosition.NONE ? null : commitLogPosition;
                            Completable memtableCompletable = cfs.apply(upd, indexTransaction, opGroup, pos);
                            if (requiresViewUpdate)
                            {
                                memtableCompletable.doOnComplete(() -> baseComplete.set(System.currentTimeMillis()));
                                memtablePutCompletables.add(viewUpdateCompletable);
                            }
                            memtablePutCompletables.add(memtableCompletable);
                        }

                        // avoid the expensive merge call if there's only 1 observable
                        if (memtablePutCompletables.size() == 1)
                            return memtablePutCompletables.get(0);
                        else
                            return Completable.merge(memtablePutCompletables);

                   }).doOnComplete(opGroup::close);
        })
        .doFinally(() -> {
            if (locks != null)
            {
                for (Lock lock : locks)
                    lock.unlock();
            }
        })
        //Route the work to the correct core
        .subscribeOn(NettyRxScheduler.getForKey(mutation.getKeyspaceName(), mutation.key(), false));
    }

    public AbstractReplicationStrategy getReplicationStrategy()
    {
        return replicationStrategy;
    }

    public List<Single<CommitLogPosition>> flush()
    {
        List<Single<CommitLogPosition>> futures = new ArrayList<>(columnFamilyStores.size());
        for (ColumnFamilyStore cfs : columnFamilyStores.values())
            futures.add(cfs.forceFlush());
        return futures;
    }

    public Iterable<ColumnFamilyStore> getValidColumnFamilies(boolean allowIndexes,
                                                              boolean autoAddIndexes,
                                                              String... cfNames) throws IOException
    {
        Set<ColumnFamilyStore> valid = new HashSet<>();

        if (cfNames.length == 0)
        {
            // all stores are interesting
            for (ColumnFamilyStore cfStore : getColumnFamilyStores())
            {
                valid.add(cfStore);
                if (autoAddIndexes)
                    valid.addAll(getIndexColumnFamilyStores(cfStore));
            }
            return valid;
        }

        // include the specified stores and possibly the stores of any of their indexes
        for (String cfName : cfNames)
        {
            if (SecondaryIndexManager.isIndexColumnFamily(cfName))
            {
                if (!allowIndexes)
                {
                    logger.warn("Operation not allowed on secondary Index table ({})", cfName);
                    continue;
                }
                String baseName = SecondaryIndexManager.getParentCfsName(cfName);
                String indexName = SecondaryIndexManager.getIndexName(cfName);

                ColumnFamilyStore baseCfs = getColumnFamilyStore(baseName);
                Index index = baseCfs.indexManager.getIndexByName(indexName);
                if (index == null)
                    throw new IllegalArgumentException(String.format("Invalid index specified: %s/%s.",
                                                                     baseCfs.metadata.cfName,
                                                                     indexName));

                if (index.getBackingTable().isPresent())
                    valid.add(index.getBackingTable().get());
            }
            else
            {
                ColumnFamilyStore cfStore = getColumnFamilyStore(cfName);
                valid.add(cfStore);
                if (autoAddIndexes)
                    valid.addAll(getIndexColumnFamilyStores(cfStore));
            }
        }

        return valid;
    }

    private Set<ColumnFamilyStore> getIndexColumnFamilyStores(ColumnFamilyStore baseCfs)
    {
        Set<ColumnFamilyStore> stores = new HashSet<>();
        for (ColumnFamilyStore indexCfs : baseCfs.indexManager.getAllIndexColumnFamilyStores())
        {
            logger.info("adding secondary index table {} to operation", indexCfs.metadata.cfName);
            stores.add(indexCfs);
        }
        return stores;
    }

    public static Iterable<Keyspace> all()
    {
        return Iterables.transform(Schema.instance.getKeyspaces(), keyspaceTransformer);
    }

    public static Iterable<Keyspace> nonSystem()
    {
        return Iterables.transform(Schema.instance.getNonSystemKeyspaces(), keyspaceTransformer);
    }

    public static Iterable<Keyspace> nonLocalStrategy()
    {
        return Iterables.transform(Schema.instance.getNonLocalStrategyKeyspaces(), keyspaceTransformer);
    }

    public static Iterable<Keyspace> system()
    {
        return Iterables.transform(SchemaConstants.SYSTEM_KEYSPACE_NAMES, keyspaceTransformer);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(name='" + getName() + "')";
    }

    public String getName()
    {
        return metadata.name;
    }
}
