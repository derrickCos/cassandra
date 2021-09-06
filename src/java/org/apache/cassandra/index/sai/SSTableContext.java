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
package org.apache.cassandra.index.sai;

import java.io.File;

import com.google.common.base.Objects;

import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.PerIndexFiles;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

/**
 * SSTableContext is created for individual sstable shared across indexes to track per-sstable index files.
 *
 * SSTableContext itself will be released when receiving sstable removed notification, but its shared copies in individual
 * SSTableIndex will be released when in-flight read requests complete.
 */
public class SSTableContext extends SharedCloseableImpl
{
    public final SSTableReader sstable;
    public final IndexDescriptor indexDescriptor;
    public final PrimaryKeyMap.Factory primaryKeyMapFactory;

    protected SSTableContext(SSTableReader sstable, PrimaryKeyMap.Factory primaryKeyMapFactory, IndexDescriptor indexDescriptor, RefCounted.Tidy tidy)
    {
        super(tidy);
        this.sstable = sstable;
        this.indexDescriptor = indexDescriptor;
        this.primaryKeyMapFactory = primaryKeyMapFactory;
    }

    protected SSTableContext(SSTableContext copy)
    {
        super(copy);
        this.sstable = copy.sstable;
        this.indexDescriptor = copy.indexDescriptor;
        this.primaryKeyMapFactory = copy.primaryKeyMapFactory;
    }

    @SuppressWarnings("resource")
    public static SSTableContext create(SSTableReader sstable)
    {
        Ref<? extends SSTableReader> sstableRef = null;
        PrimaryKeyMap.Factory primaryKeyMapFactory = null;
        IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable.descriptor, sstable.metadata());
        try
        {
            sstableRef = sstable.tryRef();

            if (sstableRef == null)
            {
                throw new IllegalStateException("Couldn't acquire reference to the sstable: " + sstable);
            }

            primaryKeyMapFactory = indexDescriptor.newPrimaryKeyMapFactory(sstable);

            Cleanup cleanup = new SSTableContext.Cleanup(primaryKeyMapFactory, sstableRef);

            return new SSTableContext(sstable, primaryKeyMapFactory, indexDescriptor, cleanup);
        }
        catch (Throwable t)
        {
            if (sstableRef != null)
            {
                sstableRef.release();
            }

            t = Throwables.close(t, primaryKeyMapFactory);
            throw Throwables.unchecked(t);
        }
    }

    @Override
    public SSTableContext sharedCopy()
    {
        return new SSTableContext(this);
    }

    /**
     * @return number of open files per {@link SSTableContext} instance
     */
    public int openFilesPerSSTable()
    {
        return indexDescriptor.version.onDiskFormat().openFilesPerSSTable();
    }

    /**
     * @return disk usage of per-sstable index files
     */
    public long diskUsage()
    {
        return indexDescriptor.version.onDiskFormat()
                                      .perSSTableComponents()
                                      .stream()
                                      .map(indexDescriptor::fileFor)
                                      .filter(File::exists)
                                      .mapToLong(File::length)
                                      .sum();
    }

    public PerIndexFiles perIndexFiles(IndexContext indexContext)
    {
        return indexDescriptor.perIndexFiles(indexContext, false);
    }

    @Override
    public String toString()
    {
        return "SSTableContext{" +
               "sstable=" + indexDescriptor.descriptor +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SSTableContext that = (SSTableContext) o;
        return Objects.equal(indexDescriptor.descriptor, that.indexDescriptor.descriptor);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(indexDescriptor.descriptor.hashCode());
    }

    private static class Cleanup implements RefCounted.Tidy
    {
        private final PrimaryKeyMap.Factory primaryKeyMapFactory;
        private final Ref<? extends SSTableReader> sstableRef;

        private Cleanup(PrimaryKeyMap.Factory primaryKeyMapFactory, Ref<? extends SSTableReader> sstableRef)
        {
            this.primaryKeyMapFactory = primaryKeyMapFactory;
            this.sstableRef = sstableRef;
        }

        @Override
        public void tidy()
        {
            Throwable t = sstableRef.ensureReleased(null);
            t = Throwables.close(t, primaryKeyMapFactory);

            Throwables.maybeFail(t);
        }

        @Override
        public String name()
        {
            return null;
        }
    }
}
