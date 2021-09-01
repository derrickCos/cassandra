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

package org.apache.cassandra.index.sai.disk.format;

import java.io.IOException;
import java.util.Set;

import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.ColumnIndexWriter;
import org.apache.cassandra.index.sai.disk.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.PerSSTableComponentsWriter;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.CompressionParams;

public interface OnDiskFormat
{
    public boolean isGroupIndexComplete(IndexDescriptor indexDescriptor);

    public boolean isColumnIndexComplete(IndexDescriptor indexDescriptor, IndexContext indexContext);

    public boolean isCompletionMarker(IndexComponent indexComponent);

    public boolean isEncryptable(IndexComponent indexComponent);

    public PerSSTableComponentsWriter createPerSSTableComponentsWriter(boolean perColumnOnly,
                                                                       IndexDescriptor indexDescriptor,
                                                                       CompressionParams compressionParams) throws IOException;

    public ColumnIndexWriter newIndexWriter(StorageAttachedIndex index,
                                            IndexDescriptor indexDescriptor,
                                            LifecycleNewTracker tracker,
                                            RowMapping rowMapping,
                                            CompressionParams compressionParams);

    public void validatePerSSTableComponent(IndexDescriptor indexDescriptor, IndexComponent indexComponent, boolean checksum) throws IOException;

    public void validatePerIndexComponent(IndexDescriptor indexDescriptor, IndexComponent indexComponent, IndexContext indexContext, boolean checksum) throws IOException;

    public Set<IndexComponent> perSSTableComponents();

    public Set<IndexComponent> perIndexComponents(IndexContext indexContext);

    public PerIndexFiles perIndexFiles(IndexDescriptor indexDescriptor, IndexContext indexContext, boolean temporary);

    public int openPerIndexFiles(boolean isLiteral);

    public SSTableContext newSSTableContext(SSTableReader sstable, IndexDescriptor indexDescriptor);
}
