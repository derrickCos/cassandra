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

package org.apache.cassandra.index.sai.disk.v2;

import java.io.IOException;
import java.util.function.BooleanSupplier;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;

public class SSTableIndexWriter extends org.apache.cassandra.index.sai.disk.v1.SSTableIndexWriter
{
    public SSTableIndexWriter(IndexDescriptor indexDescriptor, IndexContext indexContext, NamedMemoryLimiter limiter,
                              BooleanSupplier isIndexValid)
    {
        super(indexDescriptor, indexContext, limiter, isIndexValid);
    }

    protected void writeSegmentsMetadata() throws IOException
    {
        if (segments.isEmpty())
            return;

        assert segments.size() == 1 : "A post-compacted index should only contain a single segment";

        try
        {
            indexDescriptor.newIndexMetadataSerializer().serialize(new V2IndexOnDiskMetadata(segments.get(0)), indexDescriptor, indexContext);
        }
        catch (IOException e)
        {
            abort(e);
            throw e;
        }
    }
}
