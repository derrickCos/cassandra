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

package org.apache.cassandra.index;

import org.apache.cassandra.exceptions.UncheckedInternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;

/**
 * Thrown if a secondary index is not currently available.
 */
public final class IndexNotAvailableException extends UncheckedInternalRequestExecutionException
{
    /**
     * Creates a new <code>IndexNotAvailableException</code> for the specified index.
     * @param index the index
     */
    public IndexNotAvailableException(Index index)
    {
        super(RequestFailureReason.INDEX_NOT_AVAILABLE,
              String.format("The secondary index '%s' is not yet available", index.getIndexMetadata().name));
    }
}
