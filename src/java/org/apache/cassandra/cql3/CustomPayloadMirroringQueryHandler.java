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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Map;

import io.reactivex.Observable;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.MD5Digest;

/**
 * Custom QueryHandler that sends custom request payloads back with the result.
 * Used to facilitate testing.
 * Enabled with system property cassandra.custom_query_handler_class.
 */
public class CustomPayloadMirroringQueryHandler implements QueryHandler
{
    static QueryProcessor queryProcessor = QueryProcessor.instance;

    public Observable<ResultMessage> process(String query,
                                 QueryState state,
                                 QueryOptions options,
                                 Map<String, ByteBuffer> customPayload,
                                 long queryStartNanoTime)
    {
        return queryProcessor.process(query, state, options, customPayload, queryStartNanoTime)
                             .map(result -> {
                                 result.setCustomPayload(customPayload);
                                 return result;
                             });
    }

    public Observable<ResultMessage.Prepared> prepare(String query, QueryState state, Map<String, ByteBuffer> customPayload)
    {
        Observable<ResultMessage.Prepared> observable = queryProcessor.prepare(query, state, customPayload);
        observable.map(prepared -> {
            prepared.setCustomPayload(customPayload);
            return prepared;
        });
        return observable;
    }

    public ParsedStatement.Prepared getPrepared(MD5Digest id)
    {
        return queryProcessor.getPrepared(id);
    }

    public ParsedStatement.Prepared getPreparedForThrift(Integer id)
    {
        return queryProcessor.getPreparedForThrift(id);
    }

    public Observable<ResultMessage> processPrepared(CQLStatement statement,
                                         QueryState state,
                                         QueryOptions options,
                                         Map<String, ByteBuffer> customPayload,
                                         long queryStartNanoTime)
    {
        return queryProcessor.processPrepared(statement, state, options, customPayload, queryStartNanoTime)
                             .map(result -> {
                                 result.setCustomPayload(customPayload);
                                 return result;
                             });
    }

    public Observable<ResultMessage> processBatch(BatchStatement statement,
                                      QueryState state,
                                      BatchQueryOptions options,
                                      Map<String, ByteBuffer> customPayload,
                                      long queryStartNanoTime)
    {
        return queryProcessor.processBatch(statement, state, options, customPayload, queryStartNanoTime)
                             .map(result -> {
                                 result.setCustomPayload(customPayload);
                                 return result;
                             });
    }
}
