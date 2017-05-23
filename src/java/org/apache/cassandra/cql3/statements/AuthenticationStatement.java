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
package org.apache.cassandra.cql3.statements;

import io.reactivex.Scheduler;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

public abstract class AuthenticationStatement extends ParsedStatement implements CQLStatement
{
    @Override
    public Prepared prepare()
    {
        return new Prepared(this);
    }

    public int getBoundTerms()
    {
        return 0;
    }

    public Single<? extends ResultMessage> execute(QueryState state, QueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        return execute(state.getClientState());
    }

    public abstract Single<ResultMessage> execute(ClientState state) throws RequestExecutionException, RequestValidationException;

    public Single<? extends ResultMessage> executeInternal(QueryState state, QueryOptions options)
    {
        // executeInternal is for local query only, thus altering users doesn't make sense and is not supported
        return Single.error(new UnsupportedOperationException());
    }

    public Scheduler getScheduler()
    {
        return Schedulers.io();
    }

    public void checkPermission(ClientState state, Permission required, RoleResource resource) throws UnauthorizedException
    {
        try
        {
            state.ensureHasPermission(required, resource);
        }
        catch (UnauthorizedException e)
        {
            // Catch and rethrow with a more friendly message
            throw new UnauthorizedException(String.format("User %s does not have sufficient privileges " +
                                                          "to perform the requested operation",
                                                          state.getUser().getName()));
        }
    }
}

