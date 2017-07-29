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

import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;

public abstract class RoleManagementStatement extends AuthenticationStatement
{
    protected final RoleResource role;
    protected final RoleResource grantee;

    public RoleManagementStatement(RoleName name, RoleName grantee)
    {
        this.role = RoleResource.role(name.getName());
        this.grantee = RoleResource.role(grantee.getName());
    }

    public void checkAccess(QueryState state) throws UnauthorizedException
    {
        try
        {
            super.checkPermission(state, CorePermission.AUTHORIZE, role);
        }
        catch (UnauthorizedException noAuthorizePermission)
        {
            if (!state.hasGrantOption(CorePermission.AUTHORIZE, role))
                throw noAuthorizePermission;
        }
    }

    public void validate(QueryState state) throws RequestValidationException
    {
        state.ensureNotAnonymous();

        if (!DatabaseDescriptor.getRoleManager().isExistingRole(role))
            throw new InvalidRequestException(String.format("%s doesn't exist", role.getRoleName()));

        if (!DatabaseDescriptor.getRoleManager().isExistingRole(grantee))
            throw new InvalidRequestException(String.format("%s doesn't exist", grantee.getRoleName()));
    }
}
