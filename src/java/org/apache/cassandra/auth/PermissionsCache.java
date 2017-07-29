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
package org.apache.cassandra.auth;

import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;

public class PermissionsCache extends AuthCache<RoleResource, Map<IResource, PermissionSets>> implements PermissionsCacheMBean
{
    public PermissionsCache()
    {
        this(DatabaseDescriptor.getAuthorizer() != null
             ? DatabaseDescriptor.getAuthorizer()
             : new AllowAllAuthorizer());
    }

    public PermissionsCache(IAuthorizer authorizer)
    {
        super("PermissionsCache",
              DatabaseDescriptor::setPermissionsValidity,
              DatabaseDescriptor::getPermissionsValidity,
              DatabaseDescriptor::setPermissionsUpdateInterval,
              DatabaseDescriptor::getPermissionsUpdateInterval,
              DatabaseDescriptor::setPermissionsCacheMaxEntries,
              DatabaseDescriptor::getPermissionsCacheMaxEntries,
              authorizer::allPermissionSets,
              authorizer::requireAuthorization);
    }

    /**
     * Retrieve the permissions against all resources of a specific role.
     * Calls from a TPC thread should be prevented.
     */
    public Map<IResource, PermissionSets> getPermissions(RoleResource role)
    {
        return get(role, "permissions");
    }
}
