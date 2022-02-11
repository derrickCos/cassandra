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
package org.apache.cassandra.cql3.statements.schema;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.statements.RawKeyspaceAwareStatement;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.AbstractType;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.guardrails.Guardrails;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.transport.messages.ResultMessage;

import static java.lang.String.format;
import static java.lang.String.join;

import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.transform;

import static org.apache.cassandra.schema.TableMetadata.Flag;

public abstract class AlterTableStatement extends AlterSchemaStatement
{
    protected final String tableName;

    public AlterTableStatement(String queryString, String keyspaceName, String tableName)
    {
        super(queryString, keyspaceName);
        this.tableName = tableName;
    }

    public Keyspaces apply(Keyspaces schema)
    {
        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);

        TableMetadata table = null == keyspace
                            ? null
                            : keyspace.getTableOrViewNullable(tableName);

        if (null == table)
            throw ire("Table '%s.%s' doesn't exist", keyspaceName, tableName);

        if (table.isView())
            throw ire("Cannot use ALTER TABLE on a materialized view; use ALTER MATERIALIZED VIEW instead");

        return schema.withAddedOrUpdated(apply(keyspace, table));
    }

    public ResultMessage execute(QueryState state, boolean locally)
    {
        return super.execute(state, locally);
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.UPDATED, Target.TABLE, keyspaceName, tableName);
    }

    public void authorize(ClientState client)
    {
        client.ensureTablePermission(keyspaceName, tableName, Permission.ALTER);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.ALTER_TABLE, keyspaceName, tableName);
    }

    public String toString()
    {
        return format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, tableName);
    }

    abstract KeyspaceMetadata apply(KeyspaceMetadata keyspace, TableMetadata table);

    /**
     * ALTER TABLE <table> ALTER <column> TYPE <newtype>;
     *
     * No longer supported.
     */
    public static class AlterColumn extends AlterTableStatement
    {
        AlterColumn(String queryString, String keyspaceName, String tableName)
        {
            super(queryString, keyspaceName, tableName);
        }

        public KeyspaceMetadata apply(KeyspaceMetadata keyspace, TableMetadata table)
        {
            throw ire("Altering column types is no longer supported");
        }
    }

    /**
     * ALTER TABLE <table> ADD <column> <newtype>
     * ALTER TABLE <table> ADD (<column> <newtype>, <column1> <newtype1>, ... <columnn> <newtypen>)
     */
    private static class AddColumns extends AlterTableStatement
    {
        private static class Column
        {
            private final ColumnIdentifier name;
            private final CQL3Type.Raw type;
            private final boolean isStatic;

            Column(ColumnIdentifier name, CQL3Type.Raw type, boolean isStatic)
            {
                this.name = name;
                this.type = type;
                this.isStatic = isStatic;
            }

        }

        private final Collection<Column> newColumns;
        private QueryState queryState;

        @Override
        public void validate(QueryState state)
        {
            super.validate(state);

            // save the query state to use it for guardrails validation in #apply
            this.queryState = state;
        }

        private AddColumns(String queryString, String keyspaceName, String tableName, Collection<Column> newColumns)
        {
            super(queryString, keyspaceName, tableName);
            this.newColumns = newColumns;
        }

        public KeyspaceMetadata apply(KeyspaceMetadata keyspace, TableMetadata table)
        {
            TableMetadata.Builder tableBuilder = table.unbuild();
            Views.Builder viewsBuilder = keyspace.views.unbuild();
            newColumns.forEach(c -> addColumn(keyspace, table, c, tableBuilder, viewsBuilder));
            TableMetadata tableMetadata = tableBuilder.build();
            tableMetadata.validate();

            Guardrails.columnsPerTable.guard(tableBuilder.numColumns(), tableName, false, queryState);

            return keyspace.withSwapped(keyspace.tables.withSwapped(tableMetadata))
                           .withSwapped(viewsBuilder.build());
        }

        private void addColumn(KeyspaceMetadata keyspace,
                               TableMetadata table,
                               Column column,
                               TableMetadata.Builder tableBuilder,
                               Views.Builder viewsBuilder)
        {
            ColumnIdentifier name = column.name;
            AbstractType<?> type = column.type.prepare(keyspaceName, keyspace.types).getType();
            boolean isStatic = column.isStatic;

            if (null != tableBuilder.getColumn(name))
                throw ire("Column with name '%s' already exists", name);

            if (table.isCompactTable())
                throw ire("Cannot add new column to a COMPACT STORAGE table");

            if (isStatic && table.clusteringColumns().isEmpty())
                throw ire("Static columns are only useful (and thus allowed) if the table has at least one clustering column");

            ColumnMetadata droppedColumn = table.getDroppedColumn(name.bytes);
            if (null != droppedColumn)
            {
                // After #8099, not safe to re-add columns of incompatible types - until *maybe* deser logic with dropped
                // columns is pushed deeper down the line. The latter would still be problematic in cases of schema races.
                if (!type.isValueCompatibleWith(droppedColumn.type))
                {
                    throw ire("Cannot re-add previously dropped column '%s' of type %s, incompatible with previous type %s",
                              name,
                              type.asCQL3Type(),
                              droppedColumn.type.asCQL3Type());
                }

                if (droppedColumn.isStatic() != isStatic)
                {
                    throw ire("Cannot re-add previously dropped column '%s' of kind %s, incompatible with previous kind %s",
                              name,
                              isStatic ? ColumnMetadata.Kind.STATIC : ColumnMetadata.Kind.REGULAR,
                              droppedColumn.kind);
                }

                // Cannot re-add a dropped counter column. See #7831.
                if (table.isCounter())
                    throw ire("Cannot re-add previously dropped counter column %s", name);
            }

            if (isStatic)
                tableBuilder.addStaticColumn(name, type);
            else
                tableBuilder.addRegularColumn(name, type);

            if (!isStatic)
            {
                for (ViewMetadata view : keyspace.views.forTable(table.id))
                {
                    if (view.includeAllColumns)
                    {
                        ColumnMetadata viewColumn = ColumnMetadata.regularColumn(view.metadata, name.bytes, type);
                        viewsBuilder.put(viewsBuilder.get(view.name()).withAddedRegularColumn(viewColumn));
                    }
                }
            }
        }
    }

    /**
     * ALTER TABLE <table> DROP <column>
     * ALTER TABLE <table> DROP ( <column>, <column1>, ... <columnn>)
     */
    // TODO: swap UDT refs with expanded tuples on drop
    private static class DropColumns extends AlterTableStatement
    {
        private final Set<ColumnIdentifier> removedColumns;
        private final Long timestamp;

        private DropColumns(String queryString, String keyspaceName, String tableName, Set<ColumnIdentifier> removedColumns, Long timestamp)
        {
            super(queryString, keyspaceName, tableName);
            this.removedColumns = removedColumns;
            this.timestamp = timestamp;
        }

        public KeyspaceMetadata apply(KeyspaceMetadata keyspace, TableMetadata table)
        {
            TableMetadata.Builder builder = table.unbuild();
            removedColumns.forEach(c -> dropColumn(keyspace, table, c, builder));
            return keyspace.withSwapped(keyspace.tables.withSwapped(builder.build()));
        }

        private void dropColumn(KeyspaceMetadata keyspace, TableMetadata table, ColumnIdentifier column, TableMetadata.Builder builder)
        {
            ColumnMetadata currentColumn = table.getColumn(column);
            if (null == currentColumn)
                throw ire("Column %s was not found in table '%s'", column, table);

            if (currentColumn.isPrimaryKeyColumn())
                throw ire("Cannot drop PRIMARY KEY column %s", column);

            /*
             * Cannot allow dropping top-level columns of user defined types that aren't frozen because we cannot convert
             * the type into an equivalent tuple: we only support frozen tuples currently. And as such we cannot persist
             * the correct type in system_schema.dropped_columns.
             */
            if (currentColumn.type.isUDT() && currentColumn.type.isMultiCell())
                throw ire("Cannot drop non-frozen column %s of user type %s", column, currentColumn.type.asCQL3Type());

            // TODO: some day try and find a way to not rely on Keyspace/IndexManager/Index to find dependent indexes
            Set<IndexMetadata> dependentIndexes = Keyspace.openAndGetStore(table).indexManager.getDependentIndexes(currentColumn);
            if (!dependentIndexes.isEmpty())
            {
                throw ire("Cannot drop column %s because it has dependent secondary indexes (%s)",
                          currentColumn,
                          join(", ", transform(dependentIndexes, i -> i.name)));
            }

            if (!isEmpty(keyspace.views.forTable(table.id)))
                throw ire("Cannot drop column %s on base table %s with materialized views", currentColumn, table.name);

            builder.removeRegularOrStaticColumn(column);
            builder.recordColumnDrop(currentColumn, getTimestamp());
        }

        /**
         * @return timestamp from query, otherwise return current time in micros
         */
        private long getTimestamp()
        {
            return timestamp == null ? ClientState.getTimestamp() : timestamp;
        }
    }

    /**
     * ALTER TABLE <table> RENAME <column> TO <column>;
     */
    private static class RenameColumns extends AlterTableStatement
    {
        private final Map<ColumnIdentifier, ColumnIdentifier> renamedColumns;

        private RenameColumns(String queryString, String keyspaceName, String tableName, Map<ColumnIdentifier, ColumnIdentifier> renamedColumns)
        {
            super(queryString, keyspaceName, tableName);
            this.renamedColumns = renamedColumns;
        }

        public KeyspaceMetadata apply(KeyspaceMetadata keyspace, TableMetadata table)
        {
            TableMetadata.Builder tableBuilder = table.unbuild();
            Views.Builder viewsBuilder = keyspace.views.unbuild();
            renamedColumns.forEach((o, n) -> renameColumn(keyspace, table, o, n, tableBuilder, viewsBuilder));

            return keyspace.withSwapped(keyspace.tables.withSwapped(tableBuilder.build()))
                           .withSwapped(viewsBuilder.build());
        }

        private void renameColumn(KeyspaceMetadata keyspace,
                                  TableMetadata table,
                                  ColumnIdentifier oldName,
                                  ColumnIdentifier newName,
                                  TableMetadata.Builder tableBuilder,
                                  Views.Builder viewsBuilder)
        {
            ColumnMetadata column = table.getExistingColumn(oldName);
            if (null == column)
                throw ire("Column %s was not found in table %s", oldName, table);

            if (!column.isPrimaryKeyColumn())
                throw ire("Cannot rename non PRIMARY KEY column %s", oldName);

            if (null != table.getColumn(newName))
            {
                throw ire("Cannot rename column %s to %s in table '%s'; another column with that name already exists",
                          oldName,
                          newName,
                          table);
            }

            // TODO: some day try and find a way to not rely on Keyspace/IndexManager/Index to find dependent indexes
            Set<IndexMetadata> dependentIndexes = Keyspace.openAndGetStore(table).indexManager.getDependentIndexes(column);
            if (!dependentIndexes.isEmpty())
            {
                throw ire("Can't rename column %s because it has dependent secondary indexes (%s)",
                          oldName,
                          join(", ", transform(dependentIndexes, i -> i.name)));
            }

            for (ViewMetadata view : keyspace.views.forTable(table.id))
            {
                if (view.includes(oldName))
                {
                    viewsBuilder.put(viewsBuilder.get(view.name()).withRenamedPrimaryKeyColumn(oldName, newName));
                }
            }

            tableBuilder.renamePrimaryKeyColumn(oldName, newName);
        }
    }

    /**
     * ALTER TABLE <table> WITH <property> = <value>
     */
    private static class AlterOptions extends AlterTableStatement
    {
        private final TableAttributes attrs;

        private AlterOptions(String queryString, String keyspaceName, String tableName, TableAttributes attrs)
        {
            super(queryString, keyspaceName, tableName);
            this.attrs = attrs;
        }

        @Override
        public void validate(QueryState state)
        {
            super.validate(state);

            Guardrails.disallowedTableProperties.ensureAllowed(attrs.updatedProperties(), state);
            Guardrails.ignoredTableProperties.maybeIgnoreAndWarn(attrs.updatedProperties(), attrs::removeProperty, state);
        }

        public KeyspaceMetadata apply(KeyspaceMetadata keyspace, TableMetadata table)
        {
            attrs.validate();

            TableParams params = attrs.asAlteredTableParams(table.params);

            if (table.isCounter() && params.defaultTimeToLive > 0)
                throw ire("Cannot set default_time_to_live on a table with counters");

            if (!isEmpty(keyspace.views.forTable(table.id)) && params.gcGraceSeconds == 0)
            {
                throw ire("Cannot alter gc_grace_seconds of the base table of a " +
                          "materialized view to 0, since this value is used to TTL " +
                          "undelivered updates. Setting gc_grace_seconds too low might " +
                          "cause undelivered updates to expire " +
                          "before being replayed.");
            }

            if (keyspace.createReplicationStrategy().hasTransientReplicas()
                && params.readRepair != ReadRepairStrategy.NONE)
            {
                throw ire("read_repair must be set to 'NONE' for transiently replicated keyspaces");
            }

            TableMetadata.Builder builder = table.unbuild().params(params);
            for (DroppedColumn.Raw record : attrs.droppedColumnRecords())
                builder.recordColumnDrop(record.prepare(keyspaceName, tableName));

            return keyspace.withSwapped(keyspace.tables.withSwapped(builder.build()));
        }
    }


    /**
     * ALTER TABLE <table> DROP COMPACT STORAGE
     */
    private static class DropCompactStorage extends AlterTableStatement
    {
        private static final Logger logger = LoggerFactory.getLogger(AlterTableStatement.class);
        private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 5L, TimeUnit.MINUTES);
        private DropCompactStorage(String queryString, String keyspaceName, String tableName)
        {
            super(queryString, keyspaceName, tableName);
        }

        public KeyspaceMetadata apply(KeyspaceMetadata keyspace, TableMetadata table)
        {
            if (!DatabaseDescriptor.enableDropCompactStorage())
                throw new InvalidRequestException("DROP COMPACT STORAGE is disabled. Enable in cassandra.yaml to use.");

            if (!table.isCompactTable())
                throw AlterTableStatement.ire("Cannot DROP COMPACT STORAGE on table without COMPACT STORAGE");

            validateCanDropCompactStorage();

            Set<Flag> flags = table.isCounter()
                            ? ImmutableSet.of(Flag.COMPOUND, Flag.COUNTER)
                            : ImmutableSet.of(Flag.COMPOUND);

            return keyspace.withSwapped(keyspace.tables.withSwapped(table.withSwapped(flags)));
        }

        /**
         * Throws if DROP COMPACT STORAGE cannot be used (yet) because the cluster is not sufficiently upgraded. To be able
         * to use DROP COMPACT STORAGE, we need to ensure that no pre-3.0 sstables exists in the cluster, as we won't be
         * able to read them anymore once COMPACT STORAGE is dropped (see CASSANDRA-15897). In practice, this method checks
         * 3 things:
         *   1) that all nodes are on 3.0+. We need this because 2.x nodes don't advertise their sstable versions.
         *   2) for 3.0+, we use the new (CASSANDRA-15897) sstables versions set gossiped by all nodes to ensure all
         *      sstables have been upgraded cluster-wise.
         *   3) if the cluster still has some 3.0 nodes that predate CASSANDRA-15897, we will not have the sstable versions
         *      for them. In that case, we also refuse DROP COMPACT (even though it may well be safe at this point) and ask
         *      the user to upgrade all nodes.
         */
        private void validateCanDropCompactStorage()
        {
            Set<InetAddressAndPort> before4 = new HashSet<>();
            Set<InetAddressAndPort> preC15897nodes = new HashSet<>();
            Set<InetAddressAndPort> with2xSStables = new HashSet<>();
            Splitter onComma = Splitter.on(',').omitEmptyStrings().trimResults();
            for (InetAddressAndPort node : StorageService.instance.getTokenMetadataForKeyspace(keyspaceName).getAllEndpoints())
            {
                if (MessagingService.instance().versions.knows(node) &&
                    MessagingService.instance().versions.getRaw(node) < MessagingService.VERSION_40)
                {
                    before4.add(node);
                    continue;
                }

                String sstableVersionsString = Gossiper.instance.getApplicationState(node, ApplicationState.SSTABLE_VERSIONS);
                if (sstableVersionsString == null)
                {
                    preC15897nodes.add(node);
                    continue;
                }

                try
                {
                    boolean has2xSStables = onComma.splitToList(sstableVersionsString)
                                                   .stream()
                                                   .anyMatch(v -> v.compareTo("big-ma")<=0);
                    if (has2xSStables)
                        with2xSStables.add(node);
                }
                catch (IllegalArgumentException e)
                {
                    // Means VersionType::fromString didn't parse a version correctly. Which shouldn't happen, we shouldn't
                    // have garbage in Gossip. But crashing the request is not ideal, so we log the error but ignore the
                    // node otherwise.
                    noSpamLogger.error("Unexpected error parsing sstable versions from gossip for {} (gossiped value " +
                                       "is '{}'). This is a bug and should be reported. Cannot ensure that {} has no " +
                                       "non-upgraded 2.x sstables anymore. If after this DROP COMPACT STORAGE some old " +
                                       "sstables cannot be read anymore, please use `upgradesstables` with the " +
                                       "`--force-compact-storage-on` option.", node, sstableVersionsString, node);
                }
            }

            if (!before4.isEmpty())
                throw new InvalidRequestException(format("Cannot DROP COMPACT STORAGE as some nodes in the cluster (%s) " +
                                                         "are not on 4.0+ yet. Please upgrade those nodes and run " +
                                                         "`upgradesstables` before retrying.", before4));
            if (!preC15897nodes.isEmpty())
                throw new InvalidRequestException(format("Cannot guarantee that DROP COMPACT STORAGE is safe as some nodes " +
                                                         "in the cluster (%s) do not have https://issues.apache.org/jira/browse/CASSANDRA-15897. " +
                                                         "Please upgrade those nodes and retry.", preC15897nodes));
            if (!with2xSStables.isEmpty())
                throw new InvalidRequestException(format("Cannot DROP COMPACT STORAGE as some nodes in the cluster (%s) " +
                                                         "has some non-upgraded 2.x sstables. Please run `upgradesstables` " +
                                                         "on those nodes before retrying", with2xSStables));
        }
    }

    public static final class Raw extends RawKeyspaceAwareStatement<AlterTableStatement>
    {
        private enum Kind
        {
            ALTER_COLUMN, ADD_COLUMNS, DROP_COLUMNS, RENAME_COLUMNS, ALTER_OPTIONS, DROP_COMPACT_STORAGE
        }

        private final QualifiedName name;

        private Kind kind;

        // ADD
        private final List<AddColumns.Column> addedColumns = new ArrayList<>();

        // DROP
        private final Set<ColumnIdentifier> droppedColumns = new HashSet<>();
        private Long dropTimestamp = null; // will use execution timestamp if not provided by query

        // RENAME
        private final Map<ColumnIdentifier, ColumnIdentifier> renamedColumns = new HashMap<>();

        // OPTIONS
        public final TableAttributes attrs = new TableAttributes();

        public Raw(QualifiedName name)
        {
            this.name = name;
        }

        @Override
        public AlterTableStatement prepare(ClientState state, UnaryOperator<String> keyspaceMapper)
        {
            String keyspaceName = keyspaceMapper.apply(name.hasKeyspace() ? name.getKeyspace() : state.getKeyspace());
            String tableName = name.getName();

            switch (kind)
            {
                case          ALTER_COLUMN: return new AlterColumn(rawCQLStatement, keyspaceName, tableName);
                case           ADD_COLUMNS:
                    if (keyspaceMapper != Constants.IDENTITY_STRING_MAPPER)
                        addedColumns.forEach(c -> c.type.forEachUserType(utName -> utName.updateKeyspaceIfDefined(keyspaceMapper)));
                    return new AddColumns(rawCQLStatement, keyspaceName, tableName, addedColumns);
                case          DROP_COLUMNS: return new DropColumns(rawCQLStatement, keyspaceName, tableName, droppedColumns, dropTimestamp);
                case        RENAME_COLUMNS: return new RenameColumns(rawCQLStatement, keyspaceName, tableName, renamedColumns);
                case         ALTER_OPTIONS: return new AlterOptions(rawCQLStatement, keyspaceName, tableName, attrs);
                case  DROP_COMPACT_STORAGE: return new DropCompactStorage(rawCQLStatement, keyspaceName, tableName);
            }

            throw new AssertionError();
        }

        public void alter(ColumnIdentifier name, CQL3Type.Raw type)
        {
            kind = Kind.ALTER_COLUMN;
        }

        public void add(ColumnIdentifier name, CQL3Type.Raw type, boolean isStatic)
        {
            kind = Kind.ADD_COLUMNS;
            addedColumns.add(new AddColumns.Column(name, type, isStatic));
        }

        public void drop(ColumnIdentifier name)
        {
            kind = Kind.DROP_COLUMNS;
            droppedColumns.add(name);
        }

        public void dropCompactStorage()
        {
            kind = Kind.DROP_COMPACT_STORAGE;
        }

        public void dropTimestamp(long timestamp)
        {
            this.dropTimestamp = timestamp;
        }

        public void rename(ColumnIdentifier from, ColumnIdentifier to)
        {
            kind = Kind.RENAME_COLUMNS;
            renamedColumns.put(from, to);
        }

        public void attrs()
        {
            this.kind = Kind.ALTER_OPTIONS;
        }
    }
}
