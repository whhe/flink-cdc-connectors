/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oceanbase.table;

import com.ververica.cdc.connectors.oceanbase.source.OceanBaseDataSource;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.Envelope;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Utils to deal with table schema of OceanBase. */
public class OceanBaseTableSchema {

    private static final Map<String, TableSchema> tableSchemaMap = new ConcurrentHashMap<>();

    private static String topicName(String tenantName, String databaseName, String tableName) {
        return String.format("%s.%s.%s", tenantName, databaseName, tableName);
    }

    public static TableSchemaBuilder tableSchemaBuilder(
            String serverTimezone,
            String compatibleMode,
            DataSource dataSource,
            String decimalMode,
            String temporalPrecisionMode,
            String bigintUnsignedHandlingMode,
            String binaryHandlingMode) {
        ValueConverterProvider valueConverterProvider;
        if ("mysql".equalsIgnoreCase(compatibleMode)) {
            valueConverterProvider =
                    new OceanBaseMysqlValueConverters(
                            JdbcValueConverters.DecimalMode.valueOf(decimalMode.toUpperCase()),
                            TemporalPrecisionMode.parse(temporalPrecisionMode),
                            JdbcValueConverters.BigIntUnsignedMode.valueOf(
                                    bigintUnsignedHandlingMode.toUpperCase()),
                            CommonConnectorConfig.BinaryHandlingMode.parse(binaryHandlingMode));
        } else {
            valueConverterProvider =
                    new OceanBaseOracleValueConverters(
                            serverTimezone,
                            dataSource,
                            JdbcValueConverters.DecimalMode.valueOf(decimalMode.toUpperCase()),
                            TemporalPrecisionMode.parse(temporalPrecisionMode),
                            CommonConnectorConfig.BinaryHandlingMode.parse(binaryHandlingMode));
        }
        return new TableSchemaBuilder(
                valueConverterProvider,
                SchemaNameAdjuster.create(),
                new CustomConverterRegistry(null),
                sourceSchema(),
                false,
                false);
    }

    public static TableSchema getTableSchema(
            String serverTimezone,
            OceanBaseDataSource dataSource,
            String compatibleMode,
            String tenantName,
            String db,
            String schema,
            String tableName,
            String decimalMode,
            String temporalPrecisionMode,
            String bigintUnsignedHandlingMode,
            String binaryHandlingMode) {
        String topicName = topicName(tenantName, db != null ? db : schema, tableName);
        TableSchema tableSchema = tableSchemaMap.get(topicName);
        if (tableSchema == null) {
            TableEditor tableEditor = Table.editor().tableId(new TableId(db, schema, tableName));
            try (Connection connection = dataSource.getConnection()) {
                DatabaseMetaData metadata = connection.getMetaData();
                try (ResultSet columnMetadata = metadata.getColumns(db, schema, tableName, null)) {
                    while (columnMetadata.next()) {
                        final String columnName = columnMetadata.getString(4);
                        ColumnEditor column = Column.editor().name(columnName);
                        String typeName = columnMetadata.getString(6);
                        column.type(typeName);
                        column.length(columnMetadata.getInt(7));
                        if (columnMetadata.getObject(9) != null) {
                            column.scale(columnMetadata.getInt(9));
                        }
                        column.optional(isNullable(columnMetadata.getInt(11)));
                        column.position(columnMetadata.getInt(17));
                        column.jdbcType(resolveJdbcType(columnMetadata.getInt(5), typeName));
                        tableEditor.addColumn(column.create());
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException("Failed to generate table schema", e);
            }
            tableSchema =
                    tableSchemaBuilder(
                                    serverTimezone,
                                    compatibleMode,
                                    dataSource,
                                    decimalMode,
                                    temporalPrecisionMode,
                                    bigintUnsignedHandlingMode,
                                    binaryHandlingMode)
                            .create(
                                    null,
                                    Envelope.schemaName(topicName),
                                    tableEditor.create(),
                                    null,
                                    null,
                                    null);
            tableSchemaMap.put(topicName, tableSchema);
        }
        return tableSchema;
    }

    private static int resolveJdbcType(int jdbcType, String typeName) {
        String t = typeName.toUpperCase();
        if (matches(t, "TIMESTAMP")) {
            if (t.contains("WITH TIME ZONE")) {
                return Types.TIMESTAMP_WITH_TIMEZONE;
            }
            return Types.TIMESTAMP;
        }
        if (matches(t, "NCHAR")) {
            return Types.NCHAR;
        }
        if (matches(t, "NVARCHAR2")) {
            return Types.NVARCHAR;
        }
        if (matches(t, "JSON")) {
            return Types.VARCHAR;
        }
        return jdbcType;
    }

    private static boolean matches(String upperCaseTypeName, String upperCaseMatch) {
        if (upperCaseTypeName == null) {
            return false;
        }
        return upperCaseMatch.equals(upperCaseTypeName)
                || upperCaseTypeName.startsWith(upperCaseMatch + "(");
    }

    protected static boolean isNullable(int jdbcNullable) {
        return jdbcNullable == ResultSetMetaData.columnNullable
                || jdbcNullable == ResultSetMetaData.columnNullableUnknown;
    }

    public static Schema sourceSchema() {
        return SchemaBuilder.struct()
                .field("tenant", Schema.STRING_SCHEMA)
                .field("db", Schema.OPTIONAL_STRING_SCHEMA)
                .field("schema", Schema.OPTIONAL_STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .field("timestamp", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    public static Struct sourceStruct(
            String tenant, String db, String schema, String table, String timestamp) {
        Struct struct =
                new Struct(sourceSchema())
                        .put("tenant", tenant)
                        .put("db", db)
                        .put("schema", schema)
                        .put("table", table);
        if (timestamp != null) {
            struct.put("timestamp", timestamp);
        }
        return struct;
    }
}
