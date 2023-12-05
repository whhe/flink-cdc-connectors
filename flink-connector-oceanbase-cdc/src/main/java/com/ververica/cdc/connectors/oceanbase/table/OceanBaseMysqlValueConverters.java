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

import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.Bits;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Types;
import java.time.ZoneOffset;

/** JdbcValueConverters for OceanBase MySQL mode. */
public class OceanBaseMysqlValueConverters extends JdbcValueConverters {

    public OceanBaseMysqlValueConverters(
            JdbcValueConverters.DecimalMode decimalMode,
            TemporalPrecisionMode temporalPrecisionMode,
            BigIntUnsignedMode bigIntUnsignedMode,
            CommonConnectorConfig.BinaryHandlingMode binaryHandlingMode) {
        super(
                decimalMode,
                temporalPrecisionMode,
                ZoneOffset.UTC,
                x -> x,
                bigIntUnsignedMode,
                binaryHandlingMode);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        switch (column.jdbcType()) {
            case Types.BIT:
                if (column.length() > 1) {
                    return Bits.builder(column.length());
                }
                return SchemaBuilder.bool();
            case Types.TINYINT:
                if (column.length() == 1) {
                    return SchemaBuilder.bool();
                }
                if (isUnsigned(column)) {
                    return SchemaBuilder.int16();
                }
                return SchemaBuilder.int8();
            case Types.SMALLINT:
                if (isUnsigned(column)) {
                    return SchemaBuilder.int32();
                }
                return SchemaBuilder.int16();
            case Types.INTEGER:
                if (isUnsigned(column)) {
                    return SchemaBuilder.int64();
                }
                return SchemaBuilder.int32();
            case Types.BIGINT:
                if (isUnsigned(column)) {
                    switch (bigIntUnsignedMode) {
                        case LONG:
                            return SchemaBuilder.int64();
                        case PRECISE:
                            return Decimal.builder(0);
                    }
                }
                return SchemaBuilder.int64();
            case Types.REAL:
                return SchemaBuilder.float32();
            case Types.DOUBLE:
                return SchemaBuilder.float64();
            case Types.DECIMAL:
                return SpecialValueDecimal.builder(
                        decimalMode, column.length(), column.scale().get());
            case Types.DATE:
                if (column.typeName().equalsIgnoreCase("YEAR")) {
                    return io.debezium.time.Year.builder();
                }
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    return io.debezium.time.Date.builder();
                }
                return org.apache.kafka.connect.data.Date.builder();
            case Types.TIME:
                if (adaptiveTimeMicrosecondsPrecisionMode) {
                    return io.debezium.time.MicroTime.builder();
                }
                if (adaptiveTimePrecisionMode) {
                    if (getTimePrecision(column) <= 3) {
                        return io.debezium.time.Time.builder();
                    }
                    if (getTimePrecision(column) <= 6) {
                        return io.debezium.time.MicroTime.builder();
                    }
                    return io.debezium.time.NanoTime.builder();
                }
                return org.apache.kafka.connect.data.Time.builder();
            case Types.TIMESTAMP:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    if (getTimePrecision(column) <= 3) {
                        return io.debezium.time.Timestamp.builder();
                    }
                    if (getTimePrecision(column) <= 6) {
                        return io.debezium.time.MicroTimestamp.builder();
                    }
                    return io.debezium.time.NanoTimestamp.builder();
                }
                return org.apache.kafka.connect.data.Timestamp.builder();
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return SchemaBuilder.string();
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return binaryMode.getSchema();
            default:
                return super.schemaBuilder(column);
        }
    }

    private boolean isUnsigned(Column column) {
        return column.typeName().toUpperCase().contains("UNSIGNED");
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            case Types.BIT:
                return convertBits(column, fieldDefn);
            case Types.TINYINT:
                if (column.length() == 1) {
                    return data -> convertBit(column, fieldDefn, data);
                }
                if (isUnsigned(column)) {
                    return data -> convertSmallInt(column, fieldDefn, data);
                }
                return data -> convertTinyInt(column, fieldDefn, data);
            case Types.SMALLINT:
                if (isUnsigned(column)) {
                    return data -> convertInteger(column, fieldDefn, data);
                }
                return data -> convertSmallInt(column, fieldDefn, data);
            case Types.INTEGER:
                if (isUnsigned(column)) {
                    return data -> convertBigInt(column, fieldDefn, data);
                }
                return data -> convertInteger(column, fieldDefn, data);
            case Types.BIGINT:
                if (isUnsigned(column)) {
                    switch (bigIntUnsignedMode) {
                        case LONG:
                            return (data) -> convertBigInt(column, fieldDefn, data);
                        case PRECISE:
                            return (data) -> convertUnsignedBigint(column, fieldDefn, data);
                    }
                }
                return (data) -> convertBigInt(column, fieldDefn, data);
            case Types.REAL:
                return data -> convertReal(column, fieldDefn, data);
            case Types.DOUBLE:
                return data -> convertDouble(column, fieldDefn, data);
            case Types.DECIMAL:
                return (data) -> convertDecimal(column, fieldDefn, data);
            case Types.DATE:
                if (column.typeName().equalsIgnoreCase("YEAR")) {
                    return (data) -> convertYearToInt(column, fieldDefn, data);
                }
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    return (data) -> convertDateToEpochDays(column, fieldDefn, data);
                }
                return (data) -> convertDateToEpochDaysAsDate(column, fieldDefn, data);
            case Types.TIME:
                return (data) -> convertTime(column, fieldDefn, data);
            case Types.TIMESTAMP:
                return data -> {
                    if (data instanceof String) {
                        data = java.sql.Timestamp.valueOf((String) data);
                    }
                    if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                        if (getTimePrecision(column) <= 3) {
                            return convertTimestampToEpochMillis(column, fieldDefn, data);
                        }
                        if (getTimePrecision(column) <= 6) {
                            return convertTimestampToEpochMicros(column, fieldDefn, data);
                        }
                        return convertTimestampToEpochNanos(column, fieldDefn, data);
                    }
                    return convertTimestampToEpochMillisAsDate(column, fieldDefn, data);
                };
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return data -> convertString(column, fieldDefn, data);
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return (data) -> convertBinary(column, fieldDefn, data, binaryMode);
            default:
                return super.converter(column, fieldDefn);
        }
    }

    @Override
    protected Object convertBit(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return Boolean.parseBoolean((String) data) || "1".equals(data);
        }
        return super.convertBit(column, fieldDefn, data);
    }

    @Override
    protected Object convertBits(Column column, Field fieldDefn, Object data, int numBytes) {
        if (data instanceof String) {
            data = ((String) data).getBytes();
        }
        return super.convertBits(column, fieldDefn, data, numBytes);
    }

    @Override
    protected Object convertTinyInt(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return Byte.parseByte((String) data);
        }
        if (data instanceof Integer) {
            return ((Integer) data).byteValue();
        }
        if (data instanceof Byte) {
            return data;
        }
        throw new IllegalArgumentException(
                "Unexpected value for JDBC type "
                        + column.jdbcType()
                        + " and column "
                        + column
                        + ": class="
                        + data.getClass());
    }

    @Override
    protected Object convertBigInt(Column column, Field fieldDefn, Object data) {
        if (data instanceof BigInteger) {
            return ((BigInteger) data).longValue();
        }
        return super.convertBigInt(column, fieldDefn, data);
    }

    protected Object convertUnsignedBigint(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return new BigDecimal((String) data);
        }
        if (data instanceof BigInteger) {
            return new BigDecimal((BigInteger) data);
        }
        return convertDecimal(column, fieldDefn, data);
    }

    @Override
    protected Object convertReal(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return Float.parseFloat((String) data);
        }
        return super.convertReal(column, fieldDefn, data);
    }

    @Override
    protected Object convertDouble(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return Double.parseDouble((String) data);
        }
        return super.convertDouble(column, fieldDefn, data);
    }

    @SuppressWarnings("deprecation")
    protected Object convertYearToInt(Column column, Field fieldDefn, Object data) {
        if (data instanceof Date) {
            return ((Date) data).getYear() + 1900;
        }
        return convertInteger(column, fieldDefn, data);
    }

    @Override
    protected Object convertDateToEpochDays(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = Date.valueOf((String) data);
        }
        return super.convertDateToEpochDays(column, fieldDefn, data);
    }

    @Override
    protected Object convertDateToEpochDaysAsDate(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = Date.valueOf((String) data);
        }
        return super.convertDateToEpochDaysAsDate(column, fieldDefn, data);
    }

    @Override
    protected Object convertTime(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            data = Time.valueOf((String) data);
        }
        return super.convertTime(column, fieldDefn, data);
    }
}
