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

import com.oceanbase.jdbc.OceanBaseConnection;
import com.oceanbase.jdbc.extend.datatype.TIMESTAMPLTZ;
import com.oceanbase.jdbc.extend.datatype.TIMESTAMPTZ;
import com.oceanbase.jdbc.internal.ObOracleDefs;
import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.util.NumberConversions;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import javax.sql.DataSource;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;

import static io.debezium.util.NumberConversions.BYTE_FALSE;

/** JdbcValueConverters for OceanBase Oracle mode. */
public class OceanBaseOracleValueConverters extends JdbcValueConverters {

    public static final String EMPTY_BLOB_FUNCTION = "EMPTY_BLOB()";
    public static final String EMPTY_CLOB_FUNCTION = "EMPTY_CLOB()";

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .toFormatter();

    private static final DateTimeFormatter TIMESTAMP_AM_PM_SHORT_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("dd-MMM-yy hh.mm.ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .appendPattern(" a")
                    .toFormatter(Locale.ENGLISH);

    private static final DateTimeFormatter TIMESTAMP_TZ_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendPattern(".")
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
                    .optionalEnd()
                    .optionalStart()
                    .appendPattern(" ")
                    .optionalEnd()
                    .appendOffset("+HH:MM", "")
                    .toFormatter();

    private final String serverTimezone;
    private final DataSource dataSource;

    public OceanBaseOracleValueConverters(
            String serverTimezone,
            DataSource dataSource,
            JdbcValueConverters.DecimalMode decimalMode,
            TemporalPrecisionMode temporalPrecisionMode,
            CommonConnectorConfig.BinaryHandlingMode binaryHandlingMode) {
        super(decimalMode, temporalPrecisionMode, ZoneOffset.UTC, null, null, binaryHandlingMode);
        this.serverTimezone = serverTimezone;
        this.dataSource = dataSource;
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        logger.debug(
                "Building schema for column {} of type {} named {} with constraints ({},{})",
                column.name(),
                column.jdbcType(),
                column.typeName(),
                column.length(),
                column.scale());

        switch (column.jdbcType()) {
            case Types.FLOAT:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return getNumericSchema(column);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.CLOB:
                return SchemaBuilder.string();
            case ObOracleDefs.FIELD_JAVA_TYPE_BINARY_FLOAT:
                return SchemaBuilder.float32();
            case ObOracleDefs.FIELD_JAVA_TYPE_BINARY_DOUBLE:
                return SchemaBuilder.float64();
            case Types.BLOB:
                return binaryMode.getSchema();
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    if (getTimePrecision(column) <= 3) {
                        return io.debezium.time.Timestamp.builder();
                    }
                    if (getTimePrecision(column) <= 6) {
                        return MicroTimestamp.builder();
                    }
                    return NanoTimestamp.builder();
                }
                return org.apache.kafka.connect.data.Timestamp.builder();
            default:
                return super.schemaBuilder(column);
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            case Types.FLOAT:
            case Types.NUMERIC:
            case Types.DECIMAL:
                return getNumericConverter(column, fieldDefn);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.CLOB:
                return data -> convertString(column, fieldDefn, data);
            case ObOracleDefs.FIELD_JAVA_TYPE_BINARY_FLOAT:
                return data -> convertFloat(column, fieldDefn, data);
            case ObOracleDefs.FIELD_JAVA_TYPE_BINARY_DOUBLE:
                return data -> convertDouble(column, fieldDefn, data);
            case Types.BLOB:
                return data -> convertBinary(column, fieldDefn, data, binaryMode);
            case Types.DATE:
            case Types.TIMESTAMP:
                return (data) -> convertTimestamp(column, fieldDefn, data);
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return (data) -> convertTimestampTZ(column, fieldDefn, data);
            default:
                return super.converter(column, fieldDefn);
        }
    }

    private SchemaBuilder getNumericSchema(Column column) {
        if (column.scale().isPresent()) {
            // return sufficiently sized int schema for non-floating point types
            Integer scale = column.scale().get();

            // a negative scale means rounding, e.g. NUMBER(10, -2) would be rounded to hundreds
            if (scale <= 0) {
                int width = column.length() - scale;
                if (width < 3) {
                    return SchemaBuilder.int8();
                } else if (width < 5) {
                    return SchemaBuilder.int16();
                } else if (width < 10) {
                    return SchemaBuilder.int32();
                } else if (width < 19) {
                    return SchemaBuilder.int64();
                }
            }
        }
        return SpecialValueDecimal.builder(decimalMode, column.length(), column.scale().orElse(-1));
    }

    private ValueConverter getNumericConverter(Column column, Field fieldDefn) {
        if (column.scale().isPresent()) {
            int scale = column.scale().get();

            if (scale <= 0) {
                int width = column.length() - scale;
                if (width < 3) {
                    return data -> convertTinyInt(column, fieldDefn, data);
                } else if (width < 5) {
                    return data -> convertSmallInt(column, fieldDefn, data);
                } else if (width < 10) {
                    return data -> convertInteger(column, fieldDefn, data);
                } else if (width < 19) {
                    return data -> convertBigInt(column, fieldDefn, data);
                }
            }
        }
        return data -> convertDecimal(column, fieldDefn, data);
    }

    @Override
    protected Object convertTinyInt(Column column, Field fieldDefn, Object data) {
        return convertValue(
                column,
                fieldDefn,
                data,
                BYTE_FALSE,
                (r) -> {
                    if (data instanceof Byte) {
                        r.deliver(data);
                    } else if (data instanceof Number) {
                        Number value = (Number) data;
                        r.deliver(value.byteValue());
                    } else if (data instanceof Boolean) {
                        r.deliver(NumberConversions.getByte((boolean) data));
                    } else if (data instanceof String) {
                        r.deliver(Byte.parseByte((String) data));
                    }
                });
    }

    @Override
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        if (data instanceof Clob) {
            try {
                Clob clob = (Clob) data;
                // Note that java.sql.Clob specifies that the first character starts at 1
                // and that length must be greater-than or equal to 0. So for an empty
                // clob field, a call to getSubString(1, 0) is perfectly valid.
                return clob.getSubString(1, (int) clob.length());
            } catch (SQLException e) {
                throw new DebeziumException(
                        "Couldn't convert value for column " + column.name(), e);
            }
        }
        if (data instanceof String) {
            String s = (String) data;
            if (EMPTY_CLOB_FUNCTION.equals(s)) {
                return column.isOptional() ? null : "";
            }
        }
        return super.convertString(column, fieldDefn, data);
    }

    @Override
    protected Object convertBinary(
            Column column, Field fieldDefn, Object data, BinaryHandlingMode mode) {
        try {
            if (data instanceof Blob) {
                Blob blob = (Blob) data;
                data = blob.getBytes(1, Long.valueOf(blob.length()).intValue());
            }
            if (data instanceof String) {
                String str = (String) data;
                if (EMPTY_BLOB_FUNCTION.equals(str)) {
                    data = column.isOptional() ? null : "";
                }
            }
            return super.convertBinary(column, fieldDefn, data, mode);
        } catch (SQLException e) {
            throw new DebeziumException("Couldn't convert value for column " + column.name(), e);
        }
    }

    @Override
    protected Object convertFloat(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return Float.parseFloat((String) data);
        }
        return data;
    }

    @Override
    protected Object convertDouble(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            return Double.parseDouble((String) data);
        }
        return super.convertDouble(column, fieldDefn, data);
    }

    protected Object convertTimestamp(Column column, Field fieldDefn, Object data) {
        if (data instanceof TIMESTAMPLTZ) {
            try (Connection connection = dataSource.getConnection()) {
                data =
                        ((TIMESTAMPLTZ) data)
                                .timestampValue(connection.unwrap(OceanBaseConnection.class));
            } catch (SQLException e) {
                throw new RuntimeException("Failed to convert timestamp with local timezone", e);
            }
        }
        if (data instanceof String) {
            data = resolveTimestampStringAsInstant((String) data);
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
    }

    protected Object convertTimestampTZ(Column column, Field fieldDefn, Object data) {
        if (data instanceof String) {
            ZonedDateTime zonedDateTime =
                    ZonedDateTime.parse(((String) data).trim(), TIMESTAMP_TZ_FORMATTER);
            data = Timestamp.from(zonedDateTime.toInstant());
        }
        if (data instanceof TIMESTAMPTZ) {
            try {
                data = ((TIMESTAMPTZ) data).timestampValue();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to convert timestamp with timezone", e);
            }
        }
        return convertTimestamp(column, fieldDefn, data);
    }

    protected Instant resolveTimestampStringAsInstant(String dateText) {
        LocalDateTime dateTime;
        if (dateText.indexOf(" AM") > 0 || dateText.indexOf(" PM") > 0) {
            dateTime = LocalDateTime.from(TIMESTAMP_AM_PM_SHORT_FORMATTER.parse(dateText.trim()));
        } else {
            dateTime = LocalDateTime.from(TIMESTAMP_FORMATTER.parse(dateText.trim()));
        }
        return dateTime.atZone(ZoneId.of(serverTimezone)).toInstant();
    }

    @Override
    protected int getTimePrecision(Column column) {
        return column.scale().orElse(0);
    }
}
