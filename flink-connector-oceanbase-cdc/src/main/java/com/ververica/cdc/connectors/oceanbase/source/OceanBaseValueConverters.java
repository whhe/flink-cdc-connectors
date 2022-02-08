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

package com.ververica.cdc.connectors.oceanbase.source;

import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.sql.Types;
import java.time.ZoneOffset;

/** OceanBase-specific customization of the conversions from JDBC values. */
public class OceanBaseValueConverters extends JdbcValueConverters {

    public OceanBaseValueConverters() {
        super(
                DecimalMode.STRING,
                TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                ZoneOffset.UTC,
                null,
                null,
                null);
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        switch (column.jdbcType()) {
            case Types.TINYINT:
            case Types.SMALLINT:
                return SchemaBuilder.int32();
            case Types.REAL:
                return SchemaBuilder.float64();
            default:
                return super.schemaBuilder(column);
        }
    }
}
