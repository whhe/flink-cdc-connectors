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

package com.ververica.cdc.connectors.oceanbase.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/** Test for {@link OceanBaseTableSource} created by {@link OceanBaseTableSourceFactory}. */
public class OceanBaseTableFactoryTest {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("aaa", DataTypes.INT().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3))),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("aaa")));

    private static final String STARTUP_MODE = "initial";
    private static final String USERNAME = "user@sys";
    private static final String PASSWORD = "pswd";
    private static final String TENANT_NAME = "sys";
    private static final String DATABASE_NAME = "db";
    private static final String TABLE_NAME = "table";
    private static final String RS_LIST = "127.0.0.1:2882:2881";
    private static final String LOG_PROXY_HOST = "127.0.0.1";
    private static final String JDBC_URL = "jdbc:mysql://127.0.0.1:2881";

    @Test
    public void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        DynamicTableSource actualSource = createTableSource(SCHEMA, properties);
        OceanBaseTableSource expectedSource =
                new OceanBaseTableSource(
                        SCHEMA,
                        OceanBaseTableSourceFactory.StartupMode.INITIAL,
                        null,
                        USERNAME,
                        PASSWORD,
                        TENANT_NAME,
                        DATABASE_NAME,
                        TABLE_NAME,
                        RS_LIST,
                        LOG_PROXY_HOST,
                        2983,
                        null);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testOptionalProperties() {
        Map<String, String> options = getAllOptions();
        options.put("scan.startup.mode", "timestamp");
        options.put("scan.startup.timestamp", "0");
        options.put("jdbc.url", JDBC_URL);
        DynamicTableSource actualSource = createTableSource(SCHEMA, options);

        OceanBaseTableSource expectedSource =
                new OceanBaseTableSource(
                        SCHEMA,
                        OceanBaseTableSourceFactory.StartupMode.TIMESTAMP,
                        0L,
                        USERNAME,
                        PASSWORD,
                        TENANT_NAME,
                        DATABASE_NAME,
                        TABLE_NAME,
                        RS_LIST,
                        LOG_PROXY_HOST,
                        2983,
                        JDBC_URL);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testValidation() {
        try {
            Map<String, String> properties = getAllOptions();
            properties.put("unknown", "abc");

            createTableSource(SCHEMA, properties);
            fail("exception expected");
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(t, "Unsupported options:\n\nunknown")
                            .isPresent());
        }
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "oceanbase-cdc");
        options.put("scan.startup.mode", STARTUP_MODE);
        options.put("username", USERNAME);
        options.put("password", PASSWORD);
        options.put("tenant_name", TENANT_NAME);
        options.put("database_name", DATABASE_NAME);
        options.put("table_name", TABLE_NAME);
        options.put("rootserver_list", RS_LIST);
        options.put("log_proxy.host", LOG_PROXY_HOST);
        return options;
    }

    private static DynamicTableSource createTableSource(
            ResolvedSchema schema, Map<String, String> options) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(schema).build(),
                                "mock source",
                                new ArrayList<>(),
                                options),
                        schema),
                new Configuration(),
                OceanBaseTableFactoryTest.class.getClassLoader(),
                false);
    }
}
