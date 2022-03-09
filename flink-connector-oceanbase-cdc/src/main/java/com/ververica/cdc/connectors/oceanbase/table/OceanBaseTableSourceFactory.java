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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

/** Factory for creating configured instance of {@link OceanBaseTableSource}. */
public class OceanBaseTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "oceanbase-cdc";

    /** Startup modes for the OceanBase CDC Consumer. */
    public enum StartupMode {
        /**
         * Performs an initial snapshot on the monitored database tables upon first startup, and
         * continue to read the commit log.
         */
        INITIAL,

        /**
         * Never to perform snapshot on the monitored database tables upon first startup, just read
         * from the end of the commit log which means only have the changes since the connector was
         * started.
         */
        LATEST,

        /**
         * Never to perform snapshot on the monitored database tables upon first startup, and
         * directly read commit log from the specified timestamp.
         */
        TIMESTAMP
    }

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("initial")
                    .withDescription(
                            "Optional startup mode for OceanBase CDC consumer, valid enumerations are "
                                    + "\"initial\", \"latest\" or \"timestamp\"");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP =
            ConfigOptions.key("scan.startup.timestamp")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp in seconds used in case of \"timestamp\" startup mode.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username to be used when connecting to OceanBase.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to be used when connecting to OceanBase.");

    public static final ConfigOption<String> TENANT_NAME =
            ConfigOptions.key("tenant-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Tenant name of OceanBase to monitor.");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of OceanBase to monitor.");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of OceanBase to monitor.");

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "IP address or hostname of the OceanBase database server or OceanBase proxy server.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Integer port number of OceanBase database server or OceanBase proxy server.");

    public static final ConfigOption<Duration> CONNECT_TIMEOUT =
            ConfigOptions.key("connect.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The maximum time that the connector should wait after trying to connect to the OceanBase database server before timing out.");

    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription("The session time zone in database server.");

    public static final ConfigOption<String> RS_LIST =
            ConfigOptions.key("rootserver-list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The semicolon-separated list of OceanBase root servers in format `ip:rpc_port:sql_port`.");

    public static final ConfigOption<String> LOG_PROXY_HOST =
            ConfigOptions.key("logproxy.host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hostname or IP address of OceanBase log proxy service.");

    public static final ConfigOption<Integer> LOG_PROXY_PORT =
            ConfigOptions.key("logproxy.port")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Port number of OceanBase log proxy service.");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ResolvedSchema physicalSchema = context.getCatalogTable().getResolvedSchema();

        ReadableConfig config = helper.getOptions();

        String modeString = config.get(SCAN_STARTUP_MODE);
        StartupMode startupMode = StartupMode.valueOf(modeString.toUpperCase());
        Long startupTimestamp = config.get(SCAN_STARTUP_TIMESTAMP);

        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String tenantName = config.get(TENANT_NAME);
        String databaseName = config.get(DATABASE_NAME);
        String tableName = config.get(TABLE_NAME);
        String rsList = config.get(RS_LIST);
        String logProxyHost = config.get(LOG_PROXY_HOST);
        int logProxyPort = config.get(LOG_PROXY_PORT);
        String hostname = config.get(HOSTNAME);
        Integer port = config.get(PORT);
        Duration connectTimeout = config.get(CONNECT_TIMEOUT);
        ZoneId serverTimeZone = ZoneId.of(config.get(SERVER_TIME_ZONE));

        return new OceanBaseTableSource(
                physicalSchema,
                startupMode,
                startupTimestamp,
                username,
                password,
                tenantName,
                databaseName,
                tableName,
                hostname,
                port,
                connectTimeout,
                serverTimeZone,
                rsList,
                logProxyHost,
                logProxyPort);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN_STARTUP_MODE);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(TENANT_NAME);
        options.add(DATABASE_NAME);
        options.add(TABLE_NAME);
        options.add(RS_LIST);
        options.add(LOG_PROXY_HOST);
        options.add(LOG_PROXY_PORT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN_STARTUP_TIMESTAMP);
        options.add(HOSTNAME);
        options.add(PORT);
        options.add(CONNECT_TIMEOUT);
        options.add(SERVER_TIME_ZONE);
        return options;
    }
}
