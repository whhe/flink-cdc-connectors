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

package com.ververica.cdc.connectors.oceanbase;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.oceanbase.clogproxy.client.config.ClientConf;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.util.ClientIdGenerator;
import com.ververica.cdc.connectors.oceanbase.source.OceanBaseRichSourceFunction;
import com.ververica.cdc.connectors.oceanbase.table.OceanBaseDeserializationSchema;
import com.ververica.cdc.connectors.oceanbase.table.StartupMode;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.commons.lang3.StringUtils;

import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A builder to build a SourceFunction which can read snapshot and change events of OceanBase. */
@PublicEvolving
public class OceanBaseSource {

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Builder class of {@link OceanBaseSource}. */
    public static class Builder<T> {

        // common config
        private StartupMode startupMode;
        private String username;
        private String password;
        private String tenantName;
        private String databaseName;
        private String tableName;
        private String tableList;
        private String serverTimeZone;
        private Duration connectTimeout;
        private Integer connectMaxRetries;

        // snapshot reading config
        private String hostname;
        private Integer port;
        private String compatibleMode;
        private String jdbcDriver;
        private Properties jdbcProperties;
        private Integer connectionPoolSize;
        private Properties hikariProperties;
        private Boolean snapshotChunkEnabled;
        private String snapshotChunkKeyColumn;
        private Integer snapshotChunkSize;

        // incremental reading config
        private String logProxyHost;
        private Integer logProxyPort;
        private String logProxyClientId;
        private Long startupTimestamp;
        private String rsList;
        private String configUrl;
        private String workingMode;
        private String decimalMode;
        private String temporalPrecisionMode;
        private String bigintUnsignedHandlingMode;
        private String binaryHandlingMode;

        private OceanBaseDeserializationSchema<T> deserializer;
        private DebeziumDeserializationSchema<T> debeziumDeserializer;

        public Builder<T> startupMode(StartupMode startupMode) {
            this.startupMode = startupMode;
            return this;
        }

        public Builder<T> username(String username) {
            this.username = username;
            return this;
        }

        public Builder<T> password(String password) {
            this.password = password;
            return this;
        }

        public Builder<T> tenantName(String tenantName) {
            this.tenantName = tenantName;
            return this;
        }

        public Builder<T> databaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }

        public Builder<T> tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder<T> tableList(String tableList) {
            this.tableList = tableList;
            return this;
        }

        public Builder<T> serverTimeZone(String serverTimeZone) {
            this.serverTimeZone = serverTimeZone;
            return this;
        }

        public Builder<T> connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder<T> connectMaxRetries(Integer connectMaxRetries) {
            this.connectMaxRetries = connectMaxRetries;
            return this;
        }

        public Builder<T> hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public Builder<T> port(int port) {
            this.port = port;
            return this;
        }

        public Builder<T> compatibleMode(String compatibleMode) {
            this.compatibleMode = compatibleMode;
            return this;
        }

        public Builder<T> jdbcDriver(String jdbcDriver) {
            this.jdbcDriver = jdbcDriver;
            return this;
        }

        public Builder<T> jdbcProperties(Properties jdbcProperties) {
            this.jdbcProperties = jdbcProperties;
            return this;
        }

        public Builder<T> connectionPoolSize(Integer connectionPoolSize) {
            this.connectionPoolSize = connectionPoolSize;
            return this;
        }

        public Builder<T> hikariProperties(Properties hikariProperties) {
            this.hikariProperties = hikariProperties;
            return this;
        }

        public Builder<T> snapshotChunkEnabled(boolean snapshotChunkEnabled) {
            this.snapshotChunkEnabled = snapshotChunkEnabled;
            return this;
        }

        public Builder<T> snapshotChunkKeyColumn(String snapshotChunkKeyColumn) {
            this.snapshotChunkKeyColumn = snapshotChunkKeyColumn;
            return this;
        }

        public Builder<T> snapshotChunkSize(Integer snapshotChunkSize) {
            this.snapshotChunkSize = snapshotChunkSize;
            return this;
        }

        public Builder<T> logProxyHost(String logProxyHost) {
            this.logProxyHost = logProxyHost;
            return this;
        }

        public Builder<T> logProxyPort(int logProxyPort) {
            this.logProxyPort = logProxyPort;
            return this;
        }

        public Builder<T> logProxyClientId(String logProxyClientId) {
            this.logProxyClientId = logProxyClientId;
            return this;
        }

        public Builder<T> startupTimestamp(Long startupTimestamp) {
            this.startupTimestamp = startupTimestamp;
            return this;
        }

        public Builder<T> rsList(String rsList) {
            this.rsList = rsList;
            return this;
        }

        public Builder<T> configUrl(String configUrl) {
            this.configUrl = configUrl;
            return this;
        }

        public Builder<T> workingMode(String workingMode) {
            this.workingMode = workingMode;
            return this;
        }

        public Builder<T> deserializer(OceanBaseDeserializationSchema<T> deserializer) {
            this.deserializer = deserializer;
            return this;
        }

        public Builder<T> deserializer(DebeziumDeserializationSchema<T> debeziumDeserializer) {
            this.debeziumDeserializer = debeziumDeserializer;
            return this;
        }

        public Builder<T> decimalMode(String decimalMode) {
            this.decimalMode = decimalMode;
            return this;
        }

        public Builder<T> temporalPrecisionMode(String temporalPrecisionMode) {
            this.temporalPrecisionMode = temporalPrecisionMode;
            return this;
        }

        public Builder<T> bigintUnsignedHandlingMode(String bigintUnsignedHandlingMode) {
            this.bigintUnsignedHandlingMode = bigintUnsignedHandlingMode;
            return this;
        }

        public Builder<T> binaryHandlingMode(String binaryHandlingMode) {
            this.binaryHandlingMode = binaryHandlingMode;
            return this;
        }

        public SourceFunction<T> build() {
            switch (startupMode) {
                case INITIAL:
                case LATEST_OFFSET:
                    startupTimestamp = 0L;
                    break;
                case TIMESTAMP:
                    checkNotNull(
                            startupTimestamp,
                            "startupTimestamp shouldn't be null on startup mode 'timestamp'");
                    break;
                default:
                    throw new UnsupportedOperationException(
                            startupMode + " mode is not supported.");
            }

            if (startupMode.equals(StartupMode.INITIAL) || deserializer == null) {
                checkNotNull(hostname, "hostname shouldn't be null");
                checkNotNull(port, "port shouldn't be null");
            }

            if ((hostname == null || port == null)
                    && (StringUtils.isNotEmpty(databaseName)
                            || StringUtils.isNotEmpty(tableName))) {
                throw new IllegalArgumentException(
                        "If 'hostname' or 'port' is not configured, 'database-name' and 'table-name' must not be configured");
            }

            if (StringUtils.isNotEmpty(databaseName) || StringUtils.isNotEmpty(tableName)) {
                if (StringUtils.isEmpty(databaseName) || StringUtils.isEmpty(tableName)) {
                    throw new IllegalArgumentException(
                            "'database-name' and 'table-name' should be configured at the same time");
                }
            } else {
                checkNotNull(
                        tableList,
                        "'database-name', 'table-name' or 'table-list' should be configured");
            }

            if (compatibleMode == null) {
                compatibleMode = "mysql";
            }

            if (jdbcDriver == null) {
                jdbcDriver = "com.mysql.jdbc.Driver";
            }
            if (snapshotChunkEnabled == null) {
                snapshotChunkEnabled = true;
            }
            if (connectionPoolSize == null) {
                connectionPoolSize = 20;
            }
            if (!snapshotChunkEnabled) {
                snapshotChunkSize = null;
            } else if (snapshotChunkSize == null) {
                snapshotChunkSize = 1000;
            }

            if (serverTimeZone == null) {
                serverTimeZone = "+00:00";
            }

            if (connectTimeout == null) {
                connectTimeout = Duration.ofSeconds(30);
            }

            if (connectMaxRetries == null) {
                connectMaxRetries = 3;
            }

            if (logProxyClientId == null) {
                logProxyClientId =
                        String.format(
                                "%s_%s_%s",
                                ClientIdGenerator.generate(),
                                Thread.currentThread().getId(),
                                checkNotNull(tenantName));
            }
            ClientConf clientConf =
                    ClientConf.builder()
                            .clientId(logProxyClientId)
                            .connectTimeoutMs((int) connectTimeout.toMillis())
                            .build();

            ObReaderConfig obReaderConfig = new ObReaderConfig();
            if (StringUtils.isNotEmpty(rsList)) {
                obReaderConfig.setRsList(rsList);
            }
            if (StringUtils.isNotEmpty(configUrl)) {
                obReaderConfig.setClusterUrl(configUrl);
            }
            if (StringUtils.isNotEmpty(workingMode)) {
                obReaderConfig.setWorkingMode(workingMode);
            }
            obReaderConfig.setUsername(username);
            obReaderConfig.setPassword(password);
            obReaderConfig.setStartTimestamp(startupTimestamp);
            obReaderConfig.setTimezone(serverTimeZone);

            if (decimalMode == null) {
                decimalMode = "precise";
            }
            if (temporalPrecisionMode == null) {
                temporalPrecisionMode = "adaptive_time_microseconds";
            }
            if (bigintUnsignedHandlingMode == null) {
                bigintUnsignedHandlingMode = "long";
            }
            if (binaryHandlingMode == null) {
                binaryHandlingMode = "bytes";
            }

            return new OceanBaseRichSourceFunction<>(
                    StartupMode.INITIAL.equals(startupMode),
                    username,
                    password,
                    tenantName,
                    databaseName,
                    tableName,
                    tableList,
                    serverTimeZone,
                    connectTimeout,
                    connectMaxRetries,
                    hostname,
                    port,
                    compatibleMode,
                    jdbcDriver,
                    jdbcProperties,
                    connectionPoolSize,
                    hikariProperties,
                    snapshotChunkEnabled,
                    snapshotChunkKeyColumn,
                    snapshotChunkSize,
                    logProxyHost,
                    logProxyPort,
                    clientConf,
                    obReaderConfig,
                    deserializer,
                    debeziumDeserializer,
                    decimalMode,
                    temporalPrecisionMode,
                    bigintUnsignedHandlingMode,
                    binaryHandlingMode);
        }
    }
}
