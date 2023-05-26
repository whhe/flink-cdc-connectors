/*
 * Copyright 2022 Ververica Inc.
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

package com.ververica.cdc.connectors.oceanbase.source;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;

/** OceanBase connection provider. */
public class OceanBaseConnectionProvider implements AutoCloseable {

    private static final Properties DEFAULT_JDBC_PROPERTIES = initializeDefaultJdbcProperties();
    private static final String MYSQL_URL_PATTERN =
            "jdbc:mysql://${hostname}:${port}/?connectTimeout=${connectTimeout}";
    private static final String OB_URL_PATTERN =
            "jdbc:oceanbase://${hostname}:${port}/?connectTimeout=${connectTimeout}";

    private final DruidDataSource dataSource;
    private final OceanBaseDialect dialect;

    public OceanBaseConnectionProvider(
            String hostname,
            int port,
            String username,
            String password,
            Duration connectTimeout,
            String compatibleMode,
            String jdbcDriver,
            Properties jdbcProperties,
            int threadNum) {

        if ("mysql".equalsIgnoreCase(compatibleMode)) {
            this.dialect = new OceanBaseMysqlDialect();
        } else {
            throw new UnsupportedOperationException("Oracle dialect not supported for now");
        }
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setUrl(
                formatJdbcUrl(hostname, port, connectTimeout, jdbcDriver, jdbcProperties));
        druidDataSource.setUsername(username);
        druidDataSource.setPassword(password);
        druidDataSource.setDriverClassName(jdbcDriver);
        druidDataSource.setInitialSize(threadNum);
        druidDataSource.setMaxActive(threadNum + 4);
        druidDataSource.setValidationQuery(dialect.getValidationSql());
        dataSource = druidDataSource;
    }

    private static String formatJdbcUrl(
            String hostname,
            int port,
            Duration connectTimeout,
            String jdbcDriver,
            Properties jdbcProperties) {
        Properties combinedProperties = new Properties();
        combinedProperties.putAll(DEFAULT_JDBC_PROPERTIES);
        if (jdbcProperties != null) {
            combinedProperties.putAll(jdbcProperties);
        }
        String urlPattern =
                jdbcDriver.toLowerCase().contains("oceanbase") ? OB_URL_PATTERN : MYSQL_URL_PATTERN;
        urlPattern = replace(urlPattern, "hostname", hostname);
        urlPattern = replace(urlPattern, "port", port);
        urlPattern =
                replace(
                        urlPattern,
                        "connectTimeout",
                        connectTimeout == null ? 30000 : connectTimeout.toMillis());

        StringBuilder jdbcUrlStringBuilder = new StringBuilder(urlPattern);
        combinedProperties.forEach(
                (key, value) -> {
                    jdbcUrlStringBuilder.append("&").append(key).append("=").append(value);
                });
        return jdbcUrlStringBuilder.toString();
    }

    private static String replace(String url, String name, Object value) {
        return url.replaceAll("\\$\\{" + name + "\\}", value != null ? value.toString() : null);
    }

    private static Properties initializeDefaultJdbcProperties() {
        Properties defaultJdbcProperties = new Properties();
        defaultJdbcProperties.setProperty("useInformationSchema", "true");
        defaultJdbcProperties.setProperty("nullCatalogMeansCurrent", "false");
        defaultJdbcProperties.setProperty("useUnicode", "true");
        defaultJdbcProperties.setProperty("zeroDateTimeBehavior", "convertToNull");
        defaultJdbcProperties.setProperty("characterEncoding", "UTF-8");
        defaultJdbcProperties.setProperty("characterSetResults", "UTF-8");
        return defaultJdbcProperties;
    }

    public OceanBaseDialect getDialect() {
        return dialect;
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
