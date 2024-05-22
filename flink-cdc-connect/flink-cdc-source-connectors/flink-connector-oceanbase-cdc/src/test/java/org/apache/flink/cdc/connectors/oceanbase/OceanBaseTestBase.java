/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.oceanbase;

import org.apache.flink.cdc.connectors.oceanbase.source.connection.OceanBaseConnection;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.util.AbstractTestBase;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Basic class for testing OceanBase source. */
public abstract class OceanBaseTestBase extends AbstractTestBase {

    private static final Pattern COMMENT_PATTERN = Pattern.compile("^(.*)--.*$");

    protected abstract String getCompatibleMode();

    protected abstract Connection getJdbcConnection() throws SQLException;

    public OceanBaseConnection getConnection(
            String host, int port, String username, String password) {
        return new OceanBaseConnection(
                host,
                port,
                username,
                password,
                Duration.ofMillis(1000),
                getCompatibleMode(),
                "mysql".equalsIgnoreCase(getCompatibleMode())
                        ? "com.mysql.cj.jdbc.Driver"
                        : "com.oceanbase.jdbc.Driver",
                null,
                getClass().getClassLoader());
    }

    public void setGlobalTimeZone(String serverTimeZone) throws SQLException {
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(String.format("SET GLOBAL time_zone = '%s';", serverTimeZone));
        }
    }

    public void initializeTable(String sqlFile) {
        final String ddlFile = String.format("ddl/%s/%s.sql", getCompatibleMode(), sqlFile);
        final URL ddlTestFile = getClass().getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            final List<String> statements =
                    Arrays.stream(
                                    Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                                            .map(String::trim)
                                            .filter(x -> !x.startsWith("--") && !x.isEmpty())
                                            .map(
                                                    x -> {
                                                        final Matcher m =
                                                                COMMENT_PATTERN.matcher(x);
                                                        return m.matches() ? m.group(1) : x;
                                                    })
                                            .collect(Collectors.joining("\n"))
                                            .split(";"))
                            .collect(Collectors.toList());
            for (String stmt : statements) {
                statement.execute(stmt);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void waitForSinkSize(String sinkName, int expectedSize)
            throws InterruptedException {
        while (sinkSize(sinkName) < expectedSize) {
            Thread.sleep(100);
        }
    }

    public static int sinkSize(String sinkName) {
        synchronized (TestValuesTableFactory.class) {
            try {
                return TestValuesTableFactory.getRawResults(sinkName).size();
            } catch (IllegalArgumentException e) {
                // job is not started yet
                return 0;
            }
        }
    }

    public static void assertContainsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertTrue(
                String.format("expected: %s, actual: %s", expected, actual),
                actual.containsAll(expected));
    }
}
