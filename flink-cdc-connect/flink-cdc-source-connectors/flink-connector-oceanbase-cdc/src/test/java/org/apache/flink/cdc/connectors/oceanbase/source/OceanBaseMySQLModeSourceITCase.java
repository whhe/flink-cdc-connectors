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

package org.apache.flink.cdc.connectors.oceanbase.source;

import org.apache.flink.cdc.connectors.oceanbase.OceanBaseContainerUtils;
import org.apache.flink.cdc.connectors.oceanbase.OceanBaseTestBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import io.debezium.jdbc.JdbcConnection;
import org.apache.commons.lang3.StringUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.GenericContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.apache.flink.cdc.connectors.oceanbase.OceanBaseContainerUtils.createLogProxy;
import static org.apache.flink.cdc.connectors.oceanbase.OceanBaseContainerUtils.createOceanBaseServer;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertFalse;

/** IT tests for OceanBase MySQL mode source. */
@RunWith(Parameterized.class)
public class OceanBaseMySQLModeSourceITCase extends OceanBaseTestBase {

    private static final String DEFAULT_SCAN_STARTUP_MODE = "initial";

    @ClassRule public static final GenericContainer<?> OB_SERVER = createOceanBaseServer();
    @ClassRule public static final GenericContainer<?> LOG_PROXY = createLogProxy();

    @Rule public final Timeout timeoutPerTest = Timeout.seconds(300);

    /** Initial changelogs in string of table "customers" in database "customer". */
    private final List<String> initialChanges =
            Arrays.asList(
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]");

    /** First part binlog events in string, which is made by {@link #makeFirstPartChange}. */
    private final List<String> firstPartBinlogEvents =
            Arrays.asList(
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Hangzhou, 123567891234]",
                    "+U[103, user_3, Shanghai, 123567891234]");

    /** Second part binlog events in string, which is made by {@link #makeSecondPartChange}. */
    private final List<String> secondPartBinlogEvents =
            Arrays.asList(
                    "-U[1010, user_11, Shanghai, 123567891234]",
                    "+I[2001, user_22, Shanghai, 123567891234]",
                    "+I[2002, user_23, Shanghai, 123567891234]",
                    "+I[2003, user_24, Shanghai, 123567891234]",
                    "+U[1010, user_11, Hangzhou, 123567891234]");

    @Override
    protected String getCompatibleMode() {
        return "mysql";
    }

    @Override
    protected Connection getJdbcConnection() throws SQLException {
        return OceanBaseContainerUtils.getJdbcConnection();
    }

    @Parameterized.Parameter public String tableName;

    @Parameterized.Parameter(1)
    public String chunkColumnName;

    @Parameterized.Parameters(name = "table: {0}, chunkColumn: {1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[][] {
                    {"customers", null}, {"customers", "id"}, {"customers_no_pk", "id"}
                });
    }

    @Test
    public void testReadSingleTableWithSingleParallelism() throws Exception {
        testOceanBaseIncrementalSource(1, new String[] {tableName});
    }

    @Test
    public void testReadSingleTableWithSingleParallelismAndSkipBackFill() throws Exception {
        testOceanBaseIncrementalSource(
                1, DEFAULT_SCAN_STARTUP_MODE, new String[] {tableName}, true);
    }

    @Test
    public void testReadMultipleTableWithSingleParallelism() throws Exception {
        testOceanBaseIncrementalSource(1, new String[] {tableName, "customers_1"});
    }

    @Test
    public void testReadMultipleTableWithMultipleParallelism() throws Exception {
        testOceanBaseIncrementalSource(4, new String[] {tableName, "customers_1"});
    }

    private void testOceanBaseIncrementalSource(int parallelism, String[] captureCustomerTables)
            throws Exception {
        testOceanBaseIncrementalSource(
                parallelism, DEFAULT_SCAN_STARTUP_MODE, captureCustomerTables, false);
    }

    private void testOceanBaseIncrementalSource(
            int parallelism,
            String scanStartupMode,
            String[] captureCustomerTables,
            boolean skipSnapshotBackfill)
            throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);

        String databaseName = getTestDatabase("customer");

        String sourceDDL =
                format(
                        "CREATE TABLE customers ("
                                + " id BIGINT NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING"
                                + ("customers_no_pk".equals(tableName)
                                        ? ""
                                        : ", primary key (id) not enforced")
                                + ") WITH ("
                                + " 'connector' = 'oceanbase-cdc',"
                                + " 'scan.incremental.snapshot.enabled' = 'true',"
                                + " 'hostname' = '%s',"
                                + " 'port' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'logproxy.host' = '%s',"
                                + " 'logproxy.port' = '%s',"
                                + " 'working-mode' = 'memory',"
                                + " 'tenant-name' = '%s',"
                                + " 'compatible-mode' = '%s',"
                                + " 'database-name' = '%s',"
                                + " 'table-name' = '%s',"
                                + " 'scan.startup.mode' = '%s',"
                                + " 'scan.incremental.snapshot.chunk.size' = '100',"
                                + " 'scan.incremental.snapshot.backfill.skip' = '%s',"
                                + " 'server-time-zone' = 'UTC'"
                                + " %s"
                                + ")",
                        OceanBaseContainerUtils.OB_SERVER_HOST,
                        OceanBaseContainerUtils.OB_SERVER_PORT,
                        OceanBaseContainerUtils.USERNAME,
                        OceanBaseContainerUtils.PASSWORD,
                        OceanBaseContainerUtils.LOG_PROXY_HOST,
                        OceanBaseContainerUtils.LOG_PROXY_PORT,
                        OceanBaseContainerUtils.TENANT,
                        getCompatibleMode(),
                        databaseName,
                        getTableNameRegex(captureCustomerTables),
                        scanStartupMode,
                        skipSnapshotBackfill,
                        chunkColumnName == null
                                ? ""
                                : String.format(
                                        ", 'scan.incremental.snapshot.chunk.key-column' = '%s'",
                                        chunkColumnName));
        tEnv.executeSql(sourceDDL);
        TableResult tableResult = tEnv.executeSql("select * from customers");

        if (DEFAULT_SCAN_STARTUP_MODE.equals(scanStartupMode)) {
            checkSnapshotData(tableResult, captureCustomerTables);
        }

        checkChangeData(tableResult, databaseName, captureCustomerTables);

        tableResult.getJobClient().get().cancel().get();
    }

    private String getTestDatabase(String databaseName) throws SQLException {
        databaseName = databaseName + "_" + Integer.toUnsignedString(new Random().nextInt(), 36);
        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + databaseName);
        }
        return databaseName;
    }

    private void checkSnapshotData(TableResult tableResult, String[] captureCustomerTables)
            throws Exception {
        String[] snapshotForSingleTable =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]"
                };

        List<String> expectedSnapshotData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedSnapshotData.addAll(Arrays.asList(snapshotForSingleTable));
        }

        CloseableIterator<Row> iterator = tableResult.collect();
        assertContainsInAnyOrder(
                expectedSnapshotData, fetchRows(iterator, expectedSnapshotData.size()));
    }

    private void checkChangeData(
            TableResult tableResult, String databaseName, String[] captureCustomerTables)
            throws Exception {
        waitUntilJobRunning(tableResult);
        CloseableIterator<Row> iterator = tableResult.collect();

        for (String tableId : captureCustomerTables) {
            makeFirstPartChange(getConnection(), databaseName + '.' + tableId);
        }

        Thread.sleep(2000L);

        for (String tableId : captureCustomerTables) {
            makeSecondPartChange(getConnection(), databaseName + '.' + tableId);
        }

        List<String> expectedBinlogData = new ArrayList<>();
        for (int i = 0; i < captureCustomerTables.length; i++) {
            expectedBinlogData.addAll(firstPartBinlogEvents);
            expectedBinlogData.addAll(secondPartBinlogEvents);
        }

        assertContainsInAnyOrder(
                expectedBinlogData, fetchRows(iterator, expectedBinlogData.size()));
        assertFalse(hasNextData(iterator));
    }

    private static List<String> fetchRows(Iterator<Row> iter, int size) {
        List<String> rows = new ArrayList<>(size);
        while (size > 0 && iter.hasNext()) {
            Row row = iter.next();
            rows.add(row.toString());
            size--;
        }
        return rows;
    }

    private String getTableNameRegex(String[] captureCustomerTables) {
        checkState(captureCustomerTables.length > 0);
        if (captureCustomerTables.length == 1) {
            return captureCustomerTables[0];
        } else {
            // pattern that matches multiple tables
            return format("(%s)", StringUtils.join(captureCustomerTables, "|"));
        }
    }

    private void makeFirstPartChange(JdbcConnection connection, String tableId)
            throws SQLException {
        try {
            connection.setAutoCommit(false);

            // make binlog events for the first split
            connection.execute(
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                    "DELETE FROM " + tableId + " where id = 102",
                    "INSERT INTO " + tableId + " VALUES(102, 'user_2','Shanghai','123567891234')",
                    "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103");
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private void makeSecondPartChange(JdbcConnection connection, String tableId)
            throws SQLException {
        try {
            connection.setAutoCommit(false);

            // make binlog events for split-1
            connection.execute("UPDATE " + tableId + " SET address = 'Hangzhou' where id = 1010");
            connection.commit();

            // make binlog events for the last split
            connection.execute(
                    "INSERT INTO "
                            + tableId
                            + " VALUES(2001, 'user_22','Shanghai','123567891234'),"
                            + " (2002, 'user_23','Shanghai','123567891234'),"
                            + "(2003, 'user_24','Shanghai','123567891234')");
            connection.commit();
        } finally {
            connection.close();
        }
    }

    private JdbcConnection getConnection() {
        return getConnection(
                OceanBaseContainerUtils.OB_SERVER_HOST,
                OceanBaseContainerUtils.OB_SERVER_PORT,
                OceanBaseContainerUtils.USERNAME,
                OceanBaseContainerUtils.PASSWORD);
    }

    private void waitUntilJobRunning(TableResult tableResult)
            throws InterruptedException, ExecutionException {
        do {
            Thread.sleep(5000L);
        } while (tableResult.getJobClient().get().getJobStatus().get() != RUNNING);
    }

    private boolean hasNextData(final CloseableIterator<?> iterator)
            throws InterruptedException, ExecutionException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            FutureTask<Boolean> future = new FutureTask(iterator::hasNext);
            executor.execute(future);
            return future.get(3, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            return false;
        } finally {
            executor.shutdown();
        }
    }
}
