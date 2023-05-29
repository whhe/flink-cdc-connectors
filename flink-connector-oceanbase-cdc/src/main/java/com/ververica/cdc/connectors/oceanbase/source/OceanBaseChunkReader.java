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

import io.debezium.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** OceanBase Snapshot Chunk Reader. */
public class OceanBaseChunkReader implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseChunkReader.class);

    private final OceanBaseConnectionProvider connectionProvider;
    private final Integer threadNum;
    private final BlockingQueue<OceanBaseChunk> chunks;
    private final ExecutorService executor;
    private final CompletionService<Boolean> completionService;
    private final AtomicBoolean waiting = new AtomicBoolean();
    private transient volatile Exception exception;
    private transient volatile boolean closed = false;

    public OceanBaseChunkReader(OceanBaseConnectionProvider connectionProvider, int threadNum) {
        this.connectionProvider = connectionProvider;
        this.threadNum = threadNum;
        this.chunks = new LinkedBlockingQueue<>(threadNum * 2);
        this.executor = Executors.newFixedThreadPool(threadNum);
        this.completionService = new ExecutorCompletionService<>(executor);
        for (int i = 0; i < threadNum; i++) {
            this.completionService.submit(
                    () -> {
                        while (!this.closed) {
                            if (waiting.get() && chunks.isEmpty()) {
                                return true;
                            }
                            try {
                                OceanBaseChunk chunk = chunks.poll(1000, TimeUnit.MILLISECONDS);
                                if (chunk != null) {
                                    chunk.read(connectionProvider);
                                }
                            } catch (Exception e) {
                                if (e instanceof InterruptedException) {
                                    LOG.info("OceanBaseChunkReader.executor is interrupted");
                                } else {
                                    this.exception = e;
                                }
                                return false;
                            }
                        }
                        return true;
                    });
        }
    }

    public void splitChunks(
            String table,
            List<String> indexNames,
            int chunkSize,
            JdbcConnection.ResultSetConsumer resultSetConsumer)
            throws Exception {
        try (Connection connection = connectionProvider.getConnection();
                Statement statement = connection.createStatement()) {
            List<Object> left = null;
            List<Object> right = null;
            do {
                checkException();
                String sql =
                        connectionProvider
                                .getDialect()
                                .getQueryNewChunkBoundSql(table, indexNames, right, chunkSize);
                LOG.info("Query chunk bound sql: " + sql);

                ResultSet rs = statement.executeQuery(sql);
                List<Object> result;
                if (rs.next()) {
                    result = new ArrayList<>();
                    for (String indexName : indexNames) {
                        result.add(rs.getObject(indexName));
                    }
                } else {
                    result = null;
                }
                left = right;
                right = result;
                chunks.put(
                        new OceanBaseChunk(
                                table, indexNames, left, right, chunkSize, resultSetConsumer));
            } while (right != null);
        }
    }

    private void checkException() {
        if (exception != null) {
            throw new RuntimeException("Read chunk failed", exception);
        }
    }

    public void waitTermination() throws InterruptedException {
        this.waiting.set(true);
        for (int i = 0; i < threadNum; i++) {
            completionService.take();
        }
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                }
            }
        }
    }
}
