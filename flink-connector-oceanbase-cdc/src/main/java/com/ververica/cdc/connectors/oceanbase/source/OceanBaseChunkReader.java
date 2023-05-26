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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** OceanBase Snapshot Chunk Reader. */
public class OceanBaseChunkReader implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseChunkReader.class);

    private final OceanBaseConnectionProvider connectionProvider;
    private final BlockingQueue<OceanBaseChunk> chunks;
    private final ExecutorService executor;
    private transient volatile Exception exception;
    private transient volatile boolean closed = false;

    public OceanBaseChunkReader(OceanBaseConnectionProvider connectionProvider, int threadNum) {
        this.connectionProvider = connectionProvider;
        this.chunks = new LinkedBlockingQueue<>();
        this.executor = Executors.newFixedThreadPool(threadNum);
        for (int i = 0; i < threadNum; i++) {
            this.executor.execute(
                    () -> {
                        try {
                            while (!closed) {
                                OceanBaseChunk chunk = chunks.take();
                                chunk.read(connectionProvider);
                            }
                        } catch (InterruptedException e) {
                            LOG.info("OceanBaseChunkReader.executor is interrupted");
                        } catch (Exception e) {
                            this.exception = e;
                        }
                    });
        }
    }

    public void splitChunks(
            String table,
            List<String> indexNames,
            int chunkSize,
            JdbcConnection.ResultSetConsumer resultSetConsumer)
            throws Exception {
        checkException();
        try (Connection connection = connectionProvider.getConnection();
                Statement statement = connection.createStatement()) {
            List<Object> left = null;
            List<Object> right = null;
            do {
                String sql =
                        connectionProvider
                                .getDialect()
                                .getQueryNewChunkBoundSql(table, indexNames, right, chunkSize);
                LOG.info("Query sql: " + sql);

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
                chunks.put(new OceanBaseChunk(table, indexNames, left, right, resultSetConsumer));
            } while (right != null);
        }
    }

    private void checkException() {
        if (exception != null) {
            throw new RuntimeException("Read chunk failed", exception);
        }
    }

    public boolean done() {
        checkException();
        return chunks.isEmpty();
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
