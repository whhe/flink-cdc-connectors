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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/** OceanBase Snapshot Chunk. */
public class OceanBaseChunk {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseChunk.class);

    private final String table;
    private final List<String> indexNames;
    private final List<Object> left;
    private final List<Object> right;
    private final JdbcConnection.ResultSetConsumer resultSetConsumer;

    public OceanBaseChunk(
            String table,
            List<String> indexNames,
            List<Object> left,
            List<Object> right,
            JdbcConnection.ResultSetConsumer resultSetConsumer) {
        this.table = table;
        this.indexNames = indexNames;
        this.left = left;
        this.right = right;
        this.resultSetConsumer = resultSetConsumer;
    }

    public void read(OceanBaseConnectionProvider connectionProvider) throws SQLException {
        try (Connection connection = connectionProvider.getConnection();
                Statement statement = connection.createStatement()) {
            String sql =
                    connectionProvider
                            .getDialect()
                            .getQueryChunkSql(table, indexNames, left, right);
            LOG.info("Query sql: " + sql);
            ResultSet resultSet = statement.executeQuery(sql);
            resultSetConsumer.accept(resultSet);
        }
    }
}
