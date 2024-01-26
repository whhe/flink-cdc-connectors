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

package com.ververica.cdc.connectors.oceanbase.source;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ClientConf;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import com.oceanbase.clogproxy.client.exception.LogProxyClientException;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.oms.logmessage.LogMessage;
import com.ververica.cdc.connectors.oceanbase.table.OceanBaseDeserializationSchema;
import com.ververica.cdc.connectors.oceanbase.table.OceanBaseRecord;
import com.ververica.cdc.connectors.oceanbase.table.OceanBaseTableSchema;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableSchema;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The source implementation for OceanBase that read snapshot events first and then read the change
 * event.
 *
 * @param <T> The type created by the deserializer.
 */
public class OceanBaseRichSourceFunction<T> extends RichSourceFunction<T>
        implements CheckpointListener, CheckpointedFunction, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 2844054619864617340L;

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseRichSourceFunction.class);

    private final boolean snapshot;
    private final String username;
    private final String password;
    private final String tenantName;
    private final String databaseName;
    private final String tableName;
    private final String tableList;
    private final Duration connectTimeout;
    private final Integer connectMaxRetries;
    private final String hostname;
    private final Integer port;
    private final String compatibleMode;
    private final OceanBaseDialect dialect;
    private final String jdbcDriver;
    private final Properties jdbcProperties;
    private final Integer connectionPoolSize;
    private final Properties hikariProperties;
    private final boolean snapshotChunkEnabled;
    private final String snapshotChunkKeyColumn;
    private final Integer snapshotChunkSize;
    private final String logProxyHost;
    private final int logProxyPort;
    private final ClientConf logProxyClientConf;
    private final ObReaderConfig obReaderConfig;
    private final OceanBaseDeserializationSchema<T> deserializer;
    private final DebeziumDeserializationSchema<T> debeziumDeserializer;
    private final String decimalMode;
    private final String temporalPrecisionMode;
    private final String bigintUnsignedHandlingMode;
    private final String binaryHandlingMode;

    private final AtomicBoolean snapshotCompleted = new AtomicBoolean(false);
    private final List<OceanBaseRecord> changeRecordBuffer = new LinkedList<>();

    private transient Set<String> tableSet;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, OceanBaseSnapshotChunkBound> resolvedChunkBound =
            new ConcurrentHashMap<>();
    private final Map<String, List<OceanBaseSnapshotChunkReader>> resolvedChunkReaderCache =
            new ConcurrentHashMap<>();
    private transient volatile long resolvedTimestamp;
    private transient volatile long startTimestamp;
    private transient OceanBaseDataSource dataSource;
    private transient OceanBaseSnapshotChunkSplitter chunkSplitter;
    private transient LogProxyClient logProxyClient;
    private transient ListState<Map<String, String>> chunkReaderState;
    private transient ListState<Long> changeStreamState;
    private transient OutputCollector<T> outputCollector;

    public OceanBaseRichSourceFunction(
            boolean snapshot,
            String username,
            String password,
            String tenantName,
            String databaseName,
            String tableName,
            String tableList,
            Duration connectTimeout,
            Integer connectMaxRetries,
            String hostname,
            Integer port,
            String compatibleMode,
            String jdbcDriver,
            Properties jdbcProperties,
            Integer connectionPoolSize,
            Properties hikariProperties,
            boolean snapshotChunkEnabled,
            String snapshotChunkKeyColumn,
            Integer snapshotChunkSize,
            String logProxyHost,
            int logProxyPort,
            ClientConf logProxyClientConf,
            ObReaderConfig obReaderConfig,
            OceanBaseDeserializationSchema<T> deserializer,
            DebeziumDeserializationSchema<T> debeziumDeserializer,
            String decimalMode,
            String temporalPrecisionMode,
            String bigintUnsignedHandlingMode,
            String binaryHandlingMode) {
        this.snapshot = checkNotNull(snapshot);
        this.username = checkNotNull(username);
        this.password = checkNotNull(password);
        this.tenantName = checkNotNull(tenantName);
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableList = tableList;
        this.connectTimeout = checkNotNull(connectTimeout);
        this.connectMaxRetries = connectMaxRetries;
        this.hostname = hostname;
        this.port = port;
        this.compatibleMode = compatibleMode;
        this.dialect =
                "mysql".equalsIgnoreCase(compatibleMode)
                        ? new OceanBaseMysqlDialect()
                        : new OceanBaseOracleDialect();
        this.jdbcDriver = jdbcDriver;
        this.jdbcProperties = jdbcProperties;
        this.connectionPoolSize = connectionPoolSize;
        this.hikariProperties = hikariProperties;
        this.snapshotChunkEnabled = snapshotChunkEnabled;
        this.snapshotChunkKeyColumn = snapshotChunkKeyColumn;
        this.snapshotChunkSize = snapshotChunkSize;
        this.logProxyHost = checkNotNull(logProxyHost);
        this.logProxyPort = checkNotNull(logProxyPort);
        this.logProxyClientConf = checkNotNull(logProxyClientConf);
        this.obReaderConfig = checkNotNull(obReaderConfig);
        this.deserializer = deserializer;
        this.debeziumDeserializer = debeziumDeserializer;
        if (deserializer == null && debeziumDeserializer == null) {
            throw new IllegalArgumentException("deserializer should not be null");
        }
        this.decimalMode = decimalMode;
        this.temporalPrecisionMode = temporalPrecisionMode;
        this.bigintUnsignedHandlingMode = bigintUnsignedHandlingMode;
        this.binaryHandlingMode = binaryHandlingMode;
    }

    @Override
    public void open(final Configuration config) throws Exception {
        super.open(config);
        this.outputCollector = new OutputCollector<>();
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        outputCollector.context = ctx;
        try {
            LOG.info("Start to initial table whitelist");
            initTableWhiteList();

            LOG.info("Start readChangeRecords process");
            readChangeRecords();

            if (shouldReadSnapshot()) {
                LOG.info("Snapshot reading started");
                readSnapshotRecords();
                LOG.info("Snapshot reading finished");
            } else {
                LOG.info("Snapshot reading skipped");
            }

            logProxyClient.join();
        } finally {
            cancel();
        }
    }

    private boolean shouldReadSnapshot() {
        return resolvedTimestamp <= 0 && snapshot;
    }

    private OceanBaseDataSource getDataSource() {
        if (dataSource == null) {
            dataSource =
                    new OceanBaseDataSource(
                            hostname,
                            port,
                            username,
                            password,
                            connectTimeout,
                            connectMaxRetries,
                            jdbcDriver,
                            jdbcProperties,
                            connectionPoolSize,
                            hikariProperties);
        }
        return dataSource;
    }

    private void initTableWhiteList() {
        if (tableSet != null && !tableSet.isEmpty()) {
            return;
        }

        final Set<String> localTableSet = new HashSet<>();

        if (StringUtils.isNotBlank(tableList)) {
            for (String s : tableList.split(",")) {
                if (StringUtils.isNotBlank(s)) {
                    String[] schema = s.split("\\.");
                    localTableSet.add(String.format("%s.%s", schema[0].trim(), schema[1].trim()));
                }
            }
        }

        if (StringUtils.isNotBlank(databaseName) && StringUtils.isNotBlank(tableName)) {
            try {
                List<String> tables =
                        OceanBaseJdbcUtils.getTables(
                                getDataSource(), dialect, databaseName, tableName);
                LOG.info("Pattern matched tables: {}", tables);
                localTableSet.addAll(tables);
            } catch (SQLException e) {
                LOG.error(
                        String.format(
                                "Query table list by 'databaseName' %s and 'tableName' %s failed.",
                                databaseName, tableName),
                        e);
                throw new FlinkRuntimeException(e);
            }
        }

        if (localTableSet.isEmpty()) {
            throw new FlinkRuntimeException("No valid table found");
        }

        LOG.info("Table list: {}", localTableSet);
        this.tableSet = localTableSet;
        this.obReaderConfig.setTableWhiteList(String.format("%s.*.*", tenantName));
    }

    void readSnapshotRecords() throws Exception {
        chunkSplitter =
                new OceanBaseSnapshotChunkSplitter(
                        getDataSource(), connectionPoolSize, dialect, getCheckpointListener());
        for (String table : tableSet) {
            OceanBaseSnapshotChunkBound startChunkBound =
                    resolvedChunkBound.computeIfAbsent(
                            table, k -> OceanBaseSnapshotChunkBound.START_BOUND);
            if (OceanBaseSnapshotChunkBound.END_BOUND.equals(startChunkBound)) {
                continue;
            }

            String[] schema = table.split("\\.");
            String dbName = schema[0];
            String tableName = schema[1];
            OceanBaseRecord.SourceInfo sourceInfo =
                    new OceanBaseRecord.SourceInfo(
                            tenantName, dbName, tableName, resolvedTimestamp);

            List<String> chunkKeyColumns;
            if (!snapshotChunkEnabled) {
                chunkKeyColumns = null;
            } else {
                if (StringUtils.isNoneBlank(snapshotChunkKeyColumn)) {
                    chunkKeyColumns =
                            Arrays.stream(snapshotChunkKeyColumn.split(","))
                                    .map(String::trim)
                                    .collect(Collectors.toList());
                } else {
                    chunkKeyColumns =
                            OceanBaseJdbcUtils.getChunkKeyColumns(
                                    getDataSource(), dialect, dbName, tableName);
                }
            }

            chunkSplitter.split(
                    dbName,
                    tableName,
                    chunkKeyColumns,
                    startChunkBound,
                    snapshotChunkSize,
                    getResultSetConsumer(sourceInfo));
        }

        chunkSplitter.waitTermination();
        chunkSplitter.close();
        chunkSplitter.checkException();

        snapshotCompleted.set(true);
        resolvedTimestamp = startTimestamp;
    }

    private OceanBaseSnapshotChunkSplitter.ReadCompleteListener getCheckpointListener() {
        return chunkReader -> {
            String table =
                    String.format("%s.%s", chunkReader.getDbName(), chunkReader.getTableName());
            OceanBaseSnapshotChunkBound stateBound = resolvedChunkBound.get(table);
            if (chunkReader.getLowerBound().equals(stateBound)) {
                resolvedChunkBound.put(table, chunkReader.getUpperBound());
            } else {
                List<OceanBaseSnapshotChunkReader> cachedReaderList =
                        resolvedChunkReaderCache.computeIfAbsent(table, k -> new ArrayList<>());
                synchronized (cachedReaderList) {
                    cachedReaderList.add(chunkReader);
                    flushResolvedChunkCache(table);
                }
            }
        };
    }

    private void flushResolvedChunkCache(String table) {
        List<OceanBaseSnapshotChunkReader> cachedReaderList = resolvedChunkReaderCache.get(table);
        if (cachedReaderList == null || cachedReaderList.isEmpty()) {
            return;
        }
        OceanBaseSnapshotChunkBound stateBound =
                resolvedChunkBound.computeIfAbsent(
                        table, k -> OceanBaseSnapshotChunkBound.START_BOUND);
        List<OceanBaseSnapshotChunkReader> matchedList = new LinkedList<>();
        for (OceanBaseSnapshotChunkReader reader :
                cachedReaderList.stream()
                        .sorted(Comparator.comparingInt(OceanBaseSnapshotChunkReader::getChunkId))
                        .collect(Collectors.toList())) {
            if (reader.getLowerBound().equals(stateBound)) {
                matchedList.add(reader);
                stateBound = reader.getUpperBound();
            } else {
                break;
            }
        }
        if (matchedList.isEmpty()) {
            return;
        }
        resolvedChunkBound.put(table, stateBound);
        cachedReaderList.removeAll(matchedList);
        if (cachedReaderList.isEmpty()) {
            resolvedChunkReaderCache.remove(table);
        } else {
            resolvedChunkReaderCache.put(table, cachedReaderList);
        }
    }

    private JdbcConnection.ResultSetConsumer getResultSetConsumer(
            OceanBaseRecord.SourceInfo sourceInfo) {
        return rs -> {
            if (deserializer != null) {
                ResultSetMetaData metaData = rs.getMetaData();
                while (rs.next()) {
                    Map<String, Object> fieldMap = new HashMap<>();
                    for (int i = 0; i < metaData.getColumnCount(); i++) {
                        fieldMap.put(metaData.getColumnName(i + 1), rs.getObject(i + 1));
                    }
                    OceanBaseRecord record = new OceanBaseRecord(sourceInfo, fieldMap);
                    try {
                        deserializer.deserialize(record, outputCollector);
                    } catch (Exception e) {
                        LOG.error("Deserialize snapshot record failed ", e);
                        throw new FlinkRuntimeException(e);
                    }
                }
            } else {
                assert debeziumDeserializer != null;
                String topicName =
                        String.format(
                                "%s.%s.%s",
                                sourceInfo.getTenant(),
                                sourceInfo.getDatabase(),
                                sourceInfo.getTable());
                Map<String, String> partition =
                        getSourcePartition(
                                sourceInfo.getTenant(),
                                sourceInfo.getDatabase(),
                                sourceInfo.getTable());
                // the offset here is useless
                Map<String, Object> offset = getSourceOffset(resolvedTimestamp);

                String db, schema;
                if ("mysql".equalsIgnoreCase(compatibleMode)) {
                    db = sourceInfo.getDatabase();
                    schema = null;
                } else {
                    db = null;
                    schema = sourceInfo.getDatabase();
                }
                Struct source =
                        OceanBaseTableSchema.sourceStruct(
                                sourceInfo.getTenant(), db, schema, sourceInfo.getTable(), null);
                TableSchema tableSchema =
                        OceanBaseTableSchema.getTableSchema(
                                getDataSource(),
                                compatibleMode,
                                sourceInfo.getTenant(),
                                db,
                                schema,
                                sourceInfo.getTable(),
                                decimalMode,
                                temporalPrecisionMode,
                                bigintUnsignedHandlingMode,
                                binaryHandlingMode);

                List<Field> fields = tableSchema.valueSchema().fields();

                while (rs.next()) {
                    Object[] fieldValues = new Object[fields.size()];
                    for (Field field : fields) {
                        fieldValues[field.index()] = rs.getObject(field.name());
                    }
                    Struct value = tableSchema.valueFromColumnData(fieldValues);
                    Struct struct =
                            tableSchema.getEnvelopeSchema().read(value, source, Instant.now());
                    try {
                        debeziumDeserializer.deserialize(
                                new SourceRecord(
                                        partition,
                                        offset,
                                        topicName,
                                        null,
                                        null,
                                        null,
                                        struct.schema(),
                                        struct),
                                outputCollector);
                    } catch (Exception e) {
                        LOG.error("Deserialize snapshot record failed ", e);
                        throw new FlinkRuntimeException(e);
                    }
                }
            }
        };
    }

    private Map<String, String> getSourcePartition(
            String tenantName, String databaseName, String tableName) {
        Map<String, String> sourcePartition = new HashMap<>();
        sourcePartition.put("tenant", tenantName);
        sourcePartition.put("database", databaseName);
        sourcePartition.put("table", tableName);
        return sourcePartition;
    }

    private Map<String, Object> getSourceOffset(long timestamp) {
        Map<String, Object> sourceOffset = new HashMap<>();
        sourceOffset.put("timestamp", timestamp);
        return sourceOffset;
    }

    protected void readChangeRecords() throws InterruptedException, TimeoutException {
        if (resolvedTimestamp > 0) {
            obReaderConfig.updateCheckpoint(Long.toString(resolvedTimestamp));
            LOG.info("Try to start LogProxyClient from timestamp: {}", resolvedTimestamp);
        }

        logProxyClient =
                new LogProxyClient(logProxyHost, logProxyPort, obReaderConfig, logProxyClientConf);

        final CountDownLatch latch = new CountDownLatch(1);

        logProxyClient.addListener(
                new RecordListener() {

                    boolean started = false;

                    @Override
                    public void notify(LogMessage message) {
                        switch (message.getOpt()) {
                            case HEARTBEAT:
                            case BEGIN:
                                if (!started) {
                                    started = true;
                                    startTimestamp = Long.parseLong(message.getSafeTimestamp());
                                    latch.countDown();
                                }
                                break;
                            case INSERT:
                            case UPDATE:
                            case DELETE:
                                if (!started) {
                                    break;
                                }
                                OceanBaseRecord record = getChangeRecord(message);
                                if (record != null) {
                                    changeRecordBuffer.add(record);
                                }
                                break;
                            case COMMIT:
                                // flush buffer after snapshot completed
                                if (!shouldReadSnapshot() || snapshotCompleted.get()) {
                                    changeRecordBuffer.forEach(
                                            r -> {
                                                try {
                                                    if (deserializer != null) {
                                                        deserializer.deserialize(
                                                                r, outputCollector);
                                                    } else {
                                                        assert debeziumDeserializer != null;
                                                        debeziumDeserializer.deserialize(
                                                                toSourceRecord(r), outputCollector);
                                                    }

                                                } catch (Exception e) {
                                                    throw new FlinkRuntimeException(e);
                                                }
                                            });
                                    changeRecordBuffer.clear();
                                    long timestamp = Long.parseLong(message.getSafeTimestamp());
                                    if (timestamp > resolvedTimestamp) {
                                        resolvedTimestamp = timestamp;
                                    }
                                }
                                break;
                            case DDL:
                                // TODO record ddl and remove expired table schema
                                LOG.trace(
                                        "Ddl: {}",
                                        message.getFieldList().get(0).getValue().toString());
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported type: " + message.getOpt());
                        }
                    }

                    @Override
                    public void onException(LogProxyClientException e) {
                        LOG.error("LogProxyClient exception", e);
                        logProxyClient.stop();
                    }
                });

        logProxyClient.start();
        LOG.info("Wait for LogProxyClient to start");
        if (!latch.await(connectTimeout.getSeconds(), TimeUnit.SECONDS)) {
            throw new TimeoutException("Timeout to receive messages from LogProxy");
        }
        LOG.info("LogProxyClient started from timestamp: {}", startTimestamp);
    }

    private OceanBaseRecord getChangeRecord(LogMessage message) {
        String databaseName = message.getDbName().replace(tenantName + ".", "");
        if (!tableSet.contains(String.format("%s.%s", databaseName, message.getTableName()))) {
            return null;
        }
        OceanBaseRecord.SourceInfo sourceInfo =
                new OceanBaseRecord.SourceInfo(
                        tenantName,
                        databaseName,
                        message.getTableName(),
                        Long.parseLong(message.getSafeTimestamp()));
        return new OceanBaseRecord(sourceInfo, message.getOpt(), message.getFieldList());
    }

    private SourceRecord toSourceRecord(OceanBaseRecord record) {
        OceanBaseRecord.SourceInfo sourceInfo = record.getSourceInfo();
        String topicName =
                String.format(
                        "%s.%s.%s",
                        sourceInfo.getTenant(), sourceInfo.getDatabase(), sourceInfo.getTable());
        String db, schema;
        if ("mysql".equalsIgnoreCase(compatibleMode)) {
            db = sourceInfo.getDatabase();
            schema = null;
        } else {
            db = null;
            schema = sourceInfo.getDatabase();
        }
        TableSchema tableSchema =
                OceanBaseTableSchema.getTableSchema(
                        getDataSource(),
                        compatibleMode,
                        sourceInfo.getTenant(),
                        db,
                        schema,
                        sourceInfo.getTable(),
                        decimalMode,
                        temporalPrecisionMode,
                        bigintUnsignedHandlingMode,
                        binaryHandlingMode);
        Struct source =
                OceanBaseTableSchema.sourceStruct(
                        sourceInfo.getTenant(),
                        db,
                        schema,
                        sourceInfo.getTable(),
                        String.valueOf(sourceInfo.getTimestampS()));
        Map<String, String> partition =
                getSourcePartition(
                        sourceInfo.getTenant(), sourceInfo.getDatabase(), sourceInfo.getTable());
        // the offset here is useless
        Map<String, Object> offset = getSourceOffset(sourceInfo.getTimestampS());
        Struct struct;

        Schema valueSchema = tableSchema.valueSchema();
        List<Field> fields = valueSchema.fields();
        Struct before, after;
        Object[] beforeFieldValues, afterFieldValues;
        switch (record.getOpt()) {
            case INSERT:
                afterFieldValues = new Object[fields.size()];
                for (Field field : fields) {
                    afterFieldValues[field.index()] =
                            record.getLogMessageFieldsAfter().get(field.name());
                }
                after = tableSchema.valueFromColumnData(afterFieldValues);
                struct = tableSchema.getEnvelopeSchema().create(after, source, Instant.now());
                break;
            case DELETE:
                beforeFieldValues = new Object[fields.size()];
                for (Field field : fields) {
                    beforeFieldValues[field.index()] =
                            record.getLogMessageFieldsBefore().get(field.name());
                }
                before = tableSchema.valueFromColumnData(beforeFieldValues);
                struct = tableSchema.getEnvelopeSchema().delete(before, source, Instant.now());
                break;
            case UPDATE:
                beforeFieldValues = new Object[fields.size()];
                afterFieldValues = new Object[fields.size()];
                for (Field field : fields) {
                    beforeFieldValues[field.index()] =
                            record.getLogMessageFieldsBefore().get(field.name());
                    afterFieldValues[field.index()] =
                            record.getLogMessageFieldsAfter().get(field.name());
                }
                before = tableSchema.valueFromColumnData(beforeFieldValues);
                after = tableSchema.valueFromColumnData(afterFieldValues);
                struct =
                        tableSchema
                                .getEnvelopeSchema()
                                .update(before, after, source, Instant.now());
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return new SourceRecord(
                partition, offset, topicName, null, null, null, struct.schema(), struct);
    }

    @Override
    public void notifyCheckpointComplete(long l) {
        // do nothing
    }

    @Override
    public TypeInformation<T> getProducedType() {
        if (deserializer != null) {
            return deserializer.getProducedType();
        }
        return debeziumDeserializer.getProducedType();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (shouldReadSnapshot() && !snapshotCompleted.get()) {
            Map<String, String> map = new HashMap<>();
            for (Map.Entry<String, OceanBaseSnapshotChunkBound> entry :
                    resolvedChunkBound.entrySet()) {
                map.put(entry.getKey(), objectMapper.writeValueAsString(entry.getValue()));
            }
            LOG.info(
                    "snapshotState checkpoint: {} at resolvedChunkBound: {}",
                    context.getCheckpointId(),
                    map);
            chunkReaderState.clear();
            chunkReaderState.add(map);
        } else {
            LOG.info(
                    "snapshotState checkpoint: {} at resolvedTimestamp: {}",
                    context.getCheckpointId(),
                    resolvedTimestamp);
            changeStreamState.clear();
            changeStreamState.add(resolvedTimestamp);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.info("initialize checkpoint");
        chunkReaderState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "resolvedChunkBoundState",
                                        new MapSerializer<>(
                                                StringSerializer.INSTANCE,
                                                StringSerializer.INSTANCE)));
        changeStreamState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "resolvedTimestampState", LongSerializer.INSTANCE));
        if (context.isRestored()) {
            for (final Long offset : changeStreamState.get()) {
                if (offset > 0) {
                    resolvedTimestamp = offset;
                    LOG.info("Restore State from resolvedTimestamp: {}", resolvedTimestamp);
                    return;
                } else {
                    break;
                }
            }
            for (final Map<String, String> map : chunkReaderState.get()) {
                objectMapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
                for (Map.Entry<String, String> entry : map.entrySet()) {
                    resolvedChunkBound.put(
                            entry.getKey(),
                            objectMapper.readValue(
                                    entry.getValue(), OceanBaseSnapshotChunkBound.class));
                }
                LOG.info("Restore State from resolvedChunkBound: {}", map);
                return;
            }
        }
    }

    @Override
    public void cancel() {
        if (chunkSplitter != null) {
            chunkSplitter.close();
        }
        if (dataSource != null) {
            dataSource.close();
        }
        if (logProxyClient != null) {
            logProxyClient.stop();
        }
    }

    private static class OutputCollector<T> implements Collector<T> {

        private SourceContext<T> context;

        @Override
        public void collect(T record) {
            context.collect(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
