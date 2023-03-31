/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor;

import static org.slf4j.LoggerFactory.getLogger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;

import com.google.cloud.Timestamp;
import com.google.common.annotations.VisibleForTesting;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.SpannerPartition;
import io.debezium.connector.spanner.context.source.SourceInfoFactory;
import io.debezium.connector.spanner.db.DaoFactory;
import io.debezium.connector.spanner.db.metadata.SchemaRegistry;
import io.debezium.connector.spanner.db.metadata.TableId;
import io.debezium.connector.spanner.exception.SpannerConnectorException;
import io.debezium.connector.spanner.kafka.KafkaPartitionInfoProvider;
import io.debezium.connector.spanner.kafka.internal.UnifiedConsumer;
import io.debezium.connector.spanner.kafka.internal.UnifiedPublisher;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.task.TaskSyncContext;
import io.debezium.data.Envelope;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/** Spanner dispatcher for data change and schema change events. */
public class SpannerEventDispatcher extends EventDispatcher<SpannerPartition, TableId> {

    public class Records {
        private String key;
        private String value;
        private long commitTs;
        private boolean isData;
        private String partition;
        private long recSeq;
        private String trx;

        public Records(String k, String v, long ct, boolean isData, String part, long rec, String trx) {
            this.key = k;
            this.value = v;
            this.commitTs = ct;
            this.isData = isData;
            this.partition = part;
            this.recSeq = rec;
            this.trx = trx;
        }

        public String getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }

        public long getCommitTs() {
            return commitTs;
        }

        public boolean isData() {
            return isData;
        }

        public String getPartition() {
            return partition;
        }

        public long getRecSeq() {
            return recSeq;
        }

        public String getTrx() {
            return trx;
        }
    }

    public class RecordComparator implements Comparator<Records> {

        public int compare(Records r1, Records r2) {
            return r1.getKey().compareTo(r2.getKey());
        }
    }

    private static final Logger LOGGER = getLogger(SpannerEventDispatcher.class);

    private final SpannerConnectorConfig connectorConfig;
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final TopicNamingStrategy<TableId> topicNamingStrategy;
    private final SchemaRegistry schemaRegistry;

    private final DatabaseSchema<TableId> schema;

    private final SourceInfoFactory sourceInfoFactory;

    private final KafkaPartitionInfoProvider kafkaPartitionInfoProvider;

    private final DaoFactory daoFactory;

    private final UnifiedConsumer kafkaConsumer;

    private final UnifiedPublisher kafkaProducer;

    private final PriorityQueue<Records> minHeap;

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private BasicDataSource bds = new BasicDataSource();

    public SpannerEventDispatcher(
                                  SpannerConnectorConfig connectorConfig,
                                  TopicNamingStrategy<TableId> topicNamingStrategy,
                                  DatabaseSchema<TableId> schema,
                                  ChangeEventQueue<DataChangeEvent> queue,
                                  DataCollectionFilters.DataCollectionFilter<TableId> filter,
                                  ChangeEventCreator changeEventCreator,
                                  EventMetadataProvider metadataProvider,
                                  HeartbeatFactory<TableId> heartbeatFactory,
                                  SchemaNameAdjuster schemaNameAdjuster,
                                  SchemaRegistry schemaRegistry,
                                  SourceInfoFactory sourceInfoFactory,
                                  KafkaPartitionInfoProvider kafkaPartitionInfoProvider,
                                  DaoFactory daoFactory) {
        super(
                connectorConfig,
                topicNamingStrategy,
                schema,
                queue,
                filter,
                changeEventCreator,
                metadataProvider,
                heartbeatFactory.createHeartbeat(),
                schemaNameAdjuster);
        this.connectorConfig = connectorConfig;
        this.queue = queue;
        this.topicNamingStrategy = topicNamingStrategy;
        this.schemaRegistry = schemaRegistry;
        this.schema = schema;
        this.sourceInfoFactory = sourceInfoFactory;
        this.kafkaPartitionInfoProvider = kafkaPartitionInfoProvider;
        this.daoFactory = daoFactory;
        this.kafkaConsumer = new UnifiedConsumer(connectorConfig);
        this.kafkaProducer = new UnifiedPublisher(connectorConfig);
        minHeap = new PriorityQueue<Records>(new RecordComparator());
        try {
            int singlePart = kafkaPartitionInfoProvider.getPartitions("ucs", Optional.of(1)).iterator().next();
            singlePart = kafkaPartitionInfoProvider.getPartitions("ucsb", Optional.of(1)).iterator().next();
        }
        catch (Exception ex) {
            LOGGER.warn("Error while creating topics");
        }

        // Set database driver name
        bds.setDriverClassName(JDBC_DRIVER);
        // Set database url
        bds.setUrl("jdbc:mysql://localhost:3306/test");
        // Set database user
        bds.setUsername("test");
        // Set database password
        bds.setPassword("test");
        // Set the connection pool size
        bds.setInitialSize(5);
    }

    public boolean publishLowWatermarkStampEvent(String taskId, TaskSyncContext taskSyncContext) {

        try {
            if (!this.daoFactory.getPartitionMetadataDao().canWork(taskId)) {
                // tough luck, someone else locked it
                return true;
            }

            Instant thresholdTsInstant = Timestamp.now().toSqlTimestamp().toInstant().minus(1, ChronoUnit.SECONDS);
            long thresholdTs = ChronoUnit.MICROS.between(Instant.EPOCH, thresholdTsInstant);

            // we can lose a partition if it starts at currentTime and finishes ealier than current time
            // +1 TODO: handle this
            Set<String> partitionsToProcess = taskSyncContext.getAllTaskStates().values().stream()
                    .flatMap(taskState -> taskState.getPartitions().stream())
                    .filter(
                            partitionState -> ((partitionState.getState().equals(PartitionStateEnum.FINISHED)
                                    && partitionState
                                            .getFinishedTimestamp()
                                            .toSqlTimestamp()
                                            .toInstant()
                                            .compareTo(thresholdTsInstant) > 0)
                                    || (partitionState.getState().equals(PartitionStateEnum.RUNNING)
                                            && partitionState
                                                    .getStartTimestamp()
                                                    .toSqlTimestamp()
                                                    .toInstant()
                                                    .compareTo(thresholdTsInstant) < 0)))
                    .map(partitionState -> partitionState.getToken())
                    .collect(Collectors.toSet());

            Set<String> finishedPartitions = new HashSet<>();

            while (true) {
                String rec = kafkaConsumer.getRecord();
                LOGGER.info(" ### The record from Kafka: " + rec);

                if (rec.equals("")) {
                    // we have seen all records from Kafka

                    kafkaConsumer.commitOffsets();
                    writeToMysql();
                    break;
                }
                else {

                    Records r = parseRecord(rec);

                    if (!partitionsToProcess.contains(r.getPartition())) {
                        kafkaProducer.saveForLater(rec);
                    }
                    else {

                        if (r.getCommitTs() < thresholdTs) {
                            if (r.isData()) {
                                minHeap.add(r);
                            } // heartbeat we can skip
                        }
                        else {
                            kafkaProducer.saveForLater(rec);
                            finishedPartitions.add(r.getPartition());
                        }
                        if (finishedPartitions.size() == partitionsToProcess.size()) {
                            kafkaConsumer.commitOffsets();
                            writeToMysql();
                            break;
                        }
                    }
                }
            }

            return true;
        }
        catch (Exception ex) {
            if (CommonConnectorConfig.EventProcessingFailureHandlingMode.FAIL.equals(
                    connectorConfig.getEventProcessingFailureHandlingMode())) {
                throw new SpannerConnectorException("Error while publishing watermark stamp", ex);
            }
            LOGGER.warn("Error while publishing watermark stamp");

            return false;
        }

        /*
         * try {
         *
         * // List<BufferedPayload> response = this.daoFactory.getUCSDao().getNext(taskId);
         * List<BufferedPayload> response = this.daoFactory.getPartitionMetadataDao().getNext(taskId);
         *
         * // publish to Kafka
         * if (response != null) {
         * int singlePart = kafkaPartitionInfoProvider.getPartitions("ucs", Optional.of(1)).iterator().next();
         *
         * for (BufferedPayload data : response) {
         *
         * Struct sourceStruct = null;
         * DataCollectionSchema dataCollectionSchema = null;
         * for (TableId tableId : schemaRegistry.getAllTables()) {
         *
         * dataCollectionSchema = schema.schemaFor(tableId);
         *
         * sourceStruct = sourceInfoFactory
         * .getSourceInfoForBufferredPayload(tableId, data.toString())
         * .struct();
         *
         * break;
         * }
         * SourceRecord sourceRecord = emitSourceRecord("ucs", dataCollectionSchema, singlePart, sourceStruct);
         *
         * queue.enqueue(new DataChangeEvent(sourceRecord));
         * }
         * }
         * // this.daoFactory.getUCSDao().purgeProcessedData(taskId);
         * this.daoFactory.getPartitionMetadataDao().purgeProcessedData(taskId);
         *
         * // regular processing
         * /*
         * for (TableId tableId : schemaRegistry.getAllTables()) {
         *
         * String topicName = topicNamingStrategy.dataChangeTopic(tableId);
         *
         * DataCollectionSchema dataCollectionSchema = schema.schemaFor(tableId);
         *
         * Struct sourceStruct = sourceInfoFactory.getSourceInfoForLowWatermarkStamp(tableId).struct();
         *
         * int numPartitions = connectorConfig.getTopicNumPartitions();
         * for (int partition : kafkaPartitionInfoProvider.getPartitions(topicName, Optional.of(numPartitions))) {
         * SourceRecord sourceRecord = emitSourceRecord(topicName, dataCollectionSchema, partition, sourceStruct);
         * LOGGER.debug("Build low watermark stamp record {} ", sourceRecord);
         *
         * queue.enqueue(new DataChangeEvent(sourceRecord));
         * }
         * }
         */

    }

    private Records parseRecord(String rec) {

        if (rec.charAt(0) == 'D') {
            String partsub = rec.substring(rec.indexOf("partitionToken=") + 16);

            String part = partsub.substring(0, partsub.indexOf("commitTimestam") - 3);

            String commitsub = partsub.substring(partsub.indexOf("commitTimestam") + 16);

            String commit = commitsub.substring(0, commitsub.indexOf(","));
            Instant commitTsInst = Timestamp.parseTimestamp(commit).toSqlTimestamp().toInstant();
            long ct = ChronoUnit.MICROS.between(Instant.EPOCH, commitTsInst);

            String trxSub = commitsub.substring(commitsub.indexOf("serverTransactionId") + 20);

            String trx = trxSub.substring(1, trxSub.indexOf("isLastRecordInTrans") - 3);

            String recSub = trxSub.substring(trxSub.indexOf("recordSequence") + 16);

            String recCnt = recSub.substring(0, recSub.indexOf(",") - 1);
            long recSeq = Long.parseLong(recCnt.trim());
            String key = Long.toString(ct) + "_" + trx + "_" + recCnt;

            Records r = new Records(key, rec, ct, true, part, recSeq, trx);
            return r;

        }
        else {
            String commitsub = rec.substring(rec.indexOf("timestamp") + 10);

            String commit = commitsub.substring(0, commitsub.indexOf("Z") + 1);
            Instant commitTsInst = Timestamp.parseTimestamp(commit).toSqlTimestamp().toInstant();
            long ct = ChronoUnit.MICROS.between(Instant.EPOCH, commitTsInst);

            String partSub = commitsub.substring(commitsub.indexOf("partitionToke") + 16);

            String part = partSub.substring(0, partSub.indexOf("recordTimestamp") - 3);
            Records r = new Records("key", rec, ct, false, part, 0l, "");
            return r;
        }
    }

    private void writeToMysql() {
        LOGGER.info("The size of minHeap is: " + minHeap.size());
        while (!minHeap.isEmpty()) {
            Records r = minHeap.poll();
            insertToDB(r);
        }

        return;
    }

    private void insertToDB(Records r) {
        Connection connObj = null;
        PreparedStatement pstmtObj = null;

        try {
            connObj = bds.getConnection();
            pstmtObj = connObj.prepareStatement(
                    "INSERT INTO OutTable(INSERT_TS,CT,TRX_ID,REC_SEQ,NUM_REC,PAYLOAD)"
                            + " VALUES(?,?,?,?,?,?)");

            Instant instTs = Timestamp.now().toSqlTimestamp().toInstant();
            long insertTs = ChronoUnit.MICROS.between(Instant.EPOCH, instTs);

            pstmtObj.setLong(1, insertTs);

            long commitTs = r.getCommitTs();

            pstmtObj.setLong(2, commitTs);
            pstmtObj.setString(3, r.getTrx());

            pstmtObj.setLong(4, r.getRecSeq());

            pstmtObj.setLong(5, 42l);
            pstmtObj.setString(6, r.getValue());

            int rowAffected = pstmtObj.executeUpdate();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {

                if (pstmtObj != null)
                    pstmtObj.close();
                if (connObj != null)
                    connObj.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @VisibleForTesting
    SourceRecord emitSourceRecord(
                                  String topicName,
                                  DataCollectionSchema dataCollectionSchema,
                                  int partition,
                                  Struct sourceStruct) {

        Struct envelope = buildMessage(dataCollectionSchema.getEnvelopeSchema(), sourceStruct);

        return new SourceRecord(
                null,
                null,
                topicName,
                partition,
                null,
                null,
                dataCollectionSchema.getEnvelopeSchema().schema(),
                envelope,
                null,
                SourceRecordUtils.from("watermark-" + UUID.randomUUID()));
    }

    @VisibleForTesting
    Struct buildMessage(Envelope envelope, Struct sourceStruct) {
        Struct struct = new Struct(envelope.schema());
        struct.put(Envelope.FieldName.OPERATION, Envelope.Operation.MESSAGE.code());
        struct.put(Envelope.FieldName.SOURCE, sourceStruct);
        struct.put(Envelope.FieldName.TIMESTAMP, Instant.now().toEpochMilli());
        return struct;
    }

    public void destroy() {
        super.close();
    }

    @Override
    public void close() {
    }
}
