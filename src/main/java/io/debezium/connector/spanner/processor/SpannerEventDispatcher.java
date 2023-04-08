/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.processor;

import static org.slf4j.LoggerFactory.getLogger;

import java.sql.Connection;
import java.sql.DriverManager;
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

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDriver;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
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
import io.debezium.connector.spanner.kafka.internal.model.InterimRecord;
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
        private boolean isFinishedPartition;

        public Records(
                       String k,
                       String v,
                       long ct,
                       boolean isData,
                       String part,
                       long rec,
                       String trx,
                       boolean isFinishedPartition) {
            this.key = k;
            this.value = v;
            this.commitTs = ct;
            this.isData = isData;
            this.partition = part;
            this.recSeq = rec;
            this.trx = trx;
            this.isFinishedPartition = isFinishedPartition;
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

        public boolean isFinishedPartition() {
            return isFinishedPartition;
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
    private PoolingDriver driver = null;
    private int window = 10;

    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    // private BasicDataSource bds = new BasicDataSource();

    private boolean canProcess = false;
    private boolean firstTimeCheck = true;

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
        this.window = connectorConfig.getPocWindowSec();
        LOGGER.error("The window is : " + window);

        minHeap = new PriorityQueue<Records>(new RecordComparator());
        try {
            int singlePart = kafkaPartitionInfoProvider.getPartitions("ucs", Optional.of(1)).iterator().next();
            singlePart = kafkaPartitionInfoProvider.getPartitions("ucsb", Optional.of(1)).iterator().next();
            singlePart = kafkaPartitionInfoProvider.getPartitions("ordout", Optional.of(1)).iterator().next();
            singlePart = kafkaPartitionInfoProvider.getPartitions("wm", Optional.of(1)).iterator().next();
        }
        catch (Exception ex) {
            LOGGER.warn("Error while creating topics");
        }

        try {
            Class dirverClass = Class.forName(JDBC_DRIVER);
        }
        catch (ClassNotFoundException e) {
            System.err.println("There was not able to find the driver class");
        }

        ConnectionFactory driverManagerConnectionFactory = new DriverManagerConnectionFactory(
                "jdbc:mysql://104.197.203.198:3306/test", "test", "test");

        PoolableConnectionFactory poolfactory = new PoolableConnectionFactory(driverManagerConnectionFactory, null);
        ObjectPool connectionPool = new GenericObjectPool(poolfactory);

        poolfactory.setPool(connectionPool);
        try {
            Class.forName("org.apache.commons.dbcp2.PoolingDriver");
        }
        catch (ClassNotFoundException e) {
            System.err.println("There was not able to find the driver class");
        }
        try {
            driver = (PoolingDriver) DriverManager.getDriver("jdbc:apache:commons:dbcp:");
        }
        catch (SQLException e) {
            System.err.println("There was an error: " + e.getMessage());
        }
        driver.registerPool("test-poc", connectionPool);
        // Set database driver name
        /*
         * bds.setDriverClassName(JDBC_DRIVER);
         * // Set database url
         * bds.setUrl("jdbc:mysql://104.197.203.198:3306/test");
         * // bds.setUrl("jdbc:mysql://localhost:3306/test");
         * // Set database user
         * bds.setUsername("test");
         * // Set database password
         * bds.setPassword("test");
         * // Set the connection pool size
         * bds.setInitialSize(5);
         */
    }

    public boolean publishLowWatermarkStampEvent(String taskId, TaskSyncContext taskSyncContext) {

        Instant start = Instant.now();
        try {
            if (!canProcess && firstTimeCheck) {
                firstTimeCheck = false;
                if (!this.daoFactory.getPartitionMetadataDao().canWork(taskId)) {
                    // tough luck, someone else locked it
                    return true;
                }

                canProcess = true;
            }
            else {
                if (!canProcess) {
                    return true;
                }
            }

            // TODO: how to clear historically old FINISHED partitions
            // Give some initial catchup window - current minus 5 mins or so - to catchup - take it as
            // configuration

            // Instant thresholdTsInstant = Timestamp.now().toSqlTimestamp().toInstant().minus(1,
            // ChronoUnit.SECONDS);
            // long thresholdTs = ChronoUnit.MICROS.between(Instant.EPOCH, thresholdTsInstant);
            Instant thresholdTsInstant;
            long thresholdTs = 0l;
            // long savedWm = this.daoFactory.getPartitionMetadataDao().getLastWaterMark(taskId);
            long savedWm = kafkaConsumer.getWatermark();
            Instant savedThresholdInst;
            if (savedWm == 0l) // first run would need to start somewhere
            {
                thresholdTsInstant = Timestamp.now().toSqlTimestamp().toInstant().minus(1, ChronoUnit.SECONDS);
                thresholdTs = ChronoUnit.MICROS.between(Instant.EPOCH, thresholdTsInstant);
                savedThresholdInst = thresholdTsInstant;
                /*
                 * savedThresholdInst =
                 * Timestamp.now()
                 * .toSqlTimestamp()
                 * .toInstant()
                 * .minus(2, ChronoUnit.SECONDS); // lets say this is the lag
                 */

            }
            else {
                savedThresholdInst = Instant.ofEpochMilli(savedWm / 1000);

                thresholdTs = savedWm + (1000000 * window); // 10 sec window default
                thresholdTsInstant = Instant.ofEpochMilli(thresholdTs / 1000);
            }

            LOGGER.info(" ### Threshold timestamp: " + thresholdTs);
            LOGGER.info(" ### The savedTs is : " + savedThresholdInst.toString());

            /*
             * List<PartitionState> toPrintPart = taskSyncContext.getAllTaskStates().values().stream()
             * .flatMap(taskState -> taskState.getPartitions().stream())
             * .filter(
             * partitionState -> ((partitionState.getState().equals(PartitionStateEnum.FINISHED)
             * && partitionState.getToken() != "Parent0"
             * /*
             * && partitionState
             * .getFinishedTimestamp()
             * .toSqlTimestamp()
             * .toInstant()
             * .compareTo(savedThresholdInst) // finished after prev window
             * >= 0
             * )
             * || (partitionState.getState().equals(PartitionStateEnum.RUNNING)
             * && partitionState.getToken() != "Parent0"
             * && partitionState
             * .getStartTimestamp()
             * .toSqlTimestamp()
             * .toInstant()
             * .compareTo(thresholdTsInstant) < 0)))
             * .collect(Collectors.toList());
             *
             * for (PartitionState p : toPrintPart) {
             * LOGGER.info(" ### Partition state to process : " + p.toString());
             * }
             */

            Set<String> partitionsToProcess = taskSyncContext.getAllTaskStates().values().stream()
                    .flatMap(taskState -> taskState.getPartitions().stream())
                    .filter(
                            partitionState -> ((partitionState.getState().equals(PartitionStateEnum.FINISHED)
                                    && partitionState.getToken() != "Parent0"
                                    && partitionState
                                            .getFinishedTimestamp()
                                            .toSqlTimestamp()
                                            .toInstant()
                                            .compareTo(savedThresholdInst) // finished after prev window
                                            > 0
                            /*
                             * && partitionState
                             * .getFinishedTimestamp()
                             * .toSqlTimestamp()
                             * .toInstant()
                             * .compareTo(
                             * thresholdTsInstant) // finished before or at current
                             * // window
                             * < 0 commented since records saved in prev window were not
                             * getting picked up if the partition finish is after the current threshold
                             */
                            )
                                    /*
                                     * || (partitionState
                                     * .getState()
                                     * .equals(
                                     * PartitionStateEnum
                                     * .FINISHED) // gather the partitions in this window
                                     * // runs
                                     * && partitionState
                                     * .getFinishedTimestamp()
                                     * .toSqlTimestamp()
                                     * .toInstant()
                                     * .compareTo(thresholdTsInstant)
                                     * >= 0)
                                     */
                                    || (partitionState.getState().equals(PartitionStateEnum.RUNNING)
                                            && partitionState.getToken() != "Parent0"
                                            && partitionState
                                                    .getStartTimestamp()
                                                    .toSqlTimestamp()
                                                    .toInstant()
                                                    .compareTo(thresholdTsInstant) < 0)))
                    .map(partitionState -> partitionState.getToken())
                    .collect(Collectors.toSet());

            if (partitionsToProcess.size() == 0) {
                // this.daoFactory.getPartitionMetadataDao().updateLastWatermark(taskId, thresholdTs);
                kafkaProducer.putWatermark(Long.toString(thresholdTs));

                return true; // when you ask? During startup
            }

            Set<String> finishedPartitions = new HashSet<>();
            LOGGER.info(" ### The number of partitions to query: " + partitionsToProcess.size());
            long recProcessed = 0l;
            long recToWriteToKafka = 0l;
            boolean isRebufferred = false;
            Set<String> rebufferredTrx = new HashSet<>();
            while (true) {
                InterimRecord intRec = kafkaConsumer.getRecord();
                String rec = intRec.getRecord();
                LOGGER.info(" ### The record from Kafka: " + rec);

                if (rec.equals("")) {
                    // we have seen all records from Kafka, but save only when we have all the records
                    // let us re-read from same offset next time , do not extend the last saved window

                    if (finishedPartitions.size() != partitionsToProcess.size()) {
                        // sleep 1 seconds
                        LOGGER.info(" ### Waiting for records for the window");
                        Thread.sleep(1000);
                        continue;
                    }
                    else {
                        writeToKafka();
                        // this.daoFactory.getPartitionMetadataDao().updateLastWatermark(taskId, thresholdTs);
                        kafkaProducer.putWatermark(Long.toString(thresholdTs));
                        kafkaConsumer.commitOffsets();
                        break;
                    }

                }
                else {
                    recProcessed++;
                    Records r = parseRecord(rec);

                    if (!partitionsToProcess.contains(r.getPartition())) {
                        LOGGER.info(" ### Saving record for later unmatched partition ");
                        long savedOffset = kafkaProducer.saveForLater(rec);
                        if (intRec.isOriginUcsb()) {
                            if (!isRebufferred) {
                                isRebufferred = true;
                                LOGGER.info(" ### Not gonna enter loop again, done at: " + savedOffset);
                                kafkaConsumer.pauseAt(
                                        savedOffset); // we want to stop reading from ucsb in same loop
                            }
                        }
                    }
                    else {

                        if (r.getCommitTs() <= thresholdTs) {
                            if (r.isData()) {
                                LOGGER.info(
                                        " ### Saving record for processing  ct: "
                                                + r.getCommitTs()
                                                + " threshold: "
                                                + thresholdTs);
                                minHeap.add(r);
                                recToWriteToKafka++;
                                // Sadly this is not true, the isLast just says for this trx if this
                                // is the last record
                                /*
                                 * if (r.isLastInPartition()) {
                                 * finishedPartitions.add(
                                 * r.getPartition()); // we will not get more records for this partition
                                 */
                            }
                            else {
                                if (r.isFinishedPartition()) {
                                    finishedPartitions.add(r.getPartition());
                                    LOGGER.info(" ### finished partition size : " + finishedPartitions.size());
                                }
                            } // heartbeat we can skip
                        }
                        else {
                            LOGGER.info(
                                    " ### Saving record for later greater CT "
                                            + r.getCommitTs()
                                            + " threshold: "
                                            + thresholdTs);
                            long savedOffset = kafkaProducer.saveForLater(rec);
                            if (intRec.isOriginUcsb()) {
                                if (!isRebufferred) {
                                    isRebufferred = true;
                                    LOGGER.info(" ### Not gonna enter loop again, done at: " + savedOffset);

                                    kafkaConsumer.pauseAt(
                                            savedOffset); // we want to stop reading again from ucsb in same loop
                                }
                            }
                            finishedPartitions.add(r.getPartition());
                            LOGGER.info(" ### finished partition size : " + finishedPartitions.size());
                        }
                        if (finishedPartitions.size() == partitionsToProcess.size()) {
                            // writeToMysql();
                            writeToKafka();
                            kafkaProducer.putWatermark(Long.toString(thresholdTs));
                            // this.daoFactory.getPartitionMetadataDao().updateLastWatermark(taskId, thresholdTs);
                            kafkaConsumer.commitOffsets();
                            break;
                        }
                    }
                }
            }

            Instant end = Instant.now();
            long duration = ChronoUnit.SECONDS.between(start, end);
            if (duration > 0) {
                LOGGER.error(
                        " Records processed : "
                                + recProcessed
                                + " with TPS : "
                                + recProcessed / duration
                                + "  and WPS : "
                                + recToWriteToKafka / duration);
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

            /*
             * String isLastRecSub = trxSub.substring(trxSub.indexOf("=") + 2);
             * isLastRecSub = isLastRecSub.substring(isLastRecSub.indexOf("=") + 1);
             *
             * String isLastRec = isLastRecSub.substring(0, 4);
             *
             * // not used
             * boolean isLastRecForPartition = false;
             * if (isLastRec.charAt(0) == 't') {
             * isLastRecForPartition = true;
             * }
             */

            String recSub = trxSub.substring(trxSub.indexOf("recordSequence") + 16);

            String recCnt = recSub.substring(0, recSub.indexOf(",") - 1);
            long recSeq = Long.parseLong(recCnt.trim());
            String key = Long.toString(ct) + "_" + trx + "_" + recCnt;

            Records r = new Records(key, rec, ct, true, part, recSeq, trx, false);
            return r;

        }
        else {
            if (rec.charAt(0) == 'H') {
                String commitsub = rec.substring(rec.indexOf("timestamp") + 10);

                String commit = commitsub.substring(0, commitsub.indexOf("Z") + 1);
                Instant commitTsInst = Timestamp.parseTimestamp(commit).toSqlTimestamp().toInstant();
                long ct = ChronoUnit.MICROS.between(Instant.EPOCH, commitTsInst);

                String partSub = commitsub.substring(commitsub.indexOf("partitionToke") + 16);

                String part = partSub.substring(0, partSub.indexOf("recordTimestamp") - 3);
                Records r = new Records("key", rec, ct, false, part, 0l, "", false);
                return r;
            }
            else {

                String partsub = rec.substring(rec.indexOf("=") + 1);

                String part = partsub.substring(1, partsub.indexOf("timestamp") - 2);

                String commit = partsub.substring(partsub.indexOf("timestamp") + 10);
                Instant commitTsInst = Timestamp.parseTimestamp(commit).toSqlTimestamp().toInstant();
                long ct = ChronoUnit.MICROS.between(Instant.EPOCH, commitTsInst);

                Records r = new Records("key", rec, ct, false, part, 0l, "", true);
                return r;
            }
        }
    }

    private void writeToMysql() {
        LOGGER.error("The size of minHeap is: " + minHeap.size());
        List<Records> list = new ArrayList<>();
        while (!minHeap.isEmpty()) {
            Records r = minHeap.poll();
            list.add(r);
            // insertToDB(r);
        }
        LOGGER.error("Insert to DB started : " + Timestamp.now().toString());
        insertToDB(list);
        LOGGER.error("Insert to DB ended : " + Timestamp.now().toString());
        return;
    }

    private void writeToKafka() {
        LOGGER.info("##The size of minHeap is: " + minHeap.size());
        boolean shouldFlush = false;
        while (!minHeap.isEmpty()) {
            shouldFlush = true;
            Records r = minHeap.poll();
            Instant emitInst = Timestamp.now().toSqlTimestamp().toInstant();
            long emitTs = ChronoUnit.MICROS.between(Instant.EPOCH, emitInst);
            String rec = "Emit_timestamp:" + emitTs + "," + r.getValue();
            kafkaProducer.putOrderedData(rec);
        }
        if (shouldFlush)
            kafkaProducer.flushOrderedData();
    }

    private void insertToDB(List<Records> list) {
        Connection connObj = null;
        PreparedStatement pstmtObj = null;

        try {
            // connObj = bds.getConnection();
            connObj = DriverManager.getConnection("jdbc:apache:commons:dbcp:test-poc");
            pstmtObj = connObj.prepareStatement(
                    "INSERT INTO OutTable(INSERT_TS,CT,TRX_ID,REC_SEQ,NUM_REC,PAYLOAD)"
                            + " VALUES(?,?,?,?,?,?)");

            for (Records r : list) {
                Instant instTs = Timestamp.now().toSqlTimestamp().toInstant();
                long insertTs = ChronoUnit.MICROS.between(Instant.EPOCH, instTs);

                pstmtObj.setLong(1, insertTs);

                long commitTs = r.getCommitTs();

                pstmtObj.setLong(2, commitTs);
                pstmtObj.setString(3, r.getTrx());

                pstmtObj.setLong(4, r.getRecSeq());

                pstmtObj.setLong(5, 42l);
                pstmtObj.setString(6, r.getValue());
                pstmtObj.addBatch();
            }

            pstmtObj.executeBatch();
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
