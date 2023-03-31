/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.dao;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;

import io.debezium.connector.spanner.db.model.InitialPartition;
import io.debezium.connector.spanner.db.model.event.BufferedPayload;
import io.debezium.connector.spanner.db.model.event.DataChangeEvent;
import io.debezium.connector.spanner.db.model.event.HeartbeatEvent;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;

/** Provides functionality to read Spanner DB buffered change stream across all partitions */
public class PartitionMetadataDao {

    private final DatabaseClient databaseClient;

    public PartitionMetadataDao(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    /*
     * When writing a partition for the first time: there will be no records
     * and the write will succeed
     * However, during task failures, or rebalance, the partition can get assigned
     * to a different task.
     * In this case,we must update the starttime since the
     * query will start with the new start time onwards
     */
    public void writeRunningPartition(PartitionState partition) {

        String token = partition.getToken();
        if (InitialPartition.PARTITION_TOKEN.equals(token)) {
            return; // we will never get data from initial partitions
        }

        Instant startTimestampInstant = partition.getStartTimestamp().toSqlTimestamp().toInstant();
        long startTimestamp = ChronoUnit.MICROS.between(Instant.EPOCH, startTimestampInstant);

        // Does partition already exist

        Statement selectStatement = Statement.newBuilder(" SELECT Token " + " FROM MetaPartitions " + "WHERE Token = @token ")
                .bind("token")
                .to(token)
                .build();

        final ResultSet resultSet = this.databaseClient
                .singleUse() // Execute a single read or query against Cloud Spanner.
                .executeQuery(selectStatement);

        if (resultSet.next()) {
            // then we need to update the Record
            Statement updateStatement = Statement.newBuilder(
                    "UPDATE MetaPartitions  SET Watermark = NULL , FinishedAt = NULL , State ="
                            + " 'RUNNING' , StartTimestamp = @start WHERE Token = @part")
                    .bind("start")
                    .to(startTimestamp)
                    .bind("part")
                    .to(token)
                    .build();

            this.databaseClient
                    .readWriteTransaction()
                    .run(
                            transaction -> {
                                transaction.executeUpdate(updateStatement);
                                return null;
                            });

            return;
        }

        // first write
        List<Mutation> mutations = new ArrayList<>();
        mutations.add(
                Mutation.newInsertBuilder("MetaPartitions")
                        .set("Token")
                        .to(token)
                        .set("StartTimestamp")
                        .to(startTimestamp)
                        .set("State")
                        .to("RUNNING")
                        .build());

        this.databaseClient.write(mutations);
    }

    public void updateWaterMark(HeartbeatEvent hearbeat) {

        Instant commitInstant = hearbeat.getTimestamp().toSqlTimestamp().toInstant();
        long commitTs = ChronoUnit.MICROS.between(Instant.EPOCH, commitInstant);

        String part = hearbeat.getMetadata().getPartitionToken();
        Statement updateStatement = Statement.newBuilder(
                "UPDATE MetaPartitions " + "SET Watermark = @waterM " + "WHERE Token = @part")
                .bind("waterM")
                .to(commitTs)
                .bind("part")
                .to(part)
                .build();

        this.databaseClient
                .readWriteTransaction()
                .run(
                        transaction -> {
                            transaction.executeUpdate(updateStatement);
                            return null;
                        });
    }

    public void updateWaterMark(DataChangeEvent dataEvent) {

        Instant commitInstant = dataEvent.getCommitTimestamp().toSqlTimestamp().toInstant();
        long commitTs = ChronoUnit.MICROS.between(Instant.EPOCH, commitInstant);

        String part = dataEvent.getPartitionToken();
        Statement updateStatement = Statement.newBuilder(
                "UPDATE MetaPartitions " + "SET Watermark = @waterM " + "WHERE Token = @part")
                .bind("waterM")
                .to(commitTs)
                .bind("part")
                .to(part)
                .build();

        this.databaseClient
                .readWriteTransaction()
                .run(
                        transaction -> {
                            transaction.executeUpdate(updateStatement);
                            return null;
                        });
    }

    public void updateStateToFinshed(PartitionState partition) {

        String token = partition.getToken();
        Statement updateStatement = Statement.newBuilder(
                "UPDATE MetaPartitions "
                        + "SET State = @state , FinishedAt = @timestamp "
                        + "WHERE Token = @part")
                .bind("state")
                .to("FINISHED")
                .bind("timestamp")
                // .to(Value.COMMIT_TIMESTAMP) //this does not work
                .to(Timestamp.now())
                .bind("part")
                .to(token)
                .build();

        this.databaseClient
                .readWriteTransaction()
                .run(
                        transaction -> {
                            transaction.executeUpdate(updateStatement);
                            return null;
                        });
    }

    public boolean canWork(String taskId) {

        if (isLockedByWorker(taskId))
            return true;

        // We are here since the table is empty
        Statement insertStatement = Statement.newBuilder("INSERT INTO PARTITIONLOCK  (WORKER) VALUES ( '" + taskId + "')")
                .bind("worker")
                .to(taskId)
                .build();

        try {
            return this.databaseClient
                    .readWriteTransaction()
                    .run(
                            transaction -> {
                                long rowCount = transaction.executeUpdate(insertStatement);
                                if (rowCount == 1l)
                                    return true;
                                else
                                    return false;
                            });
        }
        catch (Exception e) {
            return false; // row could not be inserted as some thread succeeded to insert
        }
    }

    private boolean isLockedByWorker(String taskId) {

        Statement selectExistStatement = Statement.newBuilder(" SELECT WORKER " + " FROM PARTITIONLOCK  ").build();

        final ResultSet resultSetExist = this.databaseClient
                .singleUse() // Execute a single read or query against Cloud Spanner.
                .executeQuery(selectExistStatement);

        if (resultSetExist.next()) {
            return resultSetExist.getString(0).equals(taskId);
        }
        else {
            return false; // table is empty
        }
    }

    // checks if there are partitons that have not yet seen data
    private boolean shouldWaitForDelayedParitions() {
        Statement selectStatement = Statement.newBuilder(
                "Select count(*) from MetaPartitions where StartTimestamp < ( select min(Watermark)"
                        + " from MetaPartitions where Watermark is not null and state='RUNNING') and"
                        + " State='RUNNING'and Watermark is null")
                .build();

        final ResultSet resultSet = this.databaseClient
                .singleUse() // Execute a single read or query against Cloud Spanner.
                .executeQuery(selectStatement);
        if (resultSet.next()) {
            long count = resultSet.getLong(0);
            if (count > 0)
                return true;
            else
                return false;
        }
        else {
            return false;
        }
    }

    private void lockRecordsToExtract(String taskId) {
        Statement updateStatement = Statement.newBuilder(
                "Update UCS set WORKER = @worker  where PART in (  select Token from MetaPartitions"
                        + "   where StartTimestamp < ( select min(Watermark) from MetaPartitions where "
                        + " Watermark is not null and State='RUNNING')  and State='RUNNING'  and"
                        + " Watermark is not null )  and CT <  ( select min(Watermark) from "
                        + " MetaPartitions where Watermark is not null and State='RUNNING') OR  PART in"
                        + " (  select Token from MetaPartitions  where StartTimestamp < ( select"
                        + " MAX(Watermark) from MetaPartitions where  State='FINISHED')  and"
                        + " State='FINISHED'  and  Watermark is not null )  and CT <  ( select"
                        + " MAX(Watermark) from MetaPartitions where Watermark is not null and"
                        + " State='FINISHED') ")
                .bind("worker")
                .to(taskId)
                .build();

        this.databaseClient
                .readWriteTransaction()
                .run(
                        transaction -> {
                            transaction.executeUpdate(updateStatement);
                            return null;
                        });
    }

    public List<BufferedPayload> getNext(String taskId) {

        if (!canWork(taskId))
            return null;

        if (shouldWaitForDelayedParitions())
            return null;

        lockRecordsToExtract(taskId);

        Statement selectStatement = Statement.newBuilder(
                " SELECT CT, TRX_ID, REC_SEQ,NUM_REC,PAYLOAD "
                        + " FROM UCS "
                        + "WHERE WORKER = @worker "
                        + " ORDER BY CT,TRX_ID,REC_SEQ ")
                .bind("worker")
                .to(taskId)
                .build();

        final ResultSet resultSet = this.databaseClient
                .singleUse() // Execute a single read or query against Cloud Spanner.
                .executeQuery(selectStatement);
        List<BufferedPayload> response = new ArrayList<>();

        while (resultSet.next()) {
            BufferedPayload rec = new BufferedPayload(
                    resultSet.getLong(0),
                    resultSet.getString(1),
                    resultSet.getLong(2),
                    resultSet.getLong(3),
                    resultSet.getString(4));
            response.add(rec);
        }

        return response;
    }

    public void purgeProcessedData(String taskId) {

        Statement deleteStatement = Statement.newBuilder(" DELETE FROM UCS " + "WHERE WORKER = @worker ")
                .bind("worker")
                .to(taskId)
                .build();

        this.databaseClient
                .readWriteTransaction()
                .run(
                        transaction -> {
                            transaction.executeUpdate(deleteStatement);
                            return null;
                        });
    }
}
