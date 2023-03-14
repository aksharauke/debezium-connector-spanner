/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.dao;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;

import io.debezium.connector.spanner.context.source.SourceInfo;
import io.debezium.connector.spanner.db.model.event.BufferedPayload;
import io.debezium.connector.spanner.db.model.event.DataChangeEvent;
import io.debezium.connector.spanner.db.model.event.HeartbeatEvent;

/** Provides functionality to read Spanner DB buffered change stream across all partitions */
public class UberChangeStreamDao {

    private final DatabaseClient databaseClient;

    public UberChangeStreamDao(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    public void writeDataRecord(DataChangeEvent data, SourceInfo source) {

        Instant commitInstant = data.getCommitTimestamp().toSqlTimestamp().toInstant();

        long commitTs = ChronoUnit.MICROS.between(Instant.EPOCH, commitInstant);

        String trxId = data.getServerTransactionId();

        long recSeq = Long.parseLong(data.getRecordSequence());

        // long wm = ChronoUnit.MICROS.between(Instant.EPOCH, source.getLowWatermark());

        String part = data.getPartitionToken();

        long numRec = data.getNumberOfRecordsInTransaction();

        String payload = data.toString();

        String id = UUID.randomUUID().toString();

        List<Mutation> mutations = new ArrayList<>();
        mutations.add(
                Mutation.newInsertBuilder("UCS")
                        .set("ID")
                        .to(id)
                        .set("CT")
                        .to(commitTs)
                        .set("TRX_ID")
                        .to(trxId)
                        .set("REC_SEQ")
                        .to(recSeq)
                        .set("NUM_REC")
                        .to(numRec)
                        .set("WM")
                        .to(commitTs) // watermark is not needed
                        .set("PART")
                        .to(part)
                        .set("PAYLOAD")
                        .to(payload)
                        .build());

        this.databaseClient.write(mutations);

        // all records of the same partition_token
        // will always be in order hence no need
        // to check where wm < commitTs
        Statement updateStatement = Statement.newBuilder("UPDATE UCS " + "SET WM = @waterM " + "WHERE PART = @part")
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

    public void updateWaterMark(HeartbeatEvent hearbeat) {

        Instant commitInstant = hearbeat.getTimestamp().toSqlTimestamp().toInstant();
        long commitTs = ChronoUnit.MICROS.between(Instant.EPOCH, commitInstant);

        String part = hearbeat.getMetadata().getPartitionToken();
        Statement updateStatement = Statement.newBuilder("UPDATE UCS " + "SET WM = @waterM " + "WHERE PART = @part")
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

    private boolean canWork(String taskId) {
        Statement updateStatement = Statement.newBuilder(
                "UPDATE UCS "
                        + "SET WORKER = @worker "
                        + " WHERE CT < ( SELECT MIN(WM) FROM UCS)"
                        + " AND WORKER IS NULL")
                .bind("worker")
                .to(taskId)
                .build();

        return this.databaseClient
                .readWriteTransaction()
                .run(
                        transaction -> {
                            long rowCount = transaction.executeUpdate(updateStatement);
                            if (rowCount == 0l)
                                return false;
                            else
                                return true;
                        });
    }

    public List<BufferedPayload> getNext(String taskId) {

        if (!canWork(taskId))
            return null;

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
