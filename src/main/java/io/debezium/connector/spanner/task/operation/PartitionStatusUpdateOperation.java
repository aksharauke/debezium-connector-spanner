/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task.operation;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.DaoFactory;
import io.debezium.connector.spanner.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.task.TaskSyncContext;

/** Change the status of partition: {@link PartitionStateEnum} */
public class PartitionStatusUpdateOperation implements Operation {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionStatusUpdateOperation.class);
    private final String token;
    private final PartitionStateEnum partitionStateEnum;
    private final DaoFactory daoFactory;

    public PartitionStatusUpdateOperation(
                                          String token, PartitionStateEnum partitionStateEnum, DaoFactory daoFactory) {
        this.token = token;
        this.partitionStateEnum = partitionStateEnum;
        this.daoFactory = daoFactory;
    }

    @Override
    public boolean isRequiredPublishSyncEvent() {
        return true;
    }

    @Override
    public TaskSyncContext doOperation(TaskSyncContext taskSyncContext) {
        return setState(taskSyncContext);
    }

    private TaskSyncContext setState(TaskSyncContext taskSyncContext) {
        TaskState currentTaskState = taskSyncContext.getCurrentTaskState();

        List<PartitionState> partitionsList = currentTaskState.getPartitions().stream()
                .map(
                        partitionState -> {
                            if (partitionState.getToken().equals(token)) {
                                if (PartitionStateEnum.FINISHED.equals(partitionStateEnum)) {
                                    // mark the partition as finished in the metadata table
                                    this.daoFactory
                                            .getPartitionMetadataDao()
                                            .updateStateToFinshed(partitionState);
                                    return partitionState.toBuilder()
                                            .state(partitionStateEnum)
                                            .finishedTimestamp(Timestamp.now())
                                            .build();
                                }
                                // update entry in metadata table for running partition
                                if (PartitionStateEnum.RUNNING.equals(partitionStateEnum)) {
                                    this.daoFactory
                                            .getPartitionMetadataDao()
                                            .writeRunningPartition(partitionState);
                                }
                                return partitionState.toBuilder().state(partitionStateEnum).build();
                            }
                            return partitionState;
                        })
                .collect(Collectors.toList());

        LOGGER.info(
                "Task updated status for partition, taskUid: {}, partition: {}, status: {}",
                taskSyncContext.getTaskUid(),
                token,
                partitionStateEnum);

        return taskSyncContext.toBuilder()
                .currentTaskState(currentTaskState.toBuilder().partitions(partitionsList).build())
                .build();
    }
}
