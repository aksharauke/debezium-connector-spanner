/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db;

import com.google.cloud.spanner.Options;

import io.debezium.connector.spanner.db.dao.ChangeStreamDao;
import io.debezium.connector.spanner.db.dao.PartitionMetadataDao;
import io.debezium.connector.spanner.db.dao.SchemaDao;
import io.debezium.connector.spanner.db.dao.UberChangeStreamDao;

/** Factory for {@code ChangeStreamDao} */
public class DaoFactory {
    private final DatabaseClientFactory databaseClientFactory;

    private SchemaDao schemaDao;
    private UberChangeStreamDao ucsDao;
    private PartitionMetadataDao partitionMetadataDao;

    public DaoFactory(DatabaseClientFactory databaseClientFactory) {
        this.databaseClientFactory = databaseClientFactory;
    }

    public SchemaDao getSchemaDao() {
        if (schemaDao != null) {
            return schemaDao;
        }
        this.schemaDao = new SchemaDao(this.databaseClientFactory.getDatabaseClient());
        return schemaDao;
    }

    public ChangeStreamDao getStreamDao(
                                        String changeStreamName, Options.RpcPriority rpcPriority, String jobName) {
        return new ChangeStreamDao(
                changeStreamName, this.databaseClientFactory.getDatabaseClient(), rpcPriority, jobName);
    }

    public UberChangeStreamDao getUCSDao() {
        if (ucsDao != null) {
            return ucsDao;
        }
        this.ucsDao = new UberChangeStreamDao(this.databaseClientFactory.getDatabaseClient());
        return ucsDao;
    }

    public PartitionMetadataDao getPartitionMetadataDao() {
        if (partitionMetadataDao != null) {
            return partitionMetadataDao;
        }
        this.partitionMetadataDao = new PartitionMetadataDao(this.databaseClientFactory.getDatabaseClient());
        return partitionMetadataDao;
    }
}
