package io.debezium.connector.spanner.kafka.internal;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.exception.SpannerConnectorException;

/** */
public class UnifiedConsumer {
    private static final Logger LOGGER = getLogger(UnifiedConsumer.class);
    private KafkaConsumer<String, String> consumerUcs;
    private KafkaConsumer<String, String> consumerUcsb;

    private boolean recordInitUcsb;
    private boolean recordInitUcs;
    private ConsumerRecords<String, String> recordsUcsb;
    private ConsumerRecords<String, String> recordsUcs;
    private Map<TopicPartition, OffsetAndMetadata> commitForUcsb;
    private Map<TopicPartition, OffsetAndMetadata> commitForUcs;
    List<ConsumerRecord<String, String>> partitionRecordsUcsb;
    List<ConsumerRecord<String, String>> partitionRecordsUcs;
    private long lastOffsetForUcsb;
    private long lastOffsetForUcs;
    private int ucsbIndex;
    private int ucsIndex;

    public UnifiedConsumer(SpannerConnectorConfig config) {

        final Properties props = config.kafkaProps(
                Map.of(
                        ConsumerConfig.GROUP_ID_CONFIG,
                        "test123",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class.getName(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                        false));

        consumerUcs = new KafkaConsumer<>(props);

        final Properties props2 = config.kafkaProps(
                Map.of(
                        ConsumerConfig.GROUP_ID_CONFIG,
                        "test42",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class.getName(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                        false));
        consumerUcsb = new KafkaConsumer<>(props2);

        consumerUcs.subscribe(Arrays.asList("ucs"));
        consumerUcsb.subscribe(Arrays.asList("ucsb"));
        recordInitUcsb = false;
        recordInitUcs = false;
        commitForUcsb = new HashMap<>();
        commitForUcs = new HashMap<>();
    }

    private void recordsInitUcsb() {
        consumerUcsb.poll(0);
        recordsUcsb = consumerUcsb.poll(Duration.ofMillis(100));
        recordInitUcsb = true;

        if (recordsUcsb == null) {
            LOGGER.info("#####  recordsUcb is null");
        }
        else {
            LOGGER.info("##### Size of the recordsUcb " + recordsUcsb.count());
        }

        if (partitionRecordsUcsb != null) // partitionRecordsUcsb.clear();
            partitionRecordsUcsb = null;

        // we know there will only be one Partition

        for (TopicPartition partition : recordsUcsb.partitions()) {
            partitionRecordsUcsb = recordsUcsb.records(partition);
            LOGGER.info("##### Size of ucsb records: " + partitionRecordsUcsb.size());
            ucsbIndex = 0;
            commitForUcsb.clear();
            commitForUcsb.put(partition, new OffsetAndMetadata(Long.valueOf(ucsbIndex + 1)));
            break;
        }
    }

    private void recordsInitUcs() {
        consumerUcs.poll(0);

        recordsUcs = consumerUcs.poll(Duration.ofMillis(100));
        recordInitUcs = true;

        if (recordsUcs == null) {
            LOGGER.info("#####  recordsUcs is null");
        }
        else {
            LOGGER.info("##### Size of the recordsUcs " + recordsUcs.count());
        }

        if (partitionRecordsUcs != null)
            // partitionRecordsUcs.clear(); this would give error for some reason
            partitionRecordsUcs = null;

        for (TopicPartition partition : recordsUcs.partitions()) {
            partitionRecordsUcs = recordsUcs.records(partition);
            LOGGER.info("##### Size of the ucs records: " + partitionRecordsUcs.size());
            ucsIndex = 0;
            commitForUcs.clear();
            commitForUcs.put(partition, new OffsetAndMetadata(Long.valueOf(ucsIndex + 1)));
            break;
        }
    }

    private String readFromUcs() {
        LOGGER.info("##### Reading from ucs");
        if (!recordInitUcs) {
            LOGGER.info("##### Init of ucs");
            recordsInitUcs();
        }

        if (partitionRecordsUcs == null || partitionRecordsUcs.size() == 0) {
            recordInitUcs = false;
            return "";
        }

        if (ucsIndex == partitionRecordsUcs.size()) {
            LOGGER.info("##### ucs exhausted will reread  ");
            recordInitUcs = false;
            return readFromUcs();

        }
        else {
            ConsumerRecord<String, String> rec = partitionRecordsUcs.get(ucsIndex);
            ucsIndex++;
            LOGGER.info("##### Found record in ucs: " + rec.value());
            return rec.value();
        }
    }

    public String getRecord() {

        if (!recordInitUcsb) {
            LOGGER.info("##### Init of ucsb records");
            recordsInitUcsb();
        }

        if (partitionRecordsUcsb == null
                || ucsbIndex == partitionRecordsUcsb.size()
                || partitionRecordsUcsb.size() == 0) {
            // no more records to read in this iteration
            // read from Ucs
            if (partitionRecordsUcsb != null && partitionRecordsUcsb.size() > 0) {
                LOGGER.info("##### Rereading from ucsb to finish all records");
                recordInitUcsb = false;
                return getRecord();
            }
            else
                return readFromUcs();

        }
        else {
            ConsumerRecord<String, String> rec = partitionRecordsUcsb.get(ucsbIndex);
            ucsbIndex++;
            LOGGER.info("##### Found ucsb record: " + rec.value());
            return rec.value();
        }
    }

    public void commitOffsets() {

        try {
            if (partitionRecordsUcsb != null && partitionRecordsUcsb.size() != 0) {

                long lastOffset = partitionRecordsUcsb.get(ucsbIndex - 1).offset();
                for (Map.Entry<TopicPartition, OffsetAndMetadata> pair : commitForUcsb.entrySet()) {
                    commitForUcsb.put(pair.getKey(), new OffsetAndMetadata(Long.valueOf(lastOffset + 1)));
                    break;
                }

                consumerUcsb.commitSync(commitForUcsb);
                recordInitUcsb = false; // since we want to poll again once offset is committed
                ucsbIndex = 0;
            }

            if (partitionRecordsUcs != null && partitionRecordsUcs.size() != 0) {

                long lastOffset = partitionRecordsUcs.get(ucsIndex - 1).offset();
                for (Map.Entry<TopicPartition, OffsetAndMetadata> pair : commitForUcs.entrySet()) {
                    commitForUcs.put(pair.getKey(), new OffsetAndMetadata(Long.valueOf(lastOffset + 1)));
                    break;
                }

                consumerUcs.commitSync(commitForUcs);
                recordInitUcsb = false; // since we want to poll again once offset is committed
                ucsbIndex = 0;
            }
        }
        catch (Exception ex) {

            throw new SpannerConnectorException("Error while committing offsets in consumer", ex);
        }
    }
}
