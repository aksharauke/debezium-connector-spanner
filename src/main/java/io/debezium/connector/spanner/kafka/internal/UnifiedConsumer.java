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
import io.debezium.connector.spanner.kafka.internal.model.InterimRecord;

/** */
public class UnifiedConsumer {
    private static final Logger LOGGER = getLogger(UnifiedConsumer.class);
    private KafkaConsumer<String, String> consumerUcs;
    private KafkaConsumer<String, String> consumerUcsb;
    private KafkaConsumer<String, String> consumerWm;

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

    private boolean isPauseOnUcsbCalled;
    private long pauseOffsetForUcsb;
    private boolean crossedRead;

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
                        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                        900000,
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
                        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                        900000,
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                        false));
        consumerUcsb = new KafkaConsumer<>(props2);

        final Properties props3 = config.kafkaProps(
                Map.of(
                        ConsumerConfig.GROUP_ID_CONFIG,
                        "testwm",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class.getName(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "earliest",
                        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                        1200000,
                        ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                        1,
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                        false));
        consumerWm = new KafkaConsumer<>(props3);

        consumerUcs.subscribe(Arrays.asList("ucs"));
        consumerUcsb.subscribe(Arrays.asList("ucsb"));
        consumerWm.subscribe(Arrays.asList("wm"));

        recordInitUcsb = false;
        recordInitUcs = false;
        commitForUcsb = new HashMap<>();
        commitForUcs = new HashMap<>();

        isPauseOnUcsbCalled = false;
        pauseOffsetForUcsb = -1;
        crossedRead = false;
    }

    private void recordsInitUcsb() {
        // consumerUcsb.poll(0);
        recordsUcsb = consumerUcsb.poll(Duration.ofMillis(100));
        recordInitUcsb = true;

        if (recordsUcsb == null) {
            LOGGER.info("#####  recordsUcb is null");
        }
        else {
            LOGGER.info("##### Size of the recordsUcb " + recordsUcsb.count());
        }

        if (recordsUcsb.count() == 0) {
            /*
             * We want to return from here, since if there
             * were any messages read in the previous call
             * then we want the partitionRecordsUcsb to have
             * data needed to commit the offsets
             */
            return;
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
        // consumerUcs.poll(0);

        recordsUcs = consumerUcs.poll(Duration.ofMillis(100));
        recordInitUcs = true;

        if (recordsUcs == null) {
            LOGGER.info("#####  recordsUcs is null");
        }
        else {
            LOGGER.info("##### Size of the recordsUcs " + recordsUcs.count());
        }

        if (recordsUcs.count() == 0) {
            return;
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

    private InterimRecord readFromUcs() {
        LOGGER.info("##### Reading from ucs");
        if (!recordInitUcs) {
            LOGGER.info("##### Init of ucs");
            recordsInitUcs();
        }

        if (recordsUcs.count() == 0) {
            recordInitUcs = false;
            return new InterimRecord("", false);
        }

        if (ucsIndex == partitionRecordsUcs.size()) {
            LOGGER.info("##### ucs exhausted will reread  ");
            recordInitUcs = false;
            return readFromUcs();

        }
        else {
            ConsumerRecord<String, String> rec = partitionRecordsUcs.get(ucsIndex);
            ucsIndex++;
            LOGGER.info("##### Found record in ucs with offset : " + rec.offset());
            return new InterimRecord(rec.value(), false);
        }
    }

    public InterimRecord getRecord() {

        if (!recordInitUcsb) {
            LOGGER.info("##### Init of ucsb records");
            recordsInitUcsb();
        }

        if (recordsUcsb.count() == 0
                || ucsbIndex == partitionRecordsUcsb.size()
                || partitionRecordsUcsb.size() == 0) {
            // no more records to read in this iteration
            // read from Ucs
            if (recordsUcsb.count() != 0 && !crossedRead) {
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
            LOGGER.info("##### Found ucsb record with offset: " + rec.offset());
            if (isPauseOnUcsbCalled) {
                if (rec.offset() == pauseOffsetForUcsb) { // at this point,stop reading from ucsb
                    ucsbIndex--;
                    crossedRead = true;
                    LOGGER.info("##### Paused reading from ucsb, will read from ucs");
                    return readFromUcs();
                }
            }

            return new InterimRecord(rec.value(), true);
        }
    }

    public void pauseAt(long offset) {
        /*
         * We are here to avoid infinite loop
         * while reading from ucsb.
         * At this stage, the commit to ucsb must point to this offset
         * so that this is read again
         * plus when we reach this offset while reading readFromUcs
         * we should stop
         */
        pauseOffsetForUcsb = offset;
        isPauseOnUcsbCalled = true;
    }

    public void commitOffsets() {

        try {
            if (partitionRecordsUcsb != null && partitionRecordsUcsb.size() != 0) {
                if (isPauseOnUcsbCalled) {
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> pair : commitForUcsb.entrySet()) {
                        commitForUcsb.put(
                                pair.getKey(), new OffsetAndMetadata(Long.valueOf(pauseOffsetForUcsb)));
                        break;
                    }
                    LOGGER.info("##### Committing ucsb offset at pause " + pauseOffsetForUcsb);
                    consumerUcsb.commitSync(commitForUcsb);
                    isPauseOnUcsbCalled = false;
                    pauseOffsetForUcsb = -1;
                    crossedRead = false;
                }
                else if (ucsbIndex > 0) {
                    long lastOffset = partitionRecordsUcsb.get(ucsbIndex - 1).offset();
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> pair : commitForUcsb.entrySet()) {
                        commitForUcsb.put(pair.getKey(), new OffsetAndMetadata(Long.valueOf(lastOffset + 1)));
                        break;
                    }
                    LOGGER.info("##### Committing ucsb offset " + lastOffset);
                    consumerUcsb.commitSync(commitForUcsb);
                }
                // consumerUcsb.commitSync();
            }
            recordInitUcsb = false; // since we want to poll again once offset is committed
            ucsbIndex = 0;

            if (partitionRecordsUcs != null && partitionRecordsUcs.size() != 0) {

                if (ucsIndex > 0) {
                    long lastOffset = partitionRecordsUcs.get(ucsIndex - 1).offset();
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> pair : commitForUcs.entrySet()) {
                        commitForUcs.put(pair.getKey(), new OffsetAndMetadata(Long.valueOf(lastOffset + 1)));
                        break;
                    }
                    LOGGER.info("##### Committing ucs offset " + lastOffset);

                    consumerUcs.commitSync(commitForUcs);
                }
                // consumerUcs.commitSync();
            }
            recordInitUcs = false; // since we want to poll again once offset is committed
            ucsbIndex = 0;
            consumerWm.commitSync();
        }
        catch (Exception ex) {

            throw new SpannerConnectorException("Error while committing offsets in consumer", ex);
        }
    }

    public long getWatermark() {
        ConsumerRecords<String, String> recordsWm = consumerWm.poll(Duration.ofMillis(100));
        if (recordsWm.count() == 0) {
            return 0l;
        }
        long wm = 0l;
        for (ConsumerRecord<String, String> record : recordsWm) {
            wm = Long.parseLong(record.value());
            break;
        }
        return wm;
    }
}
