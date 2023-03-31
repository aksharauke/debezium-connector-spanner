package io.debezium.connector.spanner.kafka.internal;

/**
 * Publishes the data and heartbeat to single Kafka topic
 */
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.db.model.event.DataChangeEvent;
import io.debezium.connector.spanner.db.model.event.HeartbeatEvent;

public class UnifiedPublisher {
    private static final Logger LOGGER = getLogger(UnifiedPublisher.class);

    private final KafkaProducer<String, String> producer;

    public UnifiedPublisher(SpannerConnectorConfig config) {
        Properties properties = config.kafkaProps(
                Map.of(
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        StringSerializer.class.getName(),
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        StringSerializer.class.getName(),
                        ProducerConfig.ACKS_CONFIG,
                        "1",
                        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                        String.valueOf(config.syncRequestTimeout()),
                        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                        String.valueOf(config.syncDeliveryTimeout())));

        this.producer = new KafkaProducer<>(properties);
    }

    public void putData(DataChangeEvent event) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>("ucs", "dummyKey", event.toString());
            producer.send(record).get();
            producer.flush();
        }
        catch (ExecutionException e) {
            producer.close();
            LOGGER.info("Error during publishing to the Sync Topic", e);
        }
        catch (InterruptedException e) {
        }
    }

    public void putHeartBeat(HeartbeatEvent event) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>("ucs", "dummyKey", event.toString());
            producer.send(record).get();
            producer.flush();
        }
        catch (ExecutionException e) {
            producer.close();
            LOGGER.info("Error during publishing to the Sync Topic", e);
        }
        catch (InterruptedException e) {
        }
    }

    public void saveForLater(String event) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>("ucsb", "dummyKey", event);
            producer.send(record).get();
            producer.flush();
        }
        catch (ExecutionException e) {
            producer.close();
            LOGGER.info("Error during publishing to the Sync Topic", e);
        }
        catch (InterruptedException e) {
        }
    }
}
