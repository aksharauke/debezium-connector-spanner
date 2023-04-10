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

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.db.model.event.DataChangeEvent;
import io.debezium.connector.spanner.db.model.event.HeartbeatEvent;

public class UnifiedPublisher {
    private static final Logger LOGGER = getLogger(UnifiedPublisher.class);

    private final KafkaProducer<String, String> producer;

    private final KafkaProducer<String, String> producerB;

    private final KafkaProducer<String, String> orderedProducer;

    private final KafkaProducer<String, String> wmProducer;

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

        Properties properties2 = config.kafkaProps(
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

        this.producerB = new KafkaProducer<>(properties2);

        Properties properties3 = config.kafkaProps(
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

        this.orderedProducer = new KafkaProducer<>(properties3);

        Properties properties4 = config.kafkaProps(
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

        this.wmProducer = new KafkaProducer<>(properties4);
    }

    public void putData(DataChangeEvent event) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>("ucs", "dummyKey", event.toString());
            producer.send(record).get();
            producer.flush();
        }
        catch (ExecutionException e) {
            producer.close();
            LOGGER.info("Error during publishing to the data to ucs Topic", e);
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
            LOGGER.info("Error during publishing to the ucs Topic", e);
        }
        catch (InterruptedException e) {
        }
    }

    public long saveForLater(String event) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>("ucsb", "dummyKey", event);
            long offset = producerB.send(record).get().offset();
            producerB.flush();
            return offset;
        }
        catch (ExecutionException e) {
            producerB.close();
            LOGGER.info("Error during publishing to the ucsb Topic", e);
            return -1;
        }
        catch (InterruptedException e) {
            return -1;
        }
    }

    public void putFinishedPartition(String partition) {
        try {

            String ts = Timestamp.now().toString();
            String rec = "FinishedPartition='" + partition + "',timestamp=" + ts;
            ProducerRecord<String, String> record = new ProducerRecord<>("ucs", "dummyKey", rec);
            producer.send(record).get();
            producer.flush();
        }
        catch (ExecutionException e) {
            producer.close();
            LOGGER.info("Error during publishing finished to the ucs Topic", e);
        }
        catch (InterruptedException e) {
        }
    }

    public void putOrderedData(String rec) {
        // try {

        ProducerRecord<String, String> record = new ProducerRecord<>("ordout", "dummyKey", rec);
        orderedProducer.send(record);
        // orderedProducer.flush();
        // }
        /*
         * catch (ExecutionException e) {
         * orderedProducer.close();
         * LOGGER.info("Error during publishing to the ordout Topic", e);
         * }
         */
        // catch (InterruptedException e) {
        // }
    }

    public void flushOrderedData() {
        try {

            orderedProducer.flush();
        }
        catch (Exception e) {
            orderedProducer.close();
            LOGGER.info("Error during flush to ordout Topic", e);
        }
    }

    public void putWatermark(String rec) {
        try {

            ProducerRecord<String, String> record = new ProducerRecord<>("wm", "dummyKey", rec);
            wmProducer.send(record).get();
            wmProducer.flush();

        }
        catch (ExecutionException e) {
            wmProducer.close();
            LOGGER.info("Error during publishing to the wm Topic", e);
        }
        catch (InterruptedException e) {
        }
    }
}
