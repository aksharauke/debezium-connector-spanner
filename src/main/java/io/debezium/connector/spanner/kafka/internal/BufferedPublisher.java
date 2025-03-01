/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.kafka.internal;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.slf4j.Logger;

/**
 * This class allows to publish the latest buffered value
 * once per time period, except the case: if the value is required
 * to be published immediately.
 */
public class BufferedPublisher<V> {

    private static final Logger LOGGER = getLogger(BufferedPublisher.class);

    private final Thread thread;
    private final AtomicReference<V> value = new AtomicReference<>();
    private final Predicate<V> publishImmediately;
    private final Consumer<V> onPublish;
    private final String taskUid;

    public BufferedPublisher(String taskUid, String name, long timeout, Predicate<V> publishImmediately, Consumer<V> onPublish) {
        this.publishImmediately = publishImmediately;
        this.onPublish = onPublish;
        this.taskUid = taskUid;

        this.thread = new Thread(() -> {
            Instant lastUpdatedTime = Instant.now();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (Instant.now().isAfter(lastUpdatedTime.plus(Duration.ofSeconds(600)))) {
                        LOGGER.info(
                                "Task Uid {} is still publishing with AtomicReference value {}",
                                this.taskUid,
                                (this.value.get() == null));
                        lastUpdatedTime = Instant.now();
                    }
                    publishBuffered();
                    Thread.sleep(timeout);
                }
                catch (InterruptedException e) {
                    return;
                }
            }
        }, "SpannerConnector-" + name);
    }

    public void buffer(V update) {
        if (publishImmediately.test(update)) {
            synchronized (this) {
                this.value.set(null);
                this.onPublish.accept(update);
            }
        }
        else {
            value.set(update);
        }
    }

    private synchronized void publishBuffered() {
        V item = this.value.getAndSet(null);

        if (item != null) {
            this.onPublish.accept(item);
        }
    }

    public void start() {
        this.thread.start();
    }

    public void close() {
        thread.interrupt();
        while (!thread.getState().equals(Thread.State.TERMINATED)) {
        }
    }
}