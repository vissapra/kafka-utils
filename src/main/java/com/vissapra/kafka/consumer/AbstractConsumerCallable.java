package com.vissapra.kafka.consumer;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Boolean.TRUE;
import static java.time.Instant.now;

/**
 * Abstract Kafka consumer that supports longer operations upon message arrival.
 * Handles consumer.poll heartbeats until the task is completed.
 * Once completes, it resumes the poll operations
 */

public abstract class AbstractConsumerCallable<K, V> implements Callable<V> {


    private static final Logger LOG = LoggerFactory.getLogger(AbstractConsumerCallable.class);
    private final KafkaConsumer<K, V> consumer;
    private final List<String> topics;
    private final AtomicBoolean running;
    private final Integer pollTimeoutMs;
    private final ExecutorService messageHandler;
    private CountDownLatch shutdownLatch = new CountDownLatch(1);

    AbstractConsumerCallable(Properties properties, List<String> topics, int pollTimeoutMs) {
        this.consumer = new KafkaConsumer<>(properties);
        this.topics = topics;
        this.pollTimeoutMs = pollTimeoutMs;
        this.running = new AtomicBoolean(true);
        messageHandler = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat(String.format("%s-db-handler", topics.get(0))).build());
    }

    protected abstract void handle(List<ConsumerRecord<K, V>> records);

    public V call() {

        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                partitions.forEach(partition ->
                        LOG.info("Partition Revoked for Thread: {}, Topic:{} Partition: {}", Thread.currentThread().getName(),
                                partition.topic(), partition.partition()));
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                partitions.forEach(partition ->
                        LOG.info("Partition assigned for Thread: {}, Topic:{} Partition: {}", Thread.currentThread().getName(),
                                partition.topic(), partition.partition()));
            }
        });

        while (running.get()) {
            try {
                ConsumerRecords<K, V> records = consumer.poll(pollTimeoutMs);

                if (records.isEmpty()) {
                    continue;
                }

                List<ConsumerRecord<K, V>> consumerRecords = Lists.newArrayList(records.iterator());
                Set<TopicPartition> topicPartitions = consumer.assignment();
                consumer.pause(topicPartitions);

                Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadata = recordOffsets(consumerRecords);
                LOG.info("Paused for topic: {} on partitions: {}... ", topics, topicPartitions);
                Future<Boolean> future = messageHandler.submit(new ConsumeRecords(consumerRecords));

                boolean isCompleted = false;
                while (!isCompleted) {
                    try {
                        isCompleted = future.get(3, TimeUnit.SECONDS);
                    } catch (InterruptedException | ExecutionException | CancellationException e) {
                        LOG.error("Interruption/Cancellation of Task", e);
                    } catch (TimeoutException e) {
                        consumer.poll(0);// for hear-beat
                    }
                }

                consumer.resume(topicPartitions);
                LOG.info("...Resumed for topic: {} on partitions: {}", topics, topicPartitions);
                commitSync(topicPartitionOffsetAndMetadata);

            } catch (Exception e) {
                LOG.error(String.format("Unhandled exception for topic: %s", topics), e);
                consumer.unsubscribe();
                LOG.warn("Re-subscribing to the topic: {} due to close ...", topics);
                consumer.subscribe(topics);
            }
        }

        try {
            messageHandler.shutdown();
            while (!messageHandler.awaitTermination(5, TimeUnit.SECONDS)) ;
        } catch (InterruptedException ignored) {
        }

        consumer.close();
        shutdownLatch.countDown();
        LOG.error("Consumer thread: {} for topic: {} going down!!!!", Thread.currentThread().getName(), topics);
        return null;
    }

    /**
     * Commits the partitions and resumes the subscriptions
     *
     * @param topicPartitionToOffsetMetadata
     */
    private void commitSync(Map<TopicPartition, OffsetAndMetadata> topicPartitionToOffsetMetadata) {
        consumer.commitSync(topicPartitionToOffsetMetadata);
        topicPartitionToOffsetMetadata.entrySet().forEach(entry -> LOG.info("Offsets committed topic:{}, partition:{}, " +
                "offset: {} ", entry.getKey().topic(), entry.getKey().partition(), entry.getValue().offset()));

    }

    private Map<TopicPartition, OffsetAndMetadata> recordOffsets(List<ConsumerRecord<K, V>> batch) {
        LOG.debug("Recording offsets for batch with records: {}", batch.size());
        Map<TopicPartition, OffsetAndMetadata> topicPartitionToOffsetMetadata = new HashMap<>();
        batch.forEach(record -> {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            try {
                topicPartitionToOffsetMetadata.put(topicPartition, new OffsetAndMetadata(record.offset()));
            } catch (Exception e) {
                LOG.error("Error handling the message", e);
            }
        });
        LOG.debug("Recorded offsets for batch with records: {}", batch.size());
        return topicPartitionToOffsetMetadata;
    }

    void shutdown() throws InterruptedException {
        consumer.wakeup();
        running.set(false);
        shutdownLatch.await();
    }

    /**
     * Class that handles ConsumerRecords processing
     */
    private class ConsumeRecords implements Callable<Boolean> {

        private List<ConsumerRecord<K, V>> records;

        ConsumeRecords(List<ConsumerRecord<K, V>> records) {
            this.records = records;
        }

        @Override
        public Boolean call() throws Exception {
            LOG.info("Processing for topic: {} with {} records....", topics, records.size());
            long start = now().toEpochMilli();
            try {
                recordOffsets(records);
                handle(records);
            } catch (Exception e) {
                LOG.error(String.format("Error handling batch: %s", records), e);
            }

            LOG.info("....Processed for topic: {} with {} records in {}ms.", topics, records.size(), now().toEpochMilli() - start);
            return TRUE;
        }
    }

}
