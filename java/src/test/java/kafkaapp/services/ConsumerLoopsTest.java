package kafkaapp.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import kafkaapp.configuration.ConsumerOptions;
import kafkaapp.messages.OrderMessage;
import kafkaapp.services.consumerloops.BatchConsumerLoop;
import kafkaapp.services.consumerloops.SingleConsumerLoop;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * Поведение циклов чтения без живого Kafka: фейковый ConsumerAdapter
 * с заскриптованными выборками; остановка — через флаг после коммита.
 */
class ConsumerLoopsTest {

    private static final TopicPartition TP = new TopicPartition("orders", 0);

    private static ConsumerRecords<String, OrderMessage> records(OrderMessage... messages) {
        List<ConsumerRecord<String, OrderMessage>> list = new java.util.ArrayList<>();
        long offset = 0;
        for (OrderMessage message : messages) {
            list.add(new ConsumerRecord<>(TP.topic(), TP.partition(), offset++, "key", message));
        }
        return new ConsumerRecords<>(Map.of(TP, list));
    }

    private static ConsumerRecords<String, OrderMessage> empty() {
        return new ConsumerRecords<>(Map.of());
    }

    private static ConsumerOptions options(String commitModeRaw) {
        // name, topic, groupId, enabled, autoOffsetResetRaw, commitModeRaw,
        // fetchMinBytes, fetchMaxBytes, fetchWaitMaxMs, batchSize, batchTimeoutMs,
        // readModeRaw, serializationFormatRaw
        return new ConsumerOptions("c1", "orders", "g", true,
            "Earliest", commitModeRaw, null, null, null, 10, 5000, "Batch", "Json");
    }

    @Test
    void batchLoopCommitsOncePerBatch() {
        FakeConsumerAdapter fake = new FakeConsumerAdapter();
        fake.polls.add(records(msg(1), msg(2), msg(3)));
        AtomicBoolean stopped = new AtomicBoolean();
        BatchConsumerLoop loop = new BatchConsumerLoop();

        loop.run(fake, options("Manual"), () -> stopped.get() || fake.stopAfterCommit(stopped));

        assertEquals(1, fake.noArgCommits);
        assertEquals(0, fake.mapCommits);
    }

    @Test
    void batchLoopCapsBatchAtBatchSize() {
        FakeConsumerAdapter fake = new FakeConsumerAdapter();
        // 25 сообщений в одной выборке при BatchSize=10 — пачка обрезается до 10.
        OrderMessage[] burst = new OrderMessage[25];
        for (int i = 0; i < burst.length; i++) {
            burst[i] = msg(i + 1);
        }
        fake.polls.add(records(burst));
        AtomicBoolean stopped = new AtomicBoolean();
        BatchConsumerLoop loop = new BatchConsumerLoop();

        loop.run(fake, options("Manual"), () -> stopped.get() || fake.stopAfterCommit(stopped));

        assertEquals(1, fake.noArgCommits);
    }

    @Test
    void batchLoopDoesNotCommitInAutoMode() {
        FakeConsumerAdapter fake = new FakeConsumerAdapter();
        fake.polls.add(records(msg(1), msg(2)));
        AtomicBoolean stopped = new AtomicBoolean();
        BatchConsumerLoop loop = new BatchConsumerLoop();

        // В Auto-режиме коммита нет — останавливаемся после первой пачки вручную.
        loop.run(fake, options("Auto"), () -> stopped.getAndSet(true));

        assertEquals(0, fake.noArgCommits);
    }

    @Test
    void singleLoopCommitsEveryMessageIndividually() {
        FakeConsumerAdapter fake = new FakeConsumerAdapter();
        fake.polls.add(records(msg(1), msg(2), msg(3)));
        AtomicBoolean stopped = new AtomicBoolean();
        SingleConsumerLoop loop = new SingleConsumerLoop();

        loop.run(fake, options("Manual"), () -> stopped.get() || fake.stopAfterMapCommit(stopped, 3));

        assertEquals(3, fake.mapCommits);
        assertEquals(0, fake.noArgCommits);
    }

    private static OrderMessage msg(int id) {
        return new OrderMessage(id, "description-" + id);
    }

    private static final class FakeConsumerAdapter implements ConsumerAdapter {
        private final Queue<ConsumerRecords<String, OrderMessage>> polls = new ArrayDeque<>();
        private int noArgCommits;
        private int mapCommits;

        @Override
        public ConsumerRecords<String, OrderMessage> poll(Duration timeout) {
            ConsumerRecords<String, OrderMessage> next = polls.poll();
            return next != null ? next : empty();
        }

        @Override
        public void commitSync() {
            noArgCommits++;
        }

        @Override
        public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
            mapCommits++;
        }

        private boolean stopAfterCommit(AtomicBoolean stopped) {
            if (noArgCommits > 0) {
                stopped.set(true);
                return true;
            }
            return false;
        }

        private boolean stopAfterMapCommit(AtomicBoolean stopped, int count) {
            if (mapCommits >= count) {
                stopped.set(true);
                return true;
            }
            return false;
        }

        @Override
        public void wakeup() {
        }

        @Override
        public void close() {
        }
    }
}
