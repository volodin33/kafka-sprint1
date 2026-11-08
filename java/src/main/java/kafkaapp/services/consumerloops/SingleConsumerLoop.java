package kafkaapp.services.consumerloops;

import java.time.Duration;
import java.util.Map;
import java.util.function.BooleanSupplier;
import kafkaapp.configuration.CommitMode;
import kafkaapp.configuration.ConsumerOptions;
import kafkaapp.messages.OrderMessage;
import kafkaapp.services.ConsumerAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Последовательная обработка сообщений консюмером (ReadMode.Single).
 * Порт SingleConsumerLoop .NET-версии: одно сообщение за итерацию,
 * при CommitMode.Manual — коммит offset'а каждого сообщения после обработки.
 * Ошибки poll/consume логируются, цикл продолжается (требование ТЗ).
 */
public final class SingleConsumerLoop {

    private static final Logger log = LoggerFactory.getLogger(SingleConsumerLoop.class);
    private static final Duration POLL_SLICE = Duration.ofSeconds(1);

    public void run(ConsumerAdapter consumer, ConsumerOptions options, BooleanSupplier stopped) {
        while (!stopped.getAsBoolean()) {
            try {
                ConsumerRecords<String, OrderMessage> records = consumer.poll(POLL_SLICE);
                for (ConsumerRecord<String, OrderMessage> record : records) {
                    log.info("Consumer '{}' received message {}[{}]@{}: Id={} Description={}",
                        options.name(), record.topic(), record.partition(), record.offset(),
                        record.value().id(), record.value().description());

                    if (options.commitMode() == CommitMode.Manual) {
                        consumer.commitSync(Map.of(
                            new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1)));
                        log.info("Consumer '{}' committed {}[{}]@{}",
                            options.name(), record.topic(), record.partition(), record.offset());
                    }
                }
            } catch (WakeupException e) {
                throw e;
            } catch (KafkaException e) {
                log.error("Consumer '{}' consume error: {}", options.name(), e.getMessage(), e);
            }
        }
    }
}
