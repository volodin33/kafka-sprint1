package kafkaapp.services;

import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import kafkaapp.messages.OrderMessage;

/** Адаптер {@link KafkaConsumer} под контракт {@link ConsumerAdapter}. */
public final class KafkaConsumerAdapter implements ConsumerAdapter {

    private final KafkaConsumer<String, OrderMessage> delegate;

    public KafkaConsumerAdapter(KafkaConsumer<String, OrderMessage> delegate) {
        this.delegate = delegate;
    }

    @Override
    public ConsumerRecords<String, OrderMessage> poll(Duration timeout) {
        return delegate.poll(timeout);
    }

    @Override
    public void commitSync() {
        delegate.commitSync();
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        delegate.commitSync(offsets);
    }

    @Override
    public void wakeup() {
        delegate.wakeup();
    }

    @Override
    public void close() {
        delegate.close();
    }
}
