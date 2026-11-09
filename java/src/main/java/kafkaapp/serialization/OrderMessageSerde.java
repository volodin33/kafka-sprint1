package kafkaapp.serialization;

import kafkaapp.messages.OrderMessage;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Комбинированный контракт сериализатора / десериализатора для {@link OrderMessage}.
 * Реализует оба интерфейса Kafka-клиента, поэтому экземпляр можно передавать
 * напрямую в KafkaProducer/KafkaConsumer как value (de)serializer.
 */
public interface OrderMessageSerde extends Serializer<OrderMessage>, Deserializer<OrderMessage> {

    /** Сериализация в байты значения сообщения. */
    @Override
    byte[] serialize(String topic, OrderMessage data);

    /** Десериализация из байтов значения сообщения. */
    @Override
    OrderMessage deserialize(String topic, byte[] data);

    /** Единая реализация close() вместо конфликтующих default у Serializer/Deserializer. */
    @Override
    default void close() {
        Serializer.super.close();
    }

    /** Единая no-op реализация configure() вместо конфликтующих default. */
    @Override
    default void configure(java.util.Map<String, ?> configs, boolean isKey) {
        // Stateless-сериализаторы: конфигурация не требуется.
    }
}
