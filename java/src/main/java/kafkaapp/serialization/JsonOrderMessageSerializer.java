package kafkaapp.serialization;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import kafkaapp.messages.OrderMessage;

/**
 * Сериализатор {@link OrderMessage} через Jackson (UTF-8 байты).
 *
 * Формат совместим с System.Text.Json (JsonSerializerDefaults.Web) из .NET-версии:
 * camelCase-имена полей ({@code {"id":1,"description":"..."}}), чтение без учёта
 * регистра имён, null/пустые данные → значения по умолчанию.
 */
public final class JsonOrderMessageSerializer implements OrderMessageSerde {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
        .enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
        .build();

    @Override
    public byte[] serialize(String topic, OrderMessage data) {
        if (data == null) {
            return null;
        }
        try {
            return MAPPER.writeValueAsBytes(data);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize OrderMessage to JSON", e);
        }
    }

    @Override
    public OrderMessage deserialize(String topic, byte[] data) {
        if (data == null) {
            // Tombstone: топик не compacted, удаления не используются —
            // осознанное решение, как в .NET-версии (isNull → пустое сообщение).
            return OrderMessage.EMPTY;
        }
        if (data.length == 0) {
            // Пустой payload — не валидный JSON: не маскируем битое сообщение (как в .NET).
            throw new RuntimeException("Empty payload is not a valid JSON message.");
        }
        try {
            OrderMessage value = MAPPER.readValue(data, OrderMessage.class);
            return value != null ? value : OrderMessage.EMPTY;
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to deserialize OrderMessage from JSON", e);
        }
    }
}
