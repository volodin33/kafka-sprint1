package kafkaapp.serialization;

import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Collectors;
import kafkaapp.configuration.SerializationFormat;

/**
 * Реестр сериализаторов сообщений: формат → экземпляр {@link OrderMessageSerde}.
 * В .NET-версии реестр собирался reflection-сканом сборки; в Java регистрация
 * явная (атрибутного discovery нет).
 */
public final class SerializerRegistry {

    private final Map<SerializationFormat, OrderMessageSerde> serializers;

    public SerializerRegistry(Map<SerializationFormat, OrderMessageSerde> serializers) {
        if (serializers.isEmpty()) {
            throw new IllegalStateException(
                "No serializers registered. Add serializers for known formats.");
        }
        this.serializers = new EnumMap<>(serializers);
    }

    /** Реестр с сериализаторами по умолчанию (JSON и Protobuf). */
    public static SerializerRegistry withDefaults() {
        Map<SerializationFormat, OrderMessageSerde> map = new EnumMap<>(SerializationFormat.class);
        map.put(SerializationFormat.Json, new JsonOrderMessageSerializer());
        map.put(SerializationFormat.Protobuf, new ProtobufOrderMessageSerializer());
        return new SerializerRegistry(map);
    }

    /**
     * Возвращает сериализатор для указанного формата.
     * Бросает {@link IllegalStateException}, если формат не зарегистрирован.
     */
    public OrderMessageSerde resolve(SerializationFormat format) {
        OrderMessageSerde serde = serializers.get(format);
        if (serde != null) {
            return serde;
        }
        String registered = serializers.keySet().stream()
            .map(Enum::name)
            .collect(Collectors.joining(", "));
        throw new IllegalStateException(
            "No serializer registered for format '" + format + "'. Registered: " + registered + ".");
    }
}
