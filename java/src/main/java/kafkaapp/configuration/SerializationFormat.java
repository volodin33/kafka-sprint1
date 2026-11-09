package kafkaapp.configuration;

/**
 * Формат сериализации значения сообщений Kafka.
 * В конфиге задаётся строкой "Json" или "Protobuf" (без учёта регистра).
 */
public enum SerializationFormat {
    Json,
    Protobuf;

    public static SerializationFormat fromConfig(String raw) {
        for (SerializationFormat format : values()) {
            if (format.name().equalsIgnoreCase(raw)) {
                return format;
            }
        }
        // Как в .NET-версии: strict-валидация удалена, мусорная строка → дефолт.
        return Json;
    }
}
