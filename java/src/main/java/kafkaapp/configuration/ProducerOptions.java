package kafkaapp.configuration;

/**
 * Опции одного продюсера (элемент секции "Producers").
 */
public record ProducerOptions(
    String name,
    String topic,
    boolean enabled,
    int sendIntervalMs,
    /** Гарантия доставки: all/leader/none (регистронезависимо, допускаются -1/1/0) — см. {@link #acks()}. */
    String acksRaw,
    /** Число повторных попыток отправки (message.send.max.retries). */
    int messageSendMaxRetries,
    /** Идемпотентность продюсера (enable.idempotence). */
    boolean enableIdempotence,
    String serializationFormatRaw) {

    public static final int DEFAULT_SEND_INTERVAL_MS = 1000;
    public static final String DEFAULT_ACKS = "all";
    public static final int DEFAULT_MESSAGE_SEND_MAX_RETRIES = 3;
    public static final String DEFAULT_SERIALIZATION_FORMAT = "Json";

    public SerializationFormat serializationFormat() {
        return SerializationFormat.fromConfig(serializationFormatRaw);
    }

    /** Маппинг Acks в значение параметра acks Java Kafka-клиента. Fail-fast на мусоре. */
    public String acks() {
        return switch (acksRaw.toLowerCase(java.util.Locale.ROOT)) {
            case "all", "-1" -> "all";
            case "leader", "1" -> "1";
            case "none", "0" -> "0";
            default -> throw new IllegalArgumentException(
                "Unknown Acks value: '" + acksRaw + "'. Expected: all, leader, none.");
        };
    }
}
