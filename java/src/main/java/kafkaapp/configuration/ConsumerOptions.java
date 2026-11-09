package kafkaapp.configuration;

/**
 * Опции одного консюмера (элемент секции "Consumers").
 */
public record ConsumerOptions(
    String name,
    String topic,
    String groupId,
    boolean enabled,
    String autoOffsetResetRaw,
    String commitModeRaw,
    Integer fetchMinBytes,
    Integer fetchMaxBytes,
    Integer fetchWaitMaxMs,
    /** Целевой размер пачки для ReadMode.Batch. */
    int batchSize,
    /** Дедлайн добора пачки с момента первого сообщения, мс (ReadMode.Batch). */
    int batchTimeoutMs,
    /** Режим чтения: Single/Batch — см. {@link ReadMode}. */
    String readModeRaw,
    String serializationFormatRaw) {

    public static final String DEFAULT_AUTO_OFFSET_RESET = "Earliest";
    public static final String DEFAULT_COMMIT_MODE = "Manual";
    public static final int DEFAULT_BATCH_SIZE = 10;
    public static final int DEFAULT_BATCH_TIMEOUT_MS = 5000;
    public static final String DEFAULT_READ_MODE = "Batch";
    public static final String DEFAULT_SERIALIZATION_FORMAT = "Json";

    public CommitMode commitMode() {
        return CommitMode.fromConfig(commitModeRaw);
    }

    public ReadMode readMode() {
        return ReadMode.fromConfig(readModeRaw);
    }

    public SerializationFormat serializationFormat() {
        return SerializationFormat.fromConfig(serializationFormatRaw);
    }
}
