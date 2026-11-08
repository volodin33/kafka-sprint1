package kafkaapp.configuration;

/** Корневая секция "Kafka" конфигурации. */
public record KafkaOptions(String bootstrapServers) {
    public static final KafkaOptions EMPTY = new KafkaOptions("");
}
