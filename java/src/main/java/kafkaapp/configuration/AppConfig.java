package kafkaapp.configuration;

import java.util.List;

/**
 * Корневой объект конфигурации приложения (аналог связки секций appsettings.json).
 */
public record AppConfig(
    KafkaOptions kafka,
    List<ProducerOptions> producers,
    List<ConsumerOptions> consumers,
    LogLevelOptions logging) {

    public List<ProducerOptions> enabledProducers() {
        return producers.stream().filter(ProducerOptions::enabled).toList();
    }

    public List<ConsumerOptions> enabledConsumers() {
        return consumers.stream().filter(ConsumerOptions::enabled).toList();
    }

    /** Секция Logging:LogLevel — имена логгеров → уровень (как в appsettings.json). */
    public record LogLevelOptions(java.util.Map<String, String> levels) {
    }
}
