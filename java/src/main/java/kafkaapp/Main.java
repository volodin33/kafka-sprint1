package kafkaapp;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import kafkaapp.configuration.AppConfig;
import kafkaapp.configuration.AppConfigLoader;
import kafkaapp.configuration.KafkaOptions;
import kafkaapp.serialization.SerializerRegistry;
import kafkaapp.services.ConsumersService;
import kafkaapp.services.ProducersService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Точка входа: аналог Program.cs .NET-версии (Generic Host заменён собственным
 * жизненным циклом: сервисы стартуют в потоках-демонах, shutdown hook
 * корректно останавливает консюмеров и продюсеров по SIGINT/SIGTERM).
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private Main() {
    }

    public static void main(String[] args) {
        AppConfig config;
        try {
            config = applyEnvOverrides(loadConfig());
        } catch (IllegalArgumentException e) {
            // Fail-fast парсеры конфигурации (Acks, AutoOffsetReset) — как в .NET.
            log.error("Configuration error: {}", e.getMessage());
            System.exit(1);
            return;
        }

        applyLogLevels(config);

        SerializerRegistry registry = SerializerRegistry.withDefaults();

        var producers = new ProducersService(config.enabledProducers(), config.kafka(), registry);
        var consumers = new ConsumersService(config.enabledConsumers(), config.kafka(), registry);

        consumers.start();
        producers.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Порядок остановки: сначала консюмеры (commit/Close), затем продюсеры (Flush).
            consumers.close();
            producers.close();
        }, "shutdown-hook"));

        // Основной поток припаркован; завершение — по сигналу (Ctrl+C / SIGTERM).
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Сначала внешний файл (рабочий каталог), иначе ресурс внутри jar — как в .NET. */
    private static AppConfig loadConfig() {
        Path external = Path.of("appsettings.json");
        if (Files.exists(external)) {
            return AppConfigLoader.load(external);
        }
        try (InputStream in = Main.class.getClassLoader().getResourceAsStream("appsettings.json")) {
            if (in == null) {
                throw new IllegalArgumentException("appsettings.json not found");
            }
            return AppConfigLoader.load(in);
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("Failed to read appsettings.json: " + e.getMessage());
        }
    }

    /**
     * Env-override аналогично Kafka__BootstrapServers в .NET (конфигурация
     * контейнера через переменные окружения): KAFKA_BOOTSTRAP_SERVERS перекрывает
     * Kafka:BootstrapServers из appsettings.json.
     */
    private static AppConfig applyEnvOverrides(AppConfig config) {
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            return config;
        }
        return new AppConfig(
            new KafkaOptions(bootstrapServers),
            config.producers(),
            config.consumers(),
            config.logging());
    }

    /** Применяет Logging:LogLevel из конфигурации к корневому логгеру Logback. */
    private static void applyLogLevels(AppConfig config) {
        String defaultLevel = config.logging().levels().get("Default");
        if (defaultLevel == null) {
            return;
        }
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger rootLogger = context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(toLogbackLevel(defaultLevel));
    }

    private static Level toLogbackLevel(String dotNetLevel) {
        return switch (dotNetLevel) {
            case "Trace" -> Level.TRACE;
            case "Debug" -> Level.DEBUG;
            case "Information" -> Level.INFO;
            case "Warning" -> Level.WARN;
            case "Error" -> Level.ERROR;
            case "Critical" -> Level.ERROR;
            case "None" -> Level.OFF;
            default -> Level.INFO;
        };
    }
}
