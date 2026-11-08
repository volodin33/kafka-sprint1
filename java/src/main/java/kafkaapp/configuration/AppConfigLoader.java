package kafkaapp.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Загрузка appsettings.json в {@link AppConfig}.
 *
 * Семантика повторяет .NET-версию: без strict-валидации enum-полей
 * (мусорная строка молча заменяется дефолтом, как биндер .NET);
 * fail-fast остался только в парсерах Acks и AutoOffsetReset (маппер, не загрузчик).
 */
public final class AppConfigLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private AppConfigLoader() {
    }

    public static AppConfig load(Path path) {
        try (InputStream in = Files.newInputStream(path)) {
            return load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read config file: " + path, e);
        }
    }

    public static AppConfig load(InputStream in) {
        JsonNode root;
        try {
            root = MAPPER.readTree(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse config JSON", e);
        }

        KafkaOptions kafka = parseKafka(root.get("Kafka"));

        List<ProducerOptions> producers = new ArrayList<>();
        JsonNode producersNode = root.get("Producers");
        if (producersNode != null && producersNode.isArray()) {
            int i = 0;
            for (JsonNode node : producersNode) {
                producers.add(parseProducer(node, i++));
            }
        }

        List<ConsumerOptions> consumers = new ArrayList<>();
        JsonNode consumersNode = root.get("Consumers");
        if (consumersNode != null && consumersNode.isArray()) {
            int i = 0;
            for (JsonNode node : consumersNode) {
                consumers.add(parseConsumer(node, i++));
            }
        }

        return new AppConfig(kafka, producers, consumers, parseLogging(root.get("Logging")));
    }

    private static KafkaOptions parseKafka(JsonNode node) {
        if (node == null) {
            return KafkaOptions.EMPTY;
        }
        return new KafkaOptions(textOrEmpty(node.get("BootstrapServers")));
    }

    private static ProducerOptions parseProducer(JsonNode node, int index) {
        return new ProducerOptions(
            textOrEmpty(node.get("Name")),
            textOrEmpty(node.get("Topic")),
            boolOrDefault(node.get("Enabled"), true),
            intOrDefault(node.get("SendIntervalMs"), ProducerOptions.DEFAULT_SEND_INTERVAL_MS),
            textOrDefault(node.get("Acks"), ProducerOptions.DEFAULT_ACKS),
            intOrDefault(node.get("MessageSendMaxRetries"), ProducerOptions.DEFAULT_MESSAGE_SEND_MAX_RETRIES),
            boolOrDefault(node.get("EnableIdempotence"), false),
            textOrDefault(node.get("SerializationFormat"), ProducerOptions.DEFAULT_SERIALIZATION_FORMAT));
    }

    private static ConsumerOptions parseConsumer(JsonNode node, int index) {
        return new ConsumerOptions(
            textOrEmpty(node.get("Name")),
            textOrEmpty(node.get("Topic")),
            textOrEmpty(node.get("GroupId")),
            boolOrDefault(node.get("Enabled"), true),
            textOrDefault(node.get("AutoOffsetReset"), ConsumerOptions.DEFAULT_AUTO_OFFSET_RESET),
            textOrDefault(node.get("CommitMode"), ConsumerOptions.DEFAULT_COMMIT_MODE),
            nullableInt(node.get("FetchMinBytes")),
            nullableInt(node.get("FetchMaxBytes")),
            nullableInt(node.get("FetchWaitMaxMs")),
            intOrDefault(node.get("BatchSize"), ConsumerOptions.DEFAULT_BATCH_SIZE),
            intOrDefault(node.get("BatchTimeoutMs"), ConsumerOptions.DEFAULT_BATCH_TIMEOUT_MS),
            textOrDefault(node.get("ReadMode"), ConsumerOptions.DEFAULT_READ_MODE),
            textOrDefault(node.get("SerializationFormat"), ConsumerOptions.DEFAULT_SERIALIZATION_FORMAT));
    }

    private static AppConfig.LogLevelOptions parseLogging(JsonNode loggingNode) {
        Map<String, String> levels = new LinkedHashMap<>();
        JsonNode logLevel = loggingNode != null ? loggingNode.get("LogLevel") : null;
        if (logLevel != null && logLevel.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = logLevel.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                levels.put(field.getKey(), field.getValue().asText());
            }
        }
        return new AppConfig.LogLevelOptions(levels);
    }

    private static String textOrEmpty(JsonNode node) {
        return node != null && !node.isNull() ? node.asText() : "";
    }

    private static String textOrDefault(JsonNode node, String defaultValue) {
        return node != null && !node.isNull() ? node.asText() : defaultValue;
    }

    private static boolean boolOrDefault(JsonNode node, boolean defaultValue) {
        return node != null && !node.isNull() ? node.asBoolean() : defaultValue;
    }

    private static int intOrDefault(JsonNode node, int defaultValue) {
        return node != null && node.isNumber() ? node.intValue() : defaultValue;
    }

    private static Integer nullableInt(JsonNode node) {
        return node != null && node.isNumber() ? node.intValue() : null;
    }
}
