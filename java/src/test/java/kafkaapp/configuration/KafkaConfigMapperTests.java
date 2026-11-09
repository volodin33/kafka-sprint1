package kafkaapp.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Порт KafkaConfigMapperTests .NET-версии (маппинг в Properties Kafka-клиента). */
class KafkaConfigMapperTests {

    private static KafkaOptions defaultKafkaOptions() {
        return new KafkaOptions("127.0.0.1:9094,127.0.0.1:9095,127.0.0.1:9096");
    }

    private static ProducerOptions producerOptions() {
        return new ProducerOptions("p1", "t", true, 1000, "all", 3, false, "Json");
    }

    private static ConsumerOptions consumerOptions() {
        return new ConsumerOptions("c1", "t", "g", true, "Earliest", "Manual",
            null, null, null, 10, 5000, "Batch", "Json");
    }

    // -------------------------------------------------------------------------
    // Producer mapping
    // -------------------------------------------------------------------------

    @Test
    void toProducerPropertiesSetsBootstrapServers() {
        Properties config = KafkaConfigMapper.toProducerProperties(defaultKafkaOptions(), producerOptions());

        assertEquals("127.0.0.1:9094,127.0.0.1:9095,127.0.0.1:9096",
            config.getProperty(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    void toProducerPropertiesSetsClientIdFromName() {
        ProducerOptions producer = new ProducerOptions("my-producer", "t", true, 1000,
            "all", 3, false, "Json");

        Properties config = KafkaConfigMapper.toProducerProperties(defaultKafkaOptions(), producer);

        assertEquals("my-producer",
            config.getProperty(org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG));
    }

    @ParameterizedTest
    @CsvSource({
        "all, all",
        "All, all",
        "ALL, all",
        "-1, all",
        "leader, 1",
        "Leader, 1",
        "1, 1",
        "none, 0",
        "None, 0",
        "0, 0"
    })
    void toProducerPropertiesParsesAcks(String input, String expected) {
        ProducerOptions producer = new ProducerOptions("p1", "t", true, 1000,
            input, 3, false, "Json");

        Properties config = KafkaConfigMapper.toProducerProperties(defaultKafkaOptions(), producer);

        assertEquals(expected,
            config.getProperty(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG));
    }

    @Test
    void toProducerPropertiesUnknownAcksThrows() {
        ProducerOptions producer = new ProducerOptions("p1", "t", true, 1000,
            "quorum", 3, false, "Json");

        assertThrows(IllegalArgumentException.class,
            () -> KafkaConfigMapper.toProducerProperties(defaultKafkaOptions(), producer));
    }

    @Test
    void toProducerPropertiesDefaultProducerOptionsUseAcksAll() {
        ProducerOptions producer = new ProducerOptions("p1", "t", true, 1000,
            ProducerOptions.DEFAULT_ACKS, ProducerOptions.DEFAULT_MESSAGE_SEND_MAX_RETRIES, false, "Json");

        Properties config = KafkaConfigMapper.toProducerProperties(defaultKafkaOptions(), producer);

        assertEquals("all",
            config.getProperty(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG));
    }

    @Test
    void toProducerPropertiesMapsMessageSendMaxRetries() {
        ProducerOptions producer = new ProducerOptions("p1", "t", true, 1000,
            "all", 7, false, "Json");

        Properties config = KafkaConfigMapper.toProducerProperties(defaultKafkaOptions(), producer);

        assertEquals("7",
            config.getProperty(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void toProducerPropertiesMapsEnableIdempotence(boolean enabled) {
        ProducerOptions producer = new ProducerOptions("p1", "t", true, 1000,
            "all", 3, enabled, "Json");

        Properties config = KafkaConfigMapper.toProducerProperties(defaultKafkaOptions(), producer);

        assertEquals(Boolean.toString(enabled),
            config.getProperty(org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    }

    // -------------------------------------------------------------------------
    // Consumer mapping — AutoOffsetReset
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({
        "Earliest, earliest",
        "earliest, earliest",
        "Latest, latest",
        "latest, latest",
        "Error, none",
        "error, none"
    })
    void toConsumerPropertiesParsesAutoOffsetReset(String input, String expected) {
        ConsumerOptions consumer = new ConsumerOptions("c1", "t", "g", true, input, "Manual",
            null, null, null, 10, 5000, "Batch", "Json");

        Properties config = KafkaConfigMapper.toConsumerProperties(defaultKafkaOptions(), consumer);

        assertEquals(expected,
            config.getProperty(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }

    @Test
    void toConsumerPropertiesUnknownAutoOffsetResetThrows() {
        ConsumerOptions consumer = new ConsumerOptions("c1", "t", "g", true, "unknown", "Manual",
            null, null, null, 10, 5000, "Batch", "Json");

        assertThrows(IllegalArgumentException.class,
            () -> KafkaConfigMapper.toConsumerProperties(defaultKafkaOptions(), consumer));
    }

    // -------------------------------------------------------------------------
    // Consumer mapping — CommitMode
    // -------------------------------------------------------------------------

    @Test
    void toConsumerPropertiesCommitModeAutoEnablesAutoCommit() {
        ConsumerOptions consumer = new ConsumerOptions("c1", "t", "g", true, "Earliest", "Auto",
            null, null, null, 10, 5000, "Batch", "Json");

        Properties config = KafkaConfigMapper.toConsumerProperties(defaultKafkaOptions(), consumer);

        assertEquals("true",
            config.getProperty(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    void toConsumerPropertiesCommitModeManualDisablesAutoCommit() {
        Properties config = KafkaConfigMapper.toConsumerProperties(defaultKafkaOptions(), consumerOptions());

        assertEquals("false",
            config.getProperty(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    void toConsumerPropertiesUnknownCommitModeSilentlyDefaultsToManual() {
        // Strict-валидация удалена (как в .NET): мусорный CommitMode → Manual.
        ConsumerOptions consumer = new ConsumerOptions("c1", "t", "g", true, "Earliest", "sometimes",
            null, null, null, 10, 5000, "Batch", "Json");

        Properties config = KafkaConfigMapper.toConsumerProperties(defaultKafkaOptions(), consumer);

        assertEquals("false",
            config.getProperty(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    // -------------------------------------------------------------------------
    // Consumer mapping — fetch параметры
    // -------------------------------------------------------------------------

    @Test
    void toConsumerPropertiesMapsFetchParameters() {
        ConsumerOptions consumer = new ConsumerOptions("c1", "t", "g", true, "Earliest", "Manual",
            1024, 10_000_000, 300, 10, 5000, "Batch", "Json");

        Properties config = KafkaConfigMapper.toConsumerProperties(defaultKafkaOptions(), consumer);

        assertEquals("1024",
            config.getProperty(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
        // FetchMaxBytes → max.partition.fetch.bytes (как MaxPartitionFetchBytes в .NET).
        assertEquals("10000000",
            config.getProperty(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG));
        assertEquals("300",
            config.getProperty(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG));
    }

    @Test
    void toConsumerPropertiesOmitsFetchParametersWhenAbsent() {
        // fetch.min.bytes маппится только из явного FetchMinBytes (без деривации из BatchSize).
        Properties config = KafkaConfigMapper.toConsumerProperties(defaultKafkaOptions(), consumerOptions());

        ConsumerOptions batch25 = new ConsumerOptions("c1", "t", "g", true, "Earliest", "Manual",
            null, null, null, 25, 5000, "Batch", "Json");
        Properties config25 = KafkaConfigMapper.toConsumerProperties(defaultKafkaOptions(), batch25);

        assertFalse(config.containsKey(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
        assertFalse(config25.containsKey(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
        assertFalse(config.containsKey(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG));
        assertFalse(config.containsKey(org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG));
    }

    // -------------------------------------------------------------------------
    // Идентификаторы
    // -------------------------------------------------------------------------

    @Test
    void toConsumerPropertiesSetsGroupIdAndClientId() {
        ConsumerOptions consumer = new ConsumerOptions("consumer-X", "t", "group-X", true,
            "Latest", "Auto", null, null, null, 10, 5000, "Batch", "Json");

        Properties config = KafkaConfigMapper.toConsumerProperties(defaultKafkaOptions(), consumer);

        assertEquals("group-X",
            config.getProperty(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("consumer-X",
            config.getProperty(org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG));
    }

    @Test
    void toConsumerPropertiesSetsBootstrapServers() {
        KafkaOptions kafka = new KafkaOptions("host1:9094,host2:9095");

        Properties config = KafkaConfigMapper.toConsumerProperties(kafka, consumerOptions());

        assertEquals("host1:9094,host2:9095",
            config.getProperty(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }
}
