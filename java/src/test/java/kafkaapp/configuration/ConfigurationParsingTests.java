package kafkaapp.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Проверяет корректный парсинг секций конфигурации из JSON,
 * дефолты и молчаливую подстановку дефолтов при мусорных enum-строках
 * (strict-валидация удалена, как в .NET-версии; порт ConfigurationParsingTests).
 */
class ConfigurationParsingTests {

    private static AppConfig load(String json) {
        return AppConfigLoader.load(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    }

    // -------------------------------------------------------------------------
    // KafkaOptions
    // -------------------------------------------------------------------------

    @Test
    void kafkaSectionParsesBootstrapServers() {
        AppConfig config = load("""
            {
              "Kafka": {
                "BootstrapServers": "127.0.0.1:9094,127.0.0.1:9095"
              }
            }
            """);

        assertEquals("127.0.0.1:9094,127.0.0.1:9095", config.kafka().bootstrapServers());
    }

    // -------------------------------------------------------------------------
    // ProducerOptions
    // -------------------------------------------------------------------------

    @Test
    void producersSectionParsesSingleProducer() {
        AppConfig config = load("""
            {
              "Producers": [
                {
                  "Name": "p1",
                  "Topic": "orders",
                  "Enabled": true,
                  "SendIntervalMs": 500
                }
              ]
            }
            """);

        assertEquals(1, config.producers().size());
        ProducerOptions p = config.producers().get(0);
        assertEquals("p1", p.name());
        assertEquals("orders", p.topic());
        assertTrue(p.enabled());
        assertEquals(500, p.sendIntervalMs());
    }

    @Test
    void producersSectionDefaultsAcksAllAndThreeRetries() {
        AppConfig config = load("""
            {
              "Producers": [
                { "Name": "p1", "Topic": "t", "Enabled": true, "SendIntervalMs": 1000 }
              ]
            }
            """);

        ProducerOptions p = config.producers().get(0);
        assertEquals("all", p.acks());
        assertEquals(3, p.messageSendMaxRetries());
        assertEquals(false, p.enableIdempotence());
    }

    @Test
    void producersSectionParsesAcksMessageSendMaxRetriesAndIdempotence() {
        AppConfig config = load("""
            {
              "Producers": [
                {
                  "Name": "p1", "Topic": "t", "Enabled": true, "SendIntervalMs": 1000,
                  "Acks": "leader", "MessageSendMaxRetries": 5, "EnableIdempotence": true
                }
              ]
            }
            """);

        ProducerOptions p = config.producers().get(0);
        assertEquals("1", p.acks());
        assertEquals(5, p.messageSendMaxRetries());
        assertEquals(true, p.enableIdempotence());
    }

    @Test
    void producersSectionInvalidAcksDoesNotThrowAtLoadTime() {
        // Fail-fast перенесён в маппер (ParseAcks), загрузчик мусор не отклоняет.
        AppConfig config = load("""
            {
              "Producers": [
                { "Name": "p1", "Topic": "t", "Enabled": true, "Acks": "quorum" }
              ]
            }
            """);

        assertEquals("quorum", config.producers().get(0).acksRaw());
    }

    @Test
    void producersSectionFiltersDisabledProducers() {
        AppConfig config = load("""
            {
              "Producers": [
                { "Name": "enabled-p",  "Topic": "t", "Enabled": true,  "SendIntervalMs": 1000 },
                { "Name": "disabled-p", "Topic": "t", "Enabled": false, "SendIntervalMs": 1000 }
              ]
            }
            """);

        assertEquals(2, config.producers().size());
        assertEquals(1, config.enabledProducers().size());
        assertEquals("enabled-p", config.enabledProducers().get(0).name());
    }

    // -------------------------------------------------------------------------
    // ConsumerOptions
    // -------------------------------------------------------------------------

    @Test
    void consumersSectionParsesTwoConsumers() {
        AppConfig config = load("""
            {
              "Consumers": [
                {
                  "Name": "consumer-1",
                  "Topic": "orders",
                  "GroupId": "orders-group",
                  "Enabled": true,
                  "AutoOffsetReset": "Earliest",
                  "CommitMode": "Manual",
                  "ReadMode": "Batch",
                  "FetchMinBytes": 1,
                  "FetchMaxBytes": 52428800,
                  "FetchWaitMaxMs": 500
                },
                {
                  "Name": "consumer-2",
                  "Topic": "orders",
                  "GroupId": "analytics-group",
                  "Enabled": true,
                  "AutoOffsetReset": "Latest",
                  "CommitMode": "Auto",
                  "ReadMode": "Single",
                  "FetchMinBytes": 1048576,
                  "FetchMaxBytes": 52428800,
                  "FetchWaitMaxMs": 1000
                }
              ]
            }
            """);

        assertEquals(2, config.consumers().size());

        ConsumerOptions c1 = config.consumers().get(0);
        assertEquals("consumer-1", c1.name());
        assertEquals("orders-group", c1.groupId());
        assertEquals("Earliest", c1.autoOffsetResetRaw());
        assertEquals(CommitMode.Manual, c1.commitMode());
        assertEquals(ReadMode.Batch, c1.readMode());
        assertEquals(1, c1.fetchMinBytes());
        assertEquals(52_428_800, c1.fetchMaxBytes());
        assertEquals(500, c1.fetchWaitMaxMs());

        ConsumerOptions c2 = config.consumers().get(1);
        assertEquals("consumer-2", c2.name());
        assertEquals("analytics-group", c2.groupId());
        assertEquals("Latest", c2.autoOffsetResetRaw());
        assertEquals(CommitMode.Auto, c2.commitMode());
        assertEquals(ReadMode.Single, c2.readMode());
        assertEquals(1_048_576, c2.fetchMinBytes());
        assertEquals(1000, c2.fetchWaitMaxMs());
    }

    @Test
    void consumersSectionFiltersDisabledConsumers() {
        AppConfig config = load("""
            {
              "Consumers": [
                { "Name": "c-on",  "Topic": "t", "GroupId": "g", "Enabled": true,  "AutoOffsetReset": "Earliest", "CommitMode": "Manual" },
                { "Name": "c-off", "Topic": "t", "GroupId": "g", "Enabled": false, "AutoOffsetReset": "Earliest", "CommitMode": "Manual" }
              ]
            }
            """);

        assertEquals(2, config.consumers().size());
        assertEquals(1, config.enabledConsumers().size());
        assertEquals("c-on", config.enabledConsumers().get(0).name());
    }

    @Test
    void emptyProducersAndConsumersReturnEmptyLists() {
        AppConfig config = load("{ \"Kafka\": { \"BootstrapServers\": \"localhost:9094\" } }");

        assertTrue(config.producers().isEmpty());
        assertTrue(config.consumers().isEmpty());
    }

    // -------------------------------------------------------------------------
    // CommitMode — дефолт, разные регистры, мусор → дефолт (как в .NET)
    // -------------------------------------------------------------------------

    @Test
    void commitModeDefaultsToManual() {
        AppConfig config = load("""
            {
              "Consumers": [
                { "Name": "c1", "Topic": "t", "GroupId": "g", "Enabled": true, "AutoOffsetReset": "Earliest" }
              ]
            }
            """);

        assertEquals(CommitMode.Manual, config.consumers().get(0).commitMode());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Manual", "manual", "MANUAL", "Auto", "auto", "AUTO"})
    void commitModeBindsFromStringCaseInsensitive(String jsonValue) {
        AppConfig config = load("""
            {
              "Consumers": [
                {
                  "Name": "c1", "Topic": "t", "GroupId": "g", "Enabled": true,
                  "AutoOffsetReset": "Earliest",
                  "CommitMode": "%s"
                }
              ]
            }
            """.formatted(jsonValue));

        CommitMode expected = jsonValue.equalsIgnoreCase("Auto") ? CommitMode.Auto : CommitMode.Manual;
        assertEquals(expected, config.consumers().get(0).commitMode());
    }

    @Test
    void commitModeGarbageStringSilentlyDefaultsToManual() {
        AppConfig config = load("""
            {
              "Consumers": [
                {
                  "Name": "c1", "Topic": "t", "GroupId": "g", "Enabled": true,
                  "AutoOffsetReset": "Earliest",
                  "CommitMode": "sometimes"
                }
              ]
            }
            """);

        assertEquals(CommitMode.Manual, config.consumers().get(0).commitMode());
    }

    // -------------------------------------------------------------------------
    // ReadMode — дефолт Batch, биндинг, мусор → Batch (как в .NET)
    // -------------------------------------------------------------------------

    @Test
    void readModeDefaultsToBatch() {
        AppConfig config = load("""
            {
              "Consumers": [
                { "Name": "c1", "Topic": "t", "GroupId": "g", "Enabled": true, "AutoOffsetReset": "Earliest" }
              ]
            }
            """);

        assertEquals(ReadMode.Batch, config.consumers().get(0).readMode());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Single", "single", "SINGLE", "Batch", "batch", "BATCH"})
    void readModeBindsFromStringCaseInsensitive(String jsonValue) {
        AppConfig config = load("""
            {
              "Consumers": [
                {
                  "Name": "c1", "Topic": "t", "GroupId": "g", "Enabled": true,
                  "AutoOffsetReset": "Earliest",
                  "ReadMode": "%s"
                }
              ]
            }
            """.formatted(jsonValue));

        ReadMode expected = jsonValue.equalsIgnoreCase("Single") ? ReadMode.Single : ReadMode.Batch;
        assertEquals(expected, config.consumers().get(0).readMode());
    }

    @Test
    void readModeGarbageStringSilentlyDefaultsToBatch() {
        AppConfig config = load("""
            {
              "Consumers": [
                {
                  "Name": "c1", "Topic": "t", "GroupId": "g", "Enabled": true,
                  "AutoOffsetReset": "Earliest",
                  "ReadMode": "sideways"
                }
              ]
            }
            """);

        assertEquals(ReadMode.Batch, config.consumers().get(0).readMode());
    }

    // -------------------------------------------------------------------------
    // BatchSize / BatchTimeoutMs — дефолты и биндинг
    // -------------------------------------------------------------------------

    @Test
    void consumerOptionsBatchSizeAbsentInJsonUsesDefault() {
        AppConfig config = load("""
            {
              "Consumers": [
                {
                  "Name": "c1", "Topic": "t", "GroupId": "g", "Enabled": true,
                  "AutoOffsetReset": "Earliest", "CommitMode": "Manual"
                }
              ]
            }
            """);

        assertEquals(10, config.consumers().get(0).batchSize());
    }

    @Test
    void consumerOptionsBatchSizeBindsFromJson() {
        AppConfig config = load("""
            {
              "Consumers": [
                {
                  "Name": "c1", "Topic": "t", "GroupId": "g", "Enabled": true,
                  "AutoOffsetReset": "Earliest", "CommitMode": "Manual",
                  "BatchSize": 25
                }
              ]
            }
            """);

        assertEquals(25, config.consumers().get(0).batchSize());
    }

    @Test
    void consumerOptionsBatchTimeoutMsAbsentInJsonUsesDefault() {
        AppConfig config = load("""
            {
              "Consumers": [
                {
                  "Name": "c1", "Topic": "t", "GroupId": "g", "Enabled": true,
                  "AutoOffsetReset": "Earliest", "CommitMode": "Manual"
                }
              ]
            }
            """);

        assertEquals(5000, config.consumers().get(0).batchTimeoutMs());
    }

    @Test
    void consumerOptionsBatchTimeoutMsBindsFromJson() {
        AppConfig config = load("""
            {
              "Consumers": [
                {
                  "Name": "c1", "Topic": "t", "GroupId": "g", "Enabled": true,
                  "AutoOffsetReset": "Earliest", "CommitMode": "Manual",
                  "BatchTimeoutMs": 2500
                }
              ]
            }
            """);

        assertEquals(2500, config.consumers().get(0).batchTimeoutMs());
    }

    // -------------------------------------------------------------------------
    // SerializationFormat — мусор → дефолт Json (как в .NET)
    // -------------------------------------------------------------------------

    @Test
    void serializationFormatGarbageStringSilentlyDefaultsToJson() {
        AppConfig config = load("""
            {
              "Producers": [
                { "Name": "p1", "Topic": "t", "Enabled": true, "SerializationFormat": "avro-ish" }
              ],
              "Consumers": [
                { "Name": "c1", "Topic": "t", "GroupId": "g", "Enabled": true, "SerializationFormat": "???" }
              ]
            }
            """);

        assertEquals(SerializationFormat.Json, config.producers().get(0).serializationFormat());
        assertEquals(SerializationFormat.Json, config.consumers().get(0).serializationFormat());
    }

    // -------------------------------------------------------------------------
    // Эталонный appsettings.json из ресурсов — контракт по умолчанию
    // -------------------------------------------------------------------------

    @Test
    void bundledAppsettingsMatchesReferenceContract() throws java.io.IOException {
        try (var in = getClass().getClassLoader().getResourceAsStream("appsettings.json")) {
            assertTrue(in != null, "appsettings.json not found in resources");
            AppConfig config = AppConfigLoader.load(in);

            ProducerOptions p = config.enabledProducers().get(0);
            assertEquals("all", p.acks());
            assertEquals(3, p.messageSendMaxRetries());
            assertEquals(false, p.enableIdempotence());

            assertEquals(2, config.enabledConsumers().size());
            ConsumerOptions single = config.consumers().get(0);
            assertEquals("consumer-1", single.name());
            assertEquals("orders-group", single.groupId());
            assertEquals(CommitMode.Auto, single.commitMode());
            assertEquals(ReadMode.Single, single.readMode());

            ConsumerOptions batch = config.consumers().get(1);
            assertEquals("consumer-2", batch.name());
            assertEquals("analytics-group", batch.groupId());
            assertEquals(CommitMode.Manual, batch.commitMode());
            assertEquals(ReadMode.Batch, batch.readMode());
            assertEquals(10, batch.batchSize());
            assertEquals(5000, batch.batchTimeoutMs());
            assertEquals(1, batch.fetchMinBytes());
            assertEquals(52_428_800, batch.fetchMaxBytes());
            assertEquals(500, batch.fetchWaitMaxMs());
        }
    }

    // -------------------------------------------------------------------------
    // Logging
    // -------------------------------------------------------------------------

    @Test
    void loggingSectionParsesLogLevels() {
        AppConfig config = load("""
            {
              "Kafka": { "BootstrapServers": "localhost:9094" },
              "Logging": {
                "LogLevel": {
                  "Default": "Information",
                  "Microsoft.Hosting.Lifetime": "Information"
                }
              }
            }
            """);

        assertEquals("Information", config.logging().levels().get("Default"));
        assertEquals("Information", config.logging().levels().get("Microsoft.Hosting.Lifetime"));
    }
}
