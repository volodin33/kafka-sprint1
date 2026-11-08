package kafkaapp.serialization;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import kafkaapp.configuration.AppConfig;
import kafkaapp.configuration.AppConfigLoader;
import kafkaapp.configuration.CommitMode;
import kafkaapp.configuration.SerializationFormat;
import kafkaapp.messages.OrderMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Тесты сериализации OrderMessage (round-trip JSON и Protobuf),
 * реестра SerializerRegistry, дефолтов и strict-валидации.
 * Включает контрольные векторы, проверяющие совместимость с .NET-форматами.
 */
class SerializationTests {

    private static final String TOPIC = "test-topic";

    private static OrderMessage makeOrder(int id, String description) {
        return new OrderMessage(id, description);
    }

    private static AppConfig loadConfig(String json) {
        return AppConfigLoader.load(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    }

    // -------------------------------------------------------------------------
    // Round-trip: JSON
    // -------------------------------------------------------------------------

    @Test
    void jsonSerializerRoundTripPreservesAllFields() {
        OrderMessageSerde serde = new JsonOrderMessageSerializer();
        OrderMessage original = makeOrder(42, "Test order description");

        byte[] bytes = serde.serialize(TOPIC, original);
        OrderMessage restored = serde.deserialize(TOPIC, bytes);

        assertEquals(original.id(), restored.id());
        assertEquals(original.description(), restored.description());
    }

    @Test
    void jsonSerializerProducesCamelCaseWebFormat() {
        OrderMessageSerde serde = new JsonOrderMessageSerializer();

        byte[] bytes = serde.serialize(TOPIC, makeOrder(42, "Test order description"));

        // Формат System.Text.Json (JsonSerializerDefaults.Web) из .NET-версии.
        assertEquals("{\"id\":42,\"description\":\"Test order description\"}",
            new String(bytes, StandardCharsets.UTF_8));
    }

    @Test
    void jsonSerializerRoundTripZeroIdPreservesFields() {
        OrderMessageSerde serde = new JsonOrderMessageSerializer();
        OrderMessage original = makeOrder(0, "Zero-id order");

        byte[] bytes = serde.serialize(TOPIC, original);
        OrderMessage restored = serde.deserialize(TOPIC, bytes);

        assertEquals(0, restored.id());
        assertEquals("Zero-id order", restored.description());
    }

    @Test
    void jsonSerializerDeserializeNullReturnsDefault() {
        OrderMessageSerde serde = new JsonOrderMessageSerializer();

        OrderMessage result = serde.deserialize(TOPIC, null);

        assertNotNull(result);
        assertEquals(0, result.id());
        assertEquals("", result.description());
    }

    @Test
    void jsonSerializerDeserializeEmptyThrows() {
        // Пустой payload — не валидный JSON: не маскируем битое сообщение (как в .NET).
        OrderMessageSerde serde = new JsonOrderMessageSerializer();

        RuntimeException ex = assertThrows(RuntimeException.class,
            () -> serde.deserialize(TOPIC, new byte[0]));

        assertTrue(ex.getMessage().contains("Empty payload"));
    }

    @Test
    void jsonSerializerReadsFieldNamesCaseInsensitive() {
        OrderMessageSerde serde = new JsonOrderMessageSerializer();
        byte[] pascalCase = "{\"Id\":7,\"Description\":\"case\"}".getBytes(StandardCharsets.UTF_8);

        OrderMessage restored = serde.deserialize(TOPIC, pascalCase);

        assertEquals(7, restored.id());
        assertEquals("case", restored.description());
    }

    // -------------------------------------------------------------------------
    // Round-trip: Protobuf
    // -------------------------------------------------------------------------

    @Test
    void protobufSerializerRoundTripPreservesAllFields() {
        OrderMessageSerde serde = new ProtobufOrderMessageSerializer();
        OrderMessage original = makeOrder(99, "Protobuf order description");

        byte[] bytes = serde.serialize(TOPIC, original);
        OrderMessage restored = serde.deserialize(TOPIC, bytes);

        assertEquals(original.id(), restored.id());
        assertEquals(original.description(), restored.description());
    }

    @Test
    void protobufSerializerProducesWireCompatibleBytes() {
        OrderMessageSerde serde = new ProtobufOrderMessageSerializer();

        byte[] bytes = serde.serialize(TOPIC, makeOrder(42, "Test order description"));

        // Ручной разбор wire-формата: tag 8 (field 1, varint) + 42,
        // tag 18 (field 2, length-delimited) + 22 байта строки.
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        expected.write(0x08);
        expected.write(42);
        expected.write(0x12);
        expected.write(22);
        expected.writeBytes("Test order description".getBytes(StandardCharsets.UTF_8));

        assertArrayEquals(expected.toByteArray(), bytes);
    }

    @Test
    void protobufSerializerRoundTripZeroIdPreservesDescription() {
        OrderMessageSerde serde = new ProtobufOrderMessageSerializer();
        OrderMessage original = makeOrder(0, "Zero-id protobuf");

        byte[] bytes = serde.serialize(TOPIC, original);
        OrderMessage restored = serde.deserialize(TOPIC, bytes);

        // id=0 — default value in protobuf, не кодируется; Description должно сохраниться.
        assertEquals("Zero-id protobuf", restored.description());
    }

    @Test
    void protobufSerializerDeserializeNullReturnsDefault() {
        OrderMessageSerde serde = new ProtobufOrderMessageSerializer();

        OrderMessage result = serde.deserialize(TOPIC, null);

        assertNotNull(result);
        assertEquals(0, result.id());
        assertEquals("", result.description());
    }

    @Test
    void protobufSerializerDeserializeEmptyReturnsDefault() {
        OrderMessageSerde serde = new ProtobufOrderMessageSerializer();

        OrderMessage result = serde.deserialize(TOPIC, new byte[0]);

        assertNotNull(result);
        assertEquals(0, result.id());
        assertEquals("", result.description());
    }

    @Test
    void protobufSerializerSerializeProducesNonEmptyBytes() {
        OrderMessageSerde serde = new ProtobufOrderMessageSerializer();

        byte[] bytes = serde.serialize(TOPIC, makeOrder(1, "test"));

        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
    }

    // -------------------------------------------------------------------------
    // SerializerRegistry
    // -------------------------------------------------------------------------

    @Test
    void serializerRegistryContainsBothFormats() {
        SerializerRegistry registry = SerializerRegistry.withDefaults();

        assertNotNull(registry.resolve(SerializationFormat.Json));
        assertNotNull(registry.resolve(SerializationFormat.Protobuf));
    }

    @Test
    void serializerRegistryResolveJsonReturnsJsonSerializer() {
        SerializerRegistry registry = SerializerRegistry.withDefaults();

        assertTrue(registry.resolve(SerializationFormat.Json) instanceof JsonOrderMessageSerializer);
    }

    @Test
    void serializerRegistryResolveProtobufReturnsProtobufSerializer() {
        SerializerRegistry registry = SerializerRegistry.withDefaults();

        assertTrue(registry.resolve(SerializationFormat.Protobuf) instanceof ProtobufOrderMessageSerializer);
    }

    @Test
    void serializerRegistryResolveUnknownFormatThrows() {
        // Реестр без Protobuf-сериализатора — аналог несуществующего значения enum в .NET.
        SerializerRegistry registry = new SerializerRegistry(
            Map.of(SerializationFormat.Json, new JsonOrderMessageSerializer()));

        IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> registry.resolve(SerializationFormat.Protobuf));

        assertTrue(ex.getMessage().contains("Protobuf"));
        assertTrue(ex.getMessage().contains("Json"));
    }

    @Test
    void serializerRegistryEmptyThrows() {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
            () -> new SerializerRegistry(Map.of()));

        assertTrue(ex.getMessage().contains("No serializers registered"));
    }

    // -------------------------------------------------------------------------
    // Дефолты SerializationFormat (отсутствие поля в JSON)
    // -------------------------------------------------------------------------

    @Test
    void producerOptionsSerializationFormatAbsentInJsonUsesJsonDefault() {
        AppConfig config = loadConfig("""
            {
              "Producers": [
                { "Name": "p1", "Topic": "orders", "Enabled": true }
              ]
            }
            """);

        assertEquals(SerializationFormat.Json, config.producers().get(0).serializationFormat());
    }

    @Test
    void consumerOptionsSerializationFormatAbsentInJsonUsesJsonDefault() {
        AppConfig config = loadConfig("""
            {
              "Consumers": [
                {
                  "Name": "c1", "Topic": "t", "GroupId": "g",
                  "Enabled": true, "AutoOffsetReset": "Earliest", "CommitMode": "Manual"
                }
              ]
            }
            """);

        assertEquals(SerializationFormat.Json, config.consumers().get(0).serializationFormat());
    }

    // -------------------------------------------------------------------------
    // Биндинг SerializationFormat из конфигурации (без учёта регистра)
    // -------------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {"Json", "json", "JSON", "Protobuf", "protobuf", "PROTOBUF"})
    void producerOptionsSerializationFormatBindsFromStringCaseInsensitive(String jsonValue) {
        AppConfig config = loadConfig("""
            {
              "Producers": [
                {
                  "Name": "p1", "Topic": "orders", "Enabled": true,
                  "SerializationFormat": "%s"
                }
              ]
            }
            """.formatted(jsonValue));

        SerializationFormat expected =
            jsonValue.equalsIgnoreCase("Protobuf") ? SerializationFormat.Protobuf : SerializationFormat.Json;
        assertEquals(expected, config.producers().get(0).serializationFormat());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Json", "json", "JSON", "Protobuf", "protobuf", "PROTOBUF"})
    void consumerOptionsSerializationFormatBindsFromStringCaseInsensitive(String jsonValue) {
        AppConfig config = loadConfig("""
            {
              "Consumers": [
                {
                  "Name": "c1", "Topic": "t", "GroupId": "g", "Enabled": true,
                  "AutoOffsetReset": "Earliest", "CommitMode": "Manual",
                  "SerializationFormat": "%s"
                }
              ]
            }
            """.formatted(jsonValue));

        SerializationFormat expected =
            jsonValue.equalsIgnoreCase("Protobuf") ? SerializationFormat.Protobuf : SerializationFormat.Json;
        assertEquals(expected, config.consumers().get(0).serializationFormat());
    }
}
