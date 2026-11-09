using Confluent.Kafka;
using KafkaApp.Configuration;
using KafkaApp.Messages;
using KafkaApp.Serialization;
using Microsoft.Extensions.Configuration;

namespace KafkaApp.Tests.Serialization;

/// <summary>
/// Тесты сериализации OrderMessage (round-trip JSON и Protobuf),
/// реестра SerializerRegistry (reflection-скан, Resolve, ошибки),
/// дефолтов SerializationFormat в ProducerOptions/ConsumerOptions
/// и биндинга из конфига.
/// </summary>
public class SerializationTests
{
    // -------------------------------------------------------------------------
    // Вспомогательные объекты
    // -------------------------------------------------------------------------

    private static readonly SerializationContext AnyContext =
        new(MessageComponentType.Value, "test-topic");

    private static OrderMessage MakeOrder(int id = 42, string desc = "Hello Kafka") =>
        new() { Id = id, Description = desc };

    // -------------------------------------------------------------------------
    // Round-trip: JSON
    // -------------------------------------------------------------------------

    [Fact]
    public void JsonSerializer_RoundTrip_PreservesAllFields()
    {
        var serializer = new JsonOrderMessageSerializer();
        var original = MakeOrder(42, "Test order description");

        var bytes = serializer.Serialize(original, AnyContext);
        var restored = serializer.Deserialize(bytes, false, AnyContext);

        Assert.Equal(original.Id, restored.Id);
        Assert.Equal(original.Description, restored.Description);
    }

    [Fact]
    public void JsonSerializer_RoundTrip_ZeroId_PreservesFields()
    {
        var serializer = new JsonOrderMessageSerializer();
        var original = MakeOrder(0, "Zero-id order");

        var bytes = serializer.Serialize(original, AnyContext);
        var restored = serializer.Deserialize(bytes, false, AnyContext);

        Assert.Equal(0, restored.Id);
        Assert.Equal("Zero-id order", restored.Description);
    }

    [Fact]
    public void JsonSerializer_Deserialize_Null_ReturnsDefault()
    {
        var serializer = new JsonOrderMessageSerializer();
        var result = serializer.Deserialize(default, true, AnyContext);

        Assert.NotNull(result);
        Assert.Equal(0, result.Id);
        Assert.Equal(string.Empty, result.Description);
    }

    [Fact]
    public void JsonSerializer_Deserialize_EmptyPayload_Throws()
    {
        // Пустой payload — не валидный JSON: не маскируем битое сообщение дефолтом.
        var serializer = new JsonOrderMessageSerializer();

        Assert.Throws<InvalidDataException>(() =>
            serializer.Deserialize(ReadOnlySpan<byte>.Empty, false, AnyContext));
    }

    // -------------------------------------------------------------------------
    // Round-trip: Protobuf
    // -------------------------------------------------------------------------

    [Fact]
    public void ProtobufSerializer_RoundTrip_PreservesAllFields()
    {
        var serializer = new ProtobufOrderMessageSerializer();
        var original = MakeOrder(99, "Protobuf order description");

        var bytes = serializer.Serialize(original, AnyContext);
        var restored = serializer.Deserialize(bytes, false, AnyContext);

        Assert.Equal(original.Id, restored.Id);
        Assert.Equal(original.Description, restored.Description);
    }

    [Fact]
    public void ProtobufSerializer_RoundTrip_ZeroId_PreservesDescription()
    {
        var serializer = new ProtobufOrderMessageSerializer();
        var original = MakeOrder(0, "Zero-id protobuf");

        var bytes = serializer.Serialize(original, AnyContext);
        var restored = serializer.Deserialize(bytes, false, AnyContext);

        // id=0 — default value in protobuf, may not be encoded; Description должно сохраниться
        Assert.Equal("Zero-id protobuf", restored.Description);
    }

    [Fact]
    public void ProtobufSerializer_Deserialize_Null_ReturnsDefault()
    {
        var serializer = new ProtobufOrderMessageSerializer();
        var result = serializer.Deserialize(default, true, AnyContext);

        Assert.NotNull(result);
        Assert.Equal(0, result.Id);
        Assert.Equal(string.Empty, result.Description);
    }

    [Fact]
    public void ProtobufSerializer_Deserialize_EmptyPayload_ReturnsDefault()
    {
        // Пустой payload — легальный protobuf (все поля по умолчанию).
        var serializer = new ProtobufOrderMessageSerializer();
        var result = serializer.Deserialize(ReadOnlySpan<byte>.Empty, false, AnyContext);

        Assert.NotNull(result);
        Assert.Equal(0, result.Id);
        Assert.Equal(string.Empty, result.Description);
    }

    [Fact]
    public void ProtobufSerializer_Serialize_ProducesNonEmptyBytes()
    {
        var serializer = new ProtobufOrderMessageSerializer();
        var bytes = serializer.Serialize(MakeOrder(1, "test"), AnyContext);

        Assert.NotNull(bytes);
        Assert.NotEmpty(bytes);
    }

    // -------------------------------------------------------------------------
    // SerializerRegistry: reflection-скан и Resolve
    // -------------------------------------------------------------------------

    [Fact]
    public void SerializerRegistry_ContainsBothFormats()
    {
        var registry = new SerializerRegistry();

        // Оба формата должны быть зарегистрированы после скана
        var json = registry.Resolve(SerializationFormat.Json);
        var proto = registry.Resolve(SerializationFormat.Protobuf);

        Assert.NotNull(json);
        Assert.NotNull(proto);
    }

    [Fact]
    public void SerializerRegistry_Resolve_Json_ReturnsJsonSerializer()
    {
        var registry = new SerializerRegistry();
        var serializer = registry.Resolve(SerializationFormat.Json);

        Assert.IsType<JsonOrderMessageSerializer>(serializer);
    }

    [Fact]
    public void SerializerRegistry_Resolve_Protobuf_ReturnsProtobufSerializer()
    {
        var registry = new SerializerRegistry();
        var serializer = registry.Resolve(SerializationFormat.Protobuf);

        Assert.IsType<ProtobufOrderMessageSerializer>(serializer);
    }

    [Fact]
    public void SerializerRegistry_Resolve_Json_HasCorrectAttribute()
    {
        var registry = new SerializerRegistry();
        var serializer = registry.Resolve(SerializationFormat.Json);

        var attr = serializer.GetType()
            .GetCustomAttributes(typeof(MessageSerializerAttribute), false)
            .Cast<MessageSerializerAttribute>()
            .FirstOrDefault();

        Assert.NotNull(attr);
        Assert.Equal(SerializationFormat.Json, attr.Format);
    }

    [Fact]
    public void SerializerRegistry_Resolve_Protobuf_HasCorrectAttribute()
    {
        var registry = new SerializerRegistry();
        var serializer = registry.Resolve(SerializationFormat.Protobuf);

        var attr = serializer.GetType()
            .GetCustomAttributes(typeof(MessageSerializerAttribute), false)
            .Cast<MessageSerializerAttribute>()
            .FirstOrDefault();

        Assert.NotNull(attr);
        Assert.Equal(SerializationFormat.Protobuf, attr.Format);
    }

    [Fact]
    public void SerializerRegistry_Resolve_UnknownFormat_ThrowsInvalidOperation()
    {
        // Используем значение enum, которого нет в реестре.
        // Кастуем int в enum, чтобы получить несуществующее значение.
        var registry = new SerializerRegistry();
        var unknownFormat = (SerializationFormat)999;

        var ex = Assert.Throws<InvalidOperationException>(() => registry.Resolve(unknownFormat));
        Assert.Contains("999", ex.Message);
    }

    // -------------------------------------------------------------------------
    // Дефолты SerializationFormat в options
    // -------------------------------------------------------------------------

    [Fact]
    public void ProducerOptions_SerializationFormat_DefaultIsJson()
    {
        var opts = new ProducerOptions();
        Assert.Equal(SerializationFormat.Json, opts.SerializationFormat);
    }

    [Fact]
    public void ConsumerOptions_SerializationFormat_DefaultIsJson()
    {
        var opts = new ConsumerOptions();
        Assert.Equal(SerializationFormat.Json, opts.SerializationFormat);
    }

    // -------------------------------------------------------------------------
    // Биндинг SerializationFormat из конфигурации
    // -------------------------------------------------------------------------

    private static IConfiguration BuildConfiguration(string json)
    {
        var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        return new ConfigurationBuilder()
            .AddJsonStream(stream)
            .Build();
    }

    [Theory]
    [InlineData("Json", SerializationFormat.Json)]
    [InlineData("json", SerializationFormat.Json)]
    [InlineData("JSON", SerializationFormat.Json)]
    [InlineData("Protobuf", SerializationFormat.Protobuf)]
    [InlineData("protobuf", SerializationFormat.Protobuf)]
    [InlineData("PROTOBUF", SerializationFormat.Protobuf)]
    public void ProducerOptions_SerializationFormat_BindsFromStringCaseInsensitive(
        string jsonValue, SerializationFormat expected)
    {
        var json = $$"""
            {
              "Producers": [
                {
                  "Name": "p1",
                  "Topic": "orders",
                  "Enabled": true,
                  "SerializationFormat": "{{jsonValue}}"
                }
              ]
            }
            """;

        var cfg = BuildConfiguration(json);
        var list = cfg.GetSection("Producers").Get<List<ProducerOptions>>();

        Assert.NotNull(list);
        Assert.Single(list);
        Assert.Equal(expected, list[0].SerializationFormat);
    }

    [Theory]
    [InlineData("Json", SerializationFormat.Json)]
    [InlineData("json", SerializationFormat.Json)]
    [InlineData("JSON", SerializationFormat.Json)]
    [InlineData("Protobuf", SerializationFormat.Protobuf)]
    [InlineData("protobuf", SerializationFormat.Protobuf)]
    [InlineData("PROTOBUF", SerializationFormat.Protobuf)]
    public void ConsumerOptions_SerializationFormat_BindsFromStringCaseInsensitive(
        string jsonValue, SerializationFormat expected)
    {
        var json = $$"""
            {
              "Consumers": [
                {
                  "Name": "c1",
                  "Topic": "orders",
                  "GroupId": "g",
                  "Enabled": true,
                  "AutoOffsetReset": "Earliest",
                  "CommitMode": "Manual",
                  "SerializationFormat": "{{jsonValue}}"
                }
              ]
            }
            """;

        var cfg = BuildConfiguration(json);
        var list = cfg.GetSection("Consumers").Get<List<ConsumerOptions>>();

        Assert.NotNull(list);
        Assert.Single(list);
        Assert.Equal(expected, list[0].SerializationFormat);
    }

    [Fact]
    public void ProducerOptions_SerializationFormat_AbsentInJson_UsesJsonDefault()
    {
        const string json = """
            {
              "Producers": [
                { "Name": "p1", "Topic": "orders", "Enabled": true }
              ]
            }
            """;

        var cfg = BuildConfiguration(json);
        var list = cfg.GetSection("Producers").Get<List<ProducerOptions>>();

        Assert.NotNull(list);
        Assert.Single(list);
        Assert.Equal(SerializationFormat.Json, list[0].SerializationFormat);
    }

    [Fact]
    public void ConsumerOptions_SerializationFormat_AbsentInJson_UsesJsonDefault()
    {
        const string json = """
            {
              "Consumers": [
                {
                  "Name": "c1", "Topic": "orders", "GroupId": "g",
                  "Enabled": true, "AutoOffsetReset": "Earliest", "CommitMode": "Manual"
                }
              ]
            }
            """;

        var cfg = BuildConfiguration(json);
        var list = cfg.GetSection("Consumers").Get<List<ConsumerOptions>>();

        Assert.NotNull(list);
        Assert.Single(list);
        Assert.Equal(SerializationFormat.Json, list[0].SerializationFormat);
    }
}
