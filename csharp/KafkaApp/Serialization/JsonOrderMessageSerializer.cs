using System.Text.Json;
using Confluent.Kafka;
using KafkaApp.Configuration;
using KafkaApp.Messages;

namespace KafkaApp.Serialization;

[MessageSerializer(SerializationFormat.Json)]
public sealed class JsonOrderMessageSerializer : IOrderMessageSerializer
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);

    /// <inheritdoc />
    public byte[] Serialize(OrderMessage data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes(data, Options);

    /// <inheritdoc />
    public OrderMessage Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
            return new OrderMessage();

        if (data.IsEmpty)
            throw new InvalidDataException("Empty payload is not a valid JSON message.");

        return JsonSerializer.Deserialize<OrderMessage>(data, Options) ?? new OrderMessage();
    }
}
