using Confluent.Kafka;
using Google.Protobuf;
using KafkaApp.Configuration;
using KafkaApp.Messages;

namespace KafkaApp.Serialization;

[MessageSerializer(SerializationFormat.Protobuf)]
public sealed class ProtobufOrderMessageSerializer : IOrderMessageSerializer
{
    /// <inheritdoc />
    public byte[] Serialize(OrderMessage data, SerializationContext context)
    {
        using var ms = new MemoryStream();
        var cos = new CodedOutputStream(ms);

        if (data.Id != 0)
        {
            cos.WriteTag(1, WireFormat.WireType.Varint);
            cos.WriteInt32(data.Id);
        }

        if (!string.IsNullOrEmpty(data.Description))
        {
            cos.WriteTag(2, WireFormat.WireType.LengthDelimited);
            cos.WriteString(data.Description);
        }

        cos.Flush();
        return ms.ToArray();
    }

    /// <inheritdoc />
    public OrderMessage Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull || data.IsEmpty)
            return new OrderMessage();

        var message = new OrderMessage();
        var cis = new CodedInputStream(data.ToArray());

        uint tag;
        while ((tag = cis.ReadTag()) != 0)
        {
            switch (WireFormat.GetTagFieldNumber(tag))
            {
                case 1:
                    message.Id = cis.ReadInt32();
                    break;
                case 2:
                    message.Description = cis.ReadString();
                    break;
                default:
                    cis.SkipLastField();
                    break;
            }
        }

        return message;
    }
}
