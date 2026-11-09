using Confluent.Kafka;
using KafkaApp.Messages;

namespace KafkaApp.Serialization;

public interface IOrderMessageSerializer : ISerializer<OrderMessage>, IDeserializer<OrderMessage>
{
}
