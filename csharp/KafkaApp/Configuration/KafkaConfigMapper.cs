using Confluent.Kafka;

namespace KafkaApp.Configuration;

public static class KafkaConfigMapper
{
    public static ProducerConfig ToProducerConfig(KafkaOptions kafka, ProducerOptions producer) =>
        new()
        {
            BootstrapServers = kafka.BootstrapServers,
            ClientId = producer.Name,
            Acks = ParseAcks(producer.Acks),
            MessageSendMaxRetries = producer.MessageSendMaxRetries,
            EnableIdempotence = producer.EnableIdempotence
        };

    private static Acks ParseAcks(string value) =>
        value.ToLowerInvariant() switch
        {
            "all" or "-1"    => Acks.All,
            "leader" or "1"  => Acks.Leader,
            "none" or "0"    => Acks.None,
            _ => throw new ArgumentException(
                $"Unknown Acks value: '{value}'. Expected: all, leader, none.")
        };

    public static ConsumerConfig ToConsumerConfig(KafkaOptions kafka, ConsumerOptions consumer) =>
        new()
        {
            BootstrapServers = kafka.BootstrapServers,
            GroupId = consumer.GroupId,
            ClientId = consumer.Name,
            EnableAutoCommit = consumer.CommitMode == CommitMode.Auto,
            FetchMinBytes = consumer.FetchMinBytes,
            MaxPartitionFetchBytes = consumer.FetchMaxBytes,
            FetchWaitMaxMs = consumer.FetchWaitMaxMs,
            AutoOffsetReset = consumer.AutoOffsetReset.ToLowerInvariant() switch
            {
                "earliest" => AutoOffsetReset.Earliest,
                "latest"   => AutoOffsetReset.Latest,
                "error"    => AutoOffsetReset.Error,
                _ => throw new ArgumentException($"Unknown AutoOffsetReset value: '{consumer.AutoOffsetReset}'")
            },
        };
}
