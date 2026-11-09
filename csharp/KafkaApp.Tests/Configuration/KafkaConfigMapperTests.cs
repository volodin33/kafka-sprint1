using Confluent.Kafka;
using KafkaApp.Configuration;

namespace KafkaApp.Tests.Configuration;

public class KafkaConfigMapperTests
{
    private static KafkaOptions DefaultKafkaOptions() => new()
    {
        BootstrapServers = "127.0.0.1:9094,127.0.0.1:9095,127.0.0.1:9096"
    };

    // -------------------------------------------------------------------------
    // ProducerConfig mapping
    // -------------------------------------------------------------------------

    [Fact]
    public void ToProducerConfig_SetsBootstrapServers()
    {
        var kafka = DefaultKafkaOptions();
        var producer = new ProducerOptions { Name = "p1", Topic = "t", Enabled = true, SendIntervalMs = 500 };

        var config = KafkaConfigMapper.ToProducerConfig(kafka, producer);

        Assert.Equal(kafka.BootstrapServers, config.BootstrapServers);
    }

    [Fact]
    public void ToProducerConfig_SetsClientIdFromName()
    {
        var kafka = DefaultKafkaOptions();
        var producer = new ProducerOptions { Name = "my-producer", Topic = "t", Enabled = true };

        var config = KafkaConfigMapper.ToProducerConfig(kafka, producer);

        Assert.Equal("my-producer", config.ClientId);
    }

    [Fact]
    public void ToProducerConfig_SetsAtLeastOnceDelivery()
    {
        // Гарантия At Least Once: acks=all (все ISR-реплики) + явные retries.
        var producer = new ProducerOptions { Name = "p1", Topic = "t", Enabled = true, MessageSendMaxRetries = 7 };

        var config = KafkaConfigMapper.ToProducerConfig(DefaultKafkaOptions(), producer);

        Assert.Equal(Acks.All, config.Acks);
        Assert.Equal(7, config.MessageSendMaxRetries);
    }

    [Theory]
    [InlineData("all", Acks.All)]
    [InlineData("All", Acks.All)]
    [InlineData("-1", Acks.All)]
    [InlineData("leader", Acks.Leader)]
    [InlineData("1", Acks.Leader)]
    [InlineData("none", Acks.None)]
    [InlineData("0", Acks.None)]
    public void ToProducerConfig_ParsesAcks(string input, Acks expected)
    {
        var producer = new ProducerOptions { Name = "p1", Topic = "t", Enabled = true, Acks = input };

        var config = KafkaConfigMapper.ToProducerConfig(DefaultKafkaOptions(), producer);

        Assert.Equal(expected, config.Acks);
    }

    [Fact]
    public void ToProducerConfig_UnknownAcks_Throws()
    {
        var producer = new ProducerOptions { Name = "p1", Topic = "t", Enabled = true, Acks = "quorum" };

        Assert.Throws<ArgumentException>(() =>
            KafkaConfigMapper.ToProducerConfig(DefaultKafkaOptions(), producer));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ToProducerConfig_MapsEnableIdempotence(bool enabled)
    {
        var producer = new ProducerOptions { Name = "p1", Topic = "t", Enabled = true, EnableIdempotence = enabled };

        var config = KafkaConfigMapper.ToProducerConfig(DefaultKafkaOptions(), producer);

        Assert.Equal(enabled, config.EnableIdempotence);
    }

    // -------------------------------------------------------------------------
    // ConsumerConfig mapping — AutoOffsetReset
    // -------------------------------------------------------------------------

    [Theory]
    [InlineData("Earliest", AutoOffsetReset.Earliest)]
    [InlineData("earliest", AutoOffsetReset.Earliest)]
    [InlineData("Latest", AutoOffsetReset.Latest)]
    [InlineData("latest", AutoOffsetReset.Latest)]
    [InlineData("Error", AutoOffsetReset.Error)]
    [InlineData("error", AutoOffsetReset.Error)]
    public void ToConsumerConfig_ParsesAutoOffsetReset(string input, AutoOffsetReset expected)
    {
        var consumer = new ConsumerOptions
        {
            Name = "c1", Topic = "t", GroupId = "g",
            AutoOffsetReset = input, CommitMode = CommitMode.Manual
        };

        var config = KafkaConfigMapper.ToConsumerConfig(DefaultKafkaOptions(), consumer);

        Assert.Equal(expected, config.AutoOffsetReset);
    }

    [Fact]
    public void ToConsumerConfig_UnknownAutoOffsetReset_Throws()
    {
        var consumer = new ConsumerOptions
        {
            Name = "c1", Topic = "t", GroupId = "g",
            AutoOffsetReset = "unknown", CommitMode = CommitMode.Manual
        };

        Assert.Throws<ArgumentException>(() =>
            KafkaConfigMapper.ToConsumerConfig(DefaultKafkaOptions(), consumer));
    }

    // -------------------------------------------------------------------------
    // ConsumerConfig mapping — CommitMode (enum)
    // -------------------------------------------------------------------------

    [Fact]
    public void ToConsumerConfig_CommitModeAuto_EnablesAutoCommit()
    {
        var consumer = new ConsumerOptions
        {
            Name = "c1", Topic = "t", GroupId = "g",
            AutoOffsetReset = "Earliest", CommitMode = CommitMode.Auto
        };

        var config = KafkaConfigMapper.ToConsumerConfig(DefaultKafkaOptions(), consumer);

        Assert.True(config.EnableAutoCommit);
    }

    [Fact]
    public void ToConsumerConfig_CommitModeManual_DisablesAutoCommit()
    {
        var consumer = new ConsumerOptions
        {
            Name = "c1", Topic = "t", GroupId = "g",
            AutoOffsetReset = "Earliest", CommitMode = CommitMode.Manual
        };

        var config = KafkaConfigMapper.ToConsumerConfig(DefaultKafkaOptions(), consumer);

        Assert.False(config.EnableAutoCommit);
    }

    // -------------------------------------------------------------------------
    // ConsumerConfig mapping — fetch parameters
    // -------------------------------------------------------------------------

    [Fact]
    public void ToConsumerConfig_MapsFetchParameters()
    {
        var consumer = new ConsumerOptions
        {
            Name = "c1", Topic = "t", GroupId = "g",
            AutoOffsetReset = "Earliest", CommitMode = CommitMode.Manual,
            FetchMinBytes = 1024,
            FetchMaxBytes = 10_000_000,
            FetchWaitMaxMs = 300
        };

        var config = KafkaConfigMapper.ToConsumerConfig(DefaultKafkaOptions(), consumer);

        Assert.Equal(1024, config.FetchMinBytes);
        Assert.Equal(10_000_000, config.MaxPartitionFetchBytes);
        Assert.Equal(300, config.FetchWaitMaxMs);
    }

    [Fact]
    public void ToConsumerConfig_SetsGroupIdAndClientId()
    {
        var consumer = new ConsumerOptions
        {
            Name = "consumer-X", Topic = "t", GroupId = "group-X",
            AutoOffsetReset = "Latest", CommitMode = CommitMode.Auto
        };

        var config = KafkaConfigMapper.ToConsumerConfig(DefaultKafkaOptions(), consumer);

        Assert.Equal("group-X", config.GroupId);
        Assert.Equal("consumer-X", config.ClientId);
    }

    [Fact]
    public void ToConsumerConfig_SetsBootstrapServers()
    {
        var kafka = new KafkaOptions { BootstrapServers = "host1:9094,host2:9095" };
        var consumer = new ConsumerOptions
        {
            Name = "c1", Topic = "t", GroupId = "g",
            AutoOffsetReset = "Earliest", CommitMode = CommitMode.Manual
        };

        var config = KafkaConfigMapper.ToConsumerConfig(kafka, consumer);

        Assert.Equal("host1:9094,host2:9095", config.BootstrapServers);
    }
}
