using KafkaApp.Configuration;
using Microsoft.Extensions.Configuration;

namespace KafkaApp.Tests.Configuration;

/// <summary>
/// Проверяет корректный парсинг секций конфигурации из JSON
/// и фильтрацию по полю Enabled.
/// </summary>
public class ConfigurationParsingTests
{
    private static IConfiguration BuildConfiguration(string json)
    {
        // Запись JSON во временный поток и загрузка через ConfigurationBuilder.
        var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
        return new ConfigurationBuilder()
            .AddJsonStream(stream)
            .Build();
    }

    // -------------------------------------------------------------------------
    // KafkaOptions
    // -------------------------------------------------------------------------

    [Fact]
    public void KafkaSection_ParsesBootstrapServers()
    {
        const string json = """
            {
              "Kafka": {
                "BootstrapServers": "127.0.0.1:9094,127.0.0.1:9095"
              }
            }
            """;

        var cfg = BuildConfiguration(json);
        var opts = cfg.GetSection("Kafka").Get<KafkaOptions>();

        Assert.NotNull(opts);
        Assert.Equal("127.0.0.1:9094,127.0.0.1:9095", opts.BootstrapServers);
    }

    // -------------------------------------------------------------------------
    // ProducerOptions
    // -------------------------------------------------------------------------

    [Fact]
    public void ProducersSection_ParsesSingleProducer()
    {
        const string json = """
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
            """;

        var cfg = BuildConfiguration(json);
        var list = cfg.GetSection("Producers").Get<List<ProducerOptions>>();

        Assert.NotNull(list);
        Assert.Single(list);
        Assert.Equal("p1", list[0].Name);
        Assert.Equal("orders", list[0].Topic);
        Assert.True(list[0].Enabled);
        Assert.Equal(500, list[0].SendIntervalMs);
    }

    [Fact]
    public void ProducersSection_FiltersDisabledProducers()
    {
        const string json = """
            {
              "Producers": [
                { "Name": "enabled-p",  "Topic": "t", "Enabled": true,  "SendIntervalMs": 1000 },
                { "Name": "disabled-p", "Topic": "t", "Enabled": false, "SendIntervalMs": 1000 }
              ]
            }
            """;

        var cfg = BuildConfiguration(json);
        var all = cfg.GetSection("Producers").Get<List<ProducerOptions>>() ?? [];
        var enabled = all.Where(p => p.Enabled).ToList();

        Assert.Equal(2, all.Count);
        Assert.Single(enabled);
        Assert.Equal("enabled-p", enabled[0].Name);
    }

    // -------------------------------------------------------------------------
    // ConsumerOptions
    // -------------------------------------------------------------------------

    [Fact]
    public void ConsumersSection_ParsesTwoConsumers()
    {
        const string json = """
            {
              "Consumers": [
                {
                  "Name": "consumer-1",
                  "Topic": "orders",
                  "GroupId": "orders-group",
                  "Enabled": true,
                  "AutoOffsetReset": "Earliest",
                  "CommitMode": "Manual",
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
                  "FetchMinBytes": 1048576,
                  "FetchMaxBytes": 52428800,
                  "FetchWaitMaxMs": 1000
                }
              ]
            }
            """;

        var cfg = BuildConfiguration(json);
        var list = cfg.GetSection("Consumers").Get<List<ConsumerOptions>>();

        Assert.NotNull(list);
        Assert.Equal(2, list.Count);

        var c1 = list[0];
        Assert.Equal("consumer-1", c1.Name);
        Assert.Equal("orders-group", c1.GroupId);
        Assert.Equal("Earliest", c1.AutoOffsetReset);
        Assert.Equal(CommitMode.Manual, c1.CommitMode);
        Assert.Equal(1, c1.FetchMinBytes);
        Assert.Equal(52_428_800, c1.FetchMaxBytes);
        Assert.Equal(500, c1.FetchWaitMaxMs);

        var c2 = list[1];
        Assert.Equal("consumer-2", c2.Name);
        Assert.Equal("analytics-group", c2.GroupId);
        Assert.Equal("Latest", c2.AutoOffsetReset);
        Assert.Equal(CommitMode.Auto, c2.CommitMode);
        Assert.Equal(1_048_576, c2.FetchMinBytes);
        Assert.Equal(1000, c2.FetchWaitMaxMs);
    }

    [Fact]
    public void ConsumersSection_FiltersDisabledConsumers()
    {
        const string json = """
            {
              "Consumers": [
                { "Name": "c-on",  "Topic": "t", "GroupId": "g", "Enabled": true,  "AutoOffsetReset": "Earliest", "CommitMode": "Manual" },
                { "Name": "c-off", "Topic": "t", "GroupId": "g", "Enabled": false, "AutoOffsetReset": "Earliest", "CommitMode": "Manual" }
              ]
            }
            """;

        var cfg = BuildConfiguration(json);
        var all = cfg.GetSection("Consumers").Get<List<ConsumerOptions>>() ?? [];
        var enabled = all.Where(c => c.Enabled).ToList();

        Assert.Equal(2, all.Count);
        Assert.Single(enabled);
        Assert.Equal("c-on", enabled[0].Name);
    }

    [Fact]
    public void EmptyProducersAndConsumers_ReturnEmptyLists()
    {
        const string json = """{ "Kafka": { "BootstrapServers": "localhost:9094" } }""";

        var cfg = BuildConfiguration(json);
        var producers = cfg.GetSection("Producers").Get<List<ProducerOptions>>() ?? [];
        var consumers = cfg.GetSection("Consumers").Get<List<ConsumerOptions>>() ?? [];

        Assert.Empty(producers);
        Assert.Empty(consumers);
    }

    // -------------------------------------------------------------------------
    // CommitMode enum binding
    // -------------------------------------------------------------------------

    [Theory]
    [InlineData("Manual", CommitMode.Manual)]
    [InlineData("manual", CommitMode.Manual)]
    [InlineData("MANUAL", CommitMode.Manual)]
    [InlineData("Auto", CommitMode.Auto)]
    [InlineData("auto", CommitMode.Auto)]
    [InlineData("AUTO", CommitMode.Auto)]
    public void CommitMode_BindsFromStringCaseInsensitive(string jsonValue, CommitMode expected)
    {
        // Microsoft.Extensions.Configuration биндит enum по имени без учёта регистра из коробки.
        var json = $$"""
            {
              "Consumers": [
                {
                  "Name": "c1",
                  "Topic": "t",
                  "GroupId": "g",
                  "Enabled": true,
                  "AutoOffsetReset": "Earliest",
                  "CommitMode": "{{jsonValue}}"
                }
              ]
            }
            """;

        var cfg = BuildConfiguration(json);
        var list = cfg.GetSection("Consumers").Get<List<ConsumerOptions>>();

        Assert.NotNull(list);
        Assert.Single(list);
        Assert.Equal(expected, list[0].CommitMode);
    }

    // -------------------------------------------------------------------------
    // BatchSize / BatchTimeoutMs — дефолты и биндинг
    // -------------------------------------------------------------------------

    [Fact]
    public void ConsumerOptions_BatchSize_DefaultIs10()
    {
        var opts = new ConsumerOptions();
        Assert.Equal(10, opts.BatchSize);
    }

    [Fact]
    public void ConsumerOptions_BatchTimeoutMs_DefaultIs5000()
    {
        var opts = new ConsumerOptions();
        Assert.Equal(5000, opts.BatchTimeoutMs);
    }

    [Fact]
    public void ConsumerOptions_BatchParams_BindFromJson()
    {
        const string json = """
            {
              "Consumers": [
                {
                  "Name": "c1",
                  "Topic": "t",
                  "GroupId": "g",
                  "Enabled": true,
                  "AutoOffsetReset": "Earliest",
                  "CommitMode": "Manual",
                  "BatchSize": 25,
                  "BatchTimeoutMs": 3000
                }
              ]
            }
            """;

        var cfg = BuildConfiguration(json);
        var list = cfg.GetSection("Consumers").Get<List<ConsumerOptions>>();

        Assert.NotNull(list);
        Assert.Single(list);
        Assert.Equal(25, list[0].BatchSize);
        Assert.Equal(3000, list[0].BatchTimeoutMs);
    }

    [Fact]
    public void ConsumerOptions_BatchParams_AbsentInJson_UseDefaults()
    {
        // Если BatchSize/BatchTimeoutMs не заданы в JSON — должны применяться дефолты.
        const string json = """
            {
              "Consumers": [
                {
                  "Name": "c1",
                  "Topic": "t",
                  "GroupId": "g",
                  "Enabled": true,
                  "AutoOffsetReset": "Earliest",
                  "CommitMode": "Manual"
                }
              ]
            }
            """;

        var cfg = BuildConfiguration(json);
        var list = cfg.GetSection("Consumers").Get<List<ConsumerOptions>>();

        Assert.NotNull(list);
        Assert.Single(list);
        Assert.Equal(10, list[0].BatchSize);
        Assert.Equal(5000, list[0].BatchTimeoutMs);
    }

    // -------------------------------------------------------------------------
    // ReadMode enum binding
    // -------------------------------------------------------------------------

    [Fact]
    public void ConsumerOptions_ReadMode_DefaultIsBatch()
    {
        var opts = new ConsumerOptions();
        Assert.Equal(ReadMode.Batch, opts.ReadMode);
    }

    [Theory]
    [InlineData("Single", ReadMode.Single)]
    [InlineData("single", ReadMode.Single)]
    [InlineData("SINGLE", ReadMode.Single)]
    [InlineData("Batch", ReadMode.Batch)]
    [InlineData("batch", ReadMode.Batch)]
    [InlineData("BATCH", ReadMode.Batch)]
    public void ReadMode_BindsFromStringCaseInsensitive(string jsonValue, ReadMode expected)
    {
        // Microsoft.Extensions.Configuration биндит enum по имени без учёта регистра из коробки.
        var json = $$"""
            {
              "Consumers": [
                {
                  "Name": "c1",
                  "Topic": "t",
                  "GroupId": "g",
                  "Enabled": true,
                  "AutoOffsetReset": "Earliest",
                  "CommitMode": "Manual",
                  "ReadMode": "{{jsonValue}}"
                }
              ]
            }
            """;

        var cfg = BuildConfiguration(json);
        var list = cfg.GetSection("Consumers").Get<List<ConsumerOptions>>();

        Assert.NotNull(list);
        Assert.Single(list);
        Assert.Equal(expected, list[0].ReadMode);
    }

    [Fact]
    public void ConsumerOptions_ReadMode_AbsentInJson_UsesBatchDefault()
    {
        // Если ReadMode не задан в JSON — должен применяться дефолт Batch.
        const string json = """
            {
              "Consumers": [
                {
                  "Name": "c1",
                  "Topic": "t",
                  "GroupId": "g",
                  "Enabled": true,
                  "AutoOffsetReset": "Earliest",
                  "CommitMode": "Manual"
                }
              ]
            }
            """;

        var cfg = BuildConfiguration(json);
        var list = cfg.GetSection("Consumers").Get<List<ConsumerOptions>>();

        Assert.NotNull(list);
        Assert.Single(list);
        Assert.Equal(ReadMode.Batch, list[0].ReadMode);
    }
}
