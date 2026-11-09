namespace KafkaApp.Configuration;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; init; } = string.Empty;
}

public class KafkaEntityOptions
{
    public string Name { get; init; } = string.Empty;
    public string Topic { get; init; } = string.Empty;
    public bool Enabled { get; init; } = true;
}

public sealed class ProducerOptions: KafkaEntityOptions
{
    public int SendIntervalMs { get; init; } = 1000;
    // all/leader/none (регистронезависимо, допускаются -1/1/0). Дефолт all — At Least Once.
    public string Acks { get; set; } = "all";
    public int MessageSendMaxRetries { get; init; } = 3;
    public bool EnableIdempotence { get; set; }
    public SerializationFormat SerializationFormat { get; set; } = SerializationFormat.Json;
}

public sealed class ConsumerOptions: KafkaEntityOptions
{
    public string GroupId { get; set; } = string.Empty;
    public string AutoOffsetReset { get; set; } = "Earliest";
    public CommitMode CommitMode { get; set; } = CommitMode.Manual;
    public int? FetchMinBytes { get; init; }
    public int? FetchMaxBytes { get; init; }
    public int? FetchWaitMaxMs { get; init; }

    public int BatchSize { get; init; } = 10;
    public int BatchTimeoutMs { get; init; } = 5000;
    public ReadMode ReadMode { get; set; } = ReadMode.Batch;
    public SerializationFormat SerializationFormat { get; set; } = SerializationFormat.Json;
}

public enum CommitMode
{
    Manual,
    Auto
}

public enum ReadMode
{
    Single,
    Batch
}

public enum SerializationFormat
{
    Json,
    Protobuf
}