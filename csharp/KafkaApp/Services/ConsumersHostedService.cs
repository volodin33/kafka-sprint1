using Confluent.Kafka;
using KafkaApp.Configuration;
using KafkaApp.Messages;
using KafkaApp.Serialization;
using KafkaApp.Services.ConsumerLoops;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaApp.Services;

/// <summary>
/// Единый BackgroundService для всех консюмеров.
/// </summary>
public sealed partial class ConsumersHostedService(
    IReadOnlyList<ConsumerOptions> consumers,
    KafkaOptions kafkaOptions,
    SerializerRegistry serializerRegistry,
    ILogger<ConsumersHostedService> logger)
    : BackgroundService
{
    private readonly SingleConsumerLoop _singleLoop = new(logger);
    private readonly BatchConsumerLoop _batchLoop = new(logger);

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        LogConsumersStarting(consumers.Count);

        var tasks = consumers
            .Select(opts => Task.Run(() => RunConsumeLoop(opts, stoppingToken), stoppingToken))
            .ToList();

        return Task.WhenAll(tasks);
    }

    private void RunConsumeLoop(ConsumerOptions options, CancellationToken stoppingToken)
    {
        logger.LogInformation("Consumer '{Name}' starting. Topic={Topic}, GroupId={GroupId}", options.Name, options.Topic, options.GroupId);

        var consumerConfig = KafkaConfigMapper.ToConsumerConfig(kafkaOptions, options);
        var deserializer = serializerRegistry.Resolve(options.SerializationFormat);

        using var consumer = new ConsumerBuilder<string, OrderMessage>(consumerConfig)
            .SetValueDeserializer(deserializer)
            .SetErrorHandler((_, e) => logger.LogError("Consumer '{Name}' error: {Reason}", options.Name, e.Reason))
            .Build();

        try
        {
            consumer.Subscribe(options.Topic);

            if (options.ReadMode == ReadMode.Single)
                _singleLoop.Run(consumer, options, stoppingToken);
            else
                _batchLoop.Run(consumer, options, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            logger.LogInformation("Consumer '{Name}' cancelled.", options.Name);
        }
        finally
        {
            consumer.Close();
            logger.LogInformation("Consumer '{Name}' stopped.", options.Name);
        }
    }

    [LoggerMessage(LogLevel.Information, "ConsumersHostedService: starting {count} consumer(s) in parallel.")]
    partial void LogConsumersStarting(int count);
}
