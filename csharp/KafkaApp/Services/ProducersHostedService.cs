using Confluent.Kafka;
using KafkaApp.Configuration;
using KafkaApp.Messages;
using KafkaApp.Serialization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaApp.Services;

/// <summary>
/// Единый BackgroundService для всех продюсеров.
/// Запускает по одной задаче на каждый включённый (Enabled=true) продюсер параллельно.
/// Все задачи объединяются через Task.WhenAll и завершаются по stoppingToken.
/// </summary>
public sealed partial class ProducersHostedService(
    IReadOnlyList<ProducerOptions> producers,
    KafkaOptions kafkaOptions,
    SerializerRegistry serializerRegistry,
    ILogger<ProducersHostedService> logger)
    : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        LogProducershostedserviceStartingCountProducerSInParallel(producers.Count);

        var tasks = producers
            .Select(opts => Task.Run(() => RunProducerAsync(opts, stoppingToken), stoppingToken))
            .ToList();

        return Task.WhenAll(tasks);
    }

    private async Task RunProducerAsync(ProducerOptions options, CancellationToken stoppingToken)
    {
        var producerConfig = KafkaConfigMapper.ToProducerConfig(kafkaOptions, options);
        var serializer = serializerRegistry.Resolve(options.SerializationFormat);

        logger.LogInformation(
            "Producer '{Name}' starting. Topic={Topic}, SerializationFormat={Format}",
            options.Name, options.Topic, options.SerializationFormat);

        using var producer = new ProducerBuilder<string, OrderMessage>(producerConfig)
            .SetValueSerializer(serializer)
            .Build();

        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(options.SendIntervalMs));

        var counter = 0;
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                counter++;
                await SendMessageAsync(producer, options, counter, stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            producer.Flush(TimeSpan.FromSeconds(5));
            logger.LogInformation("Producer '{Name}' stopped.", options.Name);
        }
    }

    private async Task SendMessageAsync(
        IProducer<string, OrderMessage> producer,
        ProducerOptions options,
        int counter,
        CancellationToken ct)
    {
        var key = Guid.NewGuid().ToString("N");
        var value = new OrderMessage
        {
            Id = counter,
            Description = $"[{options.Name}] Test message  @ {DateTimeOffset.UtcNow:O}"
        };

        try
        {
            var result = await producer.ProduceAsync(
                options.Topic,
                new Message<string, OrderMessage> { Key = key, Value = value },
                ct);

            logger.LogInformation(
                "Producer '{Name}' delivered Id={Id} Description={Description} to {Topic}[{Partition}]@{Offset} Key={Key}",
                options.Name, value.Id, value.Description, result.Topic, result.Partition.Value, result.Offset.Value, key);
        }
        catch (ProduceException<string, OrderMessage> ex)
        {
            logger.LogError(ex, "Producer '{Name}' failed to send message. Error={Error}", options.Name, ex.Error.Reason);
        }
    }

    [LoggerMessage(LogLevel.Information, "ProducersHostedService: starting {Count} producer(s) in parallel.")]
    partial void LogProducershostedserviceStartingCountProducerSInParallel(int count);
}
