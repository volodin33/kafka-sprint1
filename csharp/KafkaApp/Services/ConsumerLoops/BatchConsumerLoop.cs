using Confluent.Kafka;
using KafkaApp.Configuration;
using KafkaApp.Messages;
using Microsoft.Extensions.Logging;

namespace KafkaApp.Services.ConsumerLoops;

/// <summary>
/// Пакетная обработка сообщений консюмером (ReadMode.Batch).
/// </summary>
public sealed class BatchConsumerLoop(ILogger logger)
{
    public void Run(
        IConsumer<string, OrderMessage> consumer,
        ConsumerOptions options,
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var batch = CollectBatch(consumer, options, stoppingToken);
            if (batch.Count > 0)
                FlushBatch(consumer, options, batch);
        }
    }

    private List<ConsumeResult<string, OrderMessage>> CollectBatch(
        IConsumer<string, OrderMessage> consumer,
        ConsumerOptions options,
        CancellationToken stoppingToken)
    {
        var batch = new List<ConsumeResult<string, OrderMessage>>(options.BatchSize);

        try
        {
            batch.Add(consumer.Consume(stoppingToken));
            var deadline = DateTime.UtcNow.AddMilliseconds(options.BatchTimeoutMs);

            while (batch.Count < options.BatchSize && !stoppingToken.IsCancellationRequested)
            {
                var remaining = deadline - DateTime.UtcNow;
                if (remaining <= TimeSpan.Zero)
                    break;

                var result = consumer.Consume(remaining);
                if (result is null)
                    break;

                batch.Add(result);
            }
        }
        catch (ConsumeException ex)
        {
            logger.LogError(ex, "Consumer '{Name}' consume error: {Reason}", options.Name, ex.Error.Reason);
        }

        return batch;
    }

    private void FlushBatch(
        IConsumer<string, OrderMessage> consumer,
        ConsumerOptions options,
        List<ConsumeResult<string, OrderMessage>> batch)
    {
        foreach (var r in batch)
            logger.LogDebug(
                "Consumer '{Name}' received {Topic}[{Partition}]@{Offset}: Id={Id} Description={Description}",
                options.Name, r.Topic, r.Partition.Value, r.Offset.Value, r.Message.Value.Id, r.Message.Value.Description);

        if (options.CommitMode == CommitMode.Manual)
        {
            consumer.Commit();
            logger.LogInformation("Consumer '{Name}' committed batch size: {Count}", options.Name, batch.Count);
        }
    }
}
