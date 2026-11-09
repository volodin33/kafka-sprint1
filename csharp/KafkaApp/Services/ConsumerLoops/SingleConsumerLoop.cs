using Confluent.Kafka;
using KafkaApp.Configuration;
using KafkaApp.Messages;
using Microsoft.Extensions.Logging;

namespace KafkaApp.Services.ConsumerLoops;

/// <summary>
/// Последовательная обработка сообщений консюмером (ReadMode.Single)
/// </summary>
public sealed class SingleConsumerLoop(ILogger logger)
{
    public void Run(
        IConsumer<string, OrderMessage> consumer,
        ConsumerOptions options,
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(stoppingToken);

                logger.LogInformation(
                    "Consumer '{Name}' received message {Topic}[{Partition}]@{Offset}: Id={Id} Description={Description}",
                    options.Name,
                    consumeResult.Topic,
                    consumeResult.Partition.Value,
                    consumeResult.Offset.Value,
                    consumeResult.Message.Value.Id,
                    consumeResult.Message.Value.Description);

                if (options.CommitMode == CommitMode.Manual)
                {
                    consumer.Commit(consumeResult);
                    logger.LogInformation(
                        "Consumer '{Name}' committed {Topic}[{Partition}]@{Offset}",
                        options.Name,
                        consumeResult.Topic,
                        consumeResult.Partition.Value,
                        consumeResult.Offset.Value);
                }
            }
            catch (ConsumeException ex)
            {
                logger.LogError(ex,
                    "Consumer '{Name}' consume error: {Reason}",
                    options.Name, ex.Error.Reason);
            }
        }
    }
}
