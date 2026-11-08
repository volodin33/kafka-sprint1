using KafkaApp.Configuration;
using KafkaApp.Serialization;
using KafkaApp.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaApp.Extensions;

public static class KafkaHostingExtensions
{
    public static IHostApplicationBuilder AddKafkaServices(this IHostApplicationBuilder builder)
    {
        var configuration = builder.Configuration;

        var kafkaOptions = configuration.GetSection("Kafka").Get<KafkaOptions>()
            ?? new KafkaOptions();

        builder.Services.AddSingleton<SerializerRegistry>();
        return builder
            .InitProducers(kafkaOptions)
            .InitConsumers(kafkaOptions);
    }

    private static IHostApplicationBuilder InitProducers(this IHostApplicationBuilder builder, KafkaOptions kafkaOptions)
    {
        var producers = (builder.Configuration.GetSection("Producers").Get<List<ProducerOptions>>() ?? [])
            .Where(p => p.Enabled)
            .ToList();

        if (producers.Count > 0)
        {
            builder.Services.AddSingleton<IHostedService>(sp =>
                new ProducersHostedService(
                    producers,
                    kafkaOptions,
                    sp.GetRequiredService<SerializerRegistry>(),
                    sp.GetRequiredService<ILogger<ProducersHostedService>>()));
        }

        return builder;
    }

    private static IHostApplicationBuilder InitConsumers(this IHostApplicationBuilder builder, KafkaOptions kafkaOptions)
    {
        var consumers = (builder.Configuration.GetSection("Consumers").Get<List<ConsumerOptions>>() ?? [])
            .Where(p => p.Enabled)
            .ToList();

        if (consumers.Count > 0)
        {
            builder.Services.AddSingleton<IHostedService>(sp =>
                new ConsumersHostedService(
                    consumers,
                    kafkaOptions,
                    sp.GetRequiredService<SerializerRegistry>(),
                    sp.GetRequiredService<ILogger<ConsumersHostedService>>()));
        }

        return builder;
    }
}
