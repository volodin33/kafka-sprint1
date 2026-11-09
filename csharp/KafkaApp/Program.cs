using KafkaApp.Extensions;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.AddKafkaServices();

await builder.Build().RunAsync();
