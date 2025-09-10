using Coelsa.Artifact.MessageBroker;
using Coelsa.Artifact.MessageBroker.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);
builder.Configuration.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

builder.Services.AddLogging(x => x.AddConsole());
builder.Services.AddMessageBroker(builder.Configuration);

var app = builder.Build();
var consumer = app.Services.GetRequiredService<IMessageConsumer>();
var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Consumer");

using var cts = new CancellationTokenSource();

await consumer.ConsumeAsync<Dictionary<string, object>>(async (evt, key, headers) =>
{
    logger.LogInformation("[CONSUMED] Key={Key} Type={Type} subject={Subject} data={Data}",
        key, evt.Type, evt.Subject, System.Text.Json.JsonSerializer.Serialize(evt.Data));
    await Task.CompletedTask;
}, cts.Token);

Console.WriteLine("Press ENTER to stop...");
Console.ReadLine();
cts.Cancel();
