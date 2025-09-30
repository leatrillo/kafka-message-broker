using System.Text.Json;
using Coelsa.Artifact.MessageBroker;
using Coelsa.Artifact.MessageBroker.Support.InboxOutbox;
using Coelsa.Artifact.MessageBroker.Support.Handlers; // 👈 para KafkaConsumer
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ConsumerExample;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    ContentRootPath = AppContext.BaseDirectory
});
builder.Configuration.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
builder.Services.AddLogging(l => l.AddConsole());

// NuGet (registra concretos + mapping interfaces)
builder.Services.AddMessageBroker(builder.Configuration);

// Inbox (SQL) en app
builder.Services.AddSingleton<IInboxStore, SqlInboxStore>();

// Decorar consumer ⇒ envolver el CONCRETO
builder.Services.AddSingleton<IMessageConsumer>(sp =>
{
    var inner = sp.GetRequiredService<KafkaConsumer>();
    var store = sp.GetRequiredService<IInboxStore>();
    return new InboxConsumerDecorator(inner, store);
});

var app = builder.Build();
var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Consumer");
var consumer = app.Services.GetRequiredService<IMessageConsumer>();

using var cts = new CancellationTokenSource();

// Consumo simple
_ = consumer.ConsumeAsync<Dictionary<string, object>>(async (evt, key, headers) =>
{
    logger.LogInformation("[CONSUMED] Key={Key} Type={Type} Subject={Subject} Data={Data}",
        key, evt.Type, evt.Subject, JsonSerializer.Serialize(evt.Data));
    await Task.CompletedTask;
}, cts.Token);

// Consumo batch (opcional)
_ = consumer.ConsumeBatchAsync<Dictionary<string, object>>(
    maxBatchSize: 10,
    maxWaitTime: TimeSpan.FromSeconds(5),
    onBatch: async batch =>
    {
        logger.LogInformation("[BATCH] Size={Count}", batch.Count);
        await Task.CompletedTask;
    },
    cancellationToken: cts.Token);

Console.WriteLine("Press ENTER to stop...");
Console.ReadLine();
cts.Cancel();
