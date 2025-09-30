using Coelsa.Artifact.MessageBroker;
using Coelsa.Artifact.MessageBroker.Models;
using Coelsa.Artifact.MessageBroker.Support.InboxOutbox;
using Coelsa.Artifact.MessageBroker.Support.Handlers; // 👈 para KafkaPublisher
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using ProducerExample;

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    ContentRootPath = AppContext.BaseDirectory
});
builder.Configuration.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
builder.Services.AddLogging(l => l.AddConsole());

// NuGet (registra concretos + mapping interfaces)
builder.Services.AddMessageBroker(builder.Configuration);

// Outbox (SQL) en app
builder.Services.AddSingleton<IOutboxStore, SqlOutboxStore>();

// Decorar publisher ⇒ envolver el CONCRETO
builder.Services.AddSingleton<IMessagePublisher>(sp =>
{
    var inner = sp.GetRequiredService<KafkaPublisher>(); // ✅ concreto, NO la interfaz
    var store = sp.GetRequiredService<IOutboxStore>();
    var cfg = sp.GetRequiredService<MessageBrokerSettings>();
    return new OutboxPublisher(inner, store, cfg);
});

var app = builder.Build();
var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Producer");
var publisher = app.Services.GetRequiredService<IMessagePublisher>();

// Demo batch + single
var events = new List<CloudEventMessage<object>>();
for (int i = 1; i <= 3; i++)
{
    var data = new { invoiceId = Guid.NewGuid().ToString(), customerId = "12345", totalAmount = 150.75 + i, currency = "USD" };
    var evt = CloudEventMessage<object>.Create(
        data: data,
        type: "invoice.created",
        source: "urn:coelsa.com.ar/billing/invoice",
        subject: $"invoiceId:{data.invoiceId}"
    );
    events.Add(evt);
}

await publisher.PublishBatchAsync(events, e => (e.Data as dynamic).invoiceId);
await publisher.PublishAsync(events[0], key: (events[0].Data as dynamic).invoiceId);

logger.LogInformation("Published {Count} messages", events.Count);
