using Coelsa.Artifact.MessageBroker;
using Coelsa.Artifact.MessageBroker.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);
builder.Configuration.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

builder.Services.AddLogging(x => x.AddConsole());
builder.Services.AddMessageBroker(builder.Configuration); // reads Kafka + Avro options

var app = builder.Build();
var publisher = app.Services.GetRequiredService<IMessagePublisher>();
var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Producer");

for (int i = 1; i <= 3; i++)
{
    var data = new
    {
        invoiceId = Guid.NewGuid().ToString(),
        customerId = "12345",
        totalAmount = 150.75 + i,
        currency = "USD"
    };

    var evt = CloudEventMessage<object>.Create(
        data: data,
        type: "invoice.created",
        source: "urn:coelsa.com.ar/billing/invoice",
        subject: $"invoiceId:{data.invoiceId}"
    );

    await publisher.PublishAsync(evt, key: data.invoiceId);
    logger.LogInformation("[PUBLISHED] {InvoiceId}", data.invoiceId);
}

