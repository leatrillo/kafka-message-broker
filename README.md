
# Coelsa.Artifact.MessageBroker (CloudEvents + Avro + Schema Registry)

NuGet for .NET 8 to **publish** and **consume** Kafka messages with a **CloudEvents** envelope.
It reads **everything from configuration** (Topic included).  
- **JSON** by default.
- **AVRO** when `Kafka.SchemaRegistryUrl` is set **and** `Kafka.Avro` is present.

## Config (single object)
```json
{
  "Kafka": {
    "BootstrapServers": "localhost:9094",
    "GroupId": "my-app",
    "Topic": "demo.messages",
    "EnableAutoCommit": true,
    "SchemaRegistryUrl": "http://localhost:8081",
    "Avro": {
      "SubjectPrefix": "coelsa",
      "SchemaMap": {
        "invoice.created": "schemas/invoice.created.avsc"
      }
    }
  }
}
```

## Docker (included)
`./docker/docker-compose.yml` spins up **Kafka (9094)** + **Schema Registry (8081)**

```bash
docker compose -f ./docker/docker-compose.yml up -d

# Create topic inside kafka container
docker ps -a   # find the kafka container name (ends with -kafka-1)
docker exec -it <kafka-container> bash
kafka-topics --create --topic demo.messages --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --list --bootstrap-server localhost:9092
```

## How it works
- CloudEvents stays as JSON envelope.
- If Avro is configured, `data` is serialized to **binary Avro** and placed as **`data_base64`** in the CloudEvent. Consumer detects it and deserializes using Schema Registry.

## Register services
```csharp
builder.Services.AddMessageBroker(builder.Configuration); // reads Kafka + Avro options
```

## Publish
```csharp
var evt = CloudEventMessage<object>.Create(
  data: new { invoiceId = "1", customerId = "123", totalAmount = 150.75, currency = "USD" },
  type: "invoice.created",
  source: "urn:coelsa.com.ar/billing/invoice"
);
await publisher.PublishAsync(evt, key: "1");
```

## Consume
```csharp
await consumer.StartAsync<Dictionary<string, object>>(async (evt, key, headers) =>
{
  Console.WriteLine($"[{evt.Type}] {evt.Subject} -> {System.Text.Json.JsonSerializer.Serialize(evt.Data)}");
  await Task.CompletedTask;
}, ct);
```

## Examples
Two console apps:

- `example/ProducerExample` - publishes 3 `invoice.created` events (Avro if configured).
- `example/ConsumerExample` - consumes and logs.

Run:
```bash
dotnet run --project ./example/ConsumerExample/ConsumerExample.csproj
dotnet run --project ./example/ProducerExample/ProducerExample.csproj
```

Producer copies `schemas/invoice.created.avsc` to output automatically.
