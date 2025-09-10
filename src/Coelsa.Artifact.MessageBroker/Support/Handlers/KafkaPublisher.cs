using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Avro.Generic;
using Coelsa.Artifact.MessageBroker.Models;
using Coelsa.Artifact.MessageBroker.Schemas;
using Coelsa.Artifact.MessageBroker.Support.Helpers;

namespace Coelsa.Artifact.MessageBroker.Support.Handlers;

internal sealed class KafkaPublisher : IMessagePublisher, IAsyncDisposable
{
    private readonly IProducer<string?, byte[]> _producer;
    private readonly MessageBrokerSettings _settings;
    private readonly ISchemaRegistryClient? _schemaRegistry;
    private readonly IAvroSchemaResolver? _schemaResolver;

    public KafkaPublisher(MessageBrokerSettings settings, ISchemaRegistryClient? schemaRegistry = null, IAvroSchemaResolver? schemaResolver = null)
    {
        _settings = settings;
        _schemaRegistry = schemaRegistry;
        _schemaResolver = schemaResolver;

        var config = new ProducerConfig
        {
            BootstrapServers = settings.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
        };

        if (!string.IsNullOrWhiteSpace(settings.Username))
        {
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.Plain;
            config.SaslUsername = settings.Username;
            config.SaslPassword = settings.Password;
        }

        _producer = new ProducerBuilder<string?, byte[]>(config).Build();
    }

    public async Task PublishAsync<TData>(CloudEventMessage<TData> message, string? key = null, IDictionary<string, string>? headers = null, CancellationToken cancellationToken = default)
    {
        byte[] payloadBytes;

        var avroEnabled = _schemaRegistry is not null && _schemaResolver is not null && _settings.Avro is not null;

        if (avroEnabled)
        {
            var (_, schemaJson) = await _schemaResolver!.ResolveAsync(message.Type, cancellationToken);
            var recordSchema = (Avro.RecordSchema)Avro.Schema.Parse(schemaJson);

            var rawDict = JsonSerializer.Deserialize<Dictionary<string, object>>(JsonSerializer.Serialize(message.Data)) ?? new();
            var normalized = NormalizeJsonForAvro(rawDict);

            var record = CreateGenericRecord(recordSchema, normalized);

            var avroSerializerConfig = new AvroSerializerConfig
            {
                AutoRegisterSchemas = false,
                UseLatestVersion = false,
                SubjectNameStrategy = SubjectNameStrategy.TopicRecord
            };
            var avroSerializer = new AvroSerializer<GenericRecord>(_schemaRegistry!, avroSerializerConfig);
            var avroBinary = await avroSerializer.SerializeAsync(record, new SerializationContext(MessageComponentType.Value, _settings.Topic));

            var cloudEventBinary = new
            {
                specversion = message.SpecVersion,
                id = message.Id,
                source = message.Source,
                type = message.Type,
                time = message.Time,
                dataContentType = "avro/binary",
                subject = message.Subject,
                data_base64 = avroBinary,
                extensions = message.Extensions
            };

            payloadBytes = JsonSerializer.SerializeToUtf8Bytes(cloudEventBinary);
        }
        else
        {
            payloadBytes = JsonMessageSerializer.Serialize(message);
        }

        var kafkaMessage = new Message<string?, byte[]>
        {
            Key = key,
            Value = payloadBytes,
            Headers = new Headers()
        };

        if (headers is not null)
        {
            foreach (var kv in headers)
                kafkaMessage.Headers!.Add(kv.Key, Encoding.UTF8.GetBytes(kv.Value));
        }

        await _producer.ProduceAsync(_settings.Topic, kafkaMessage, cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        _producer.Flush(TimeSpan.FromSeconds(5));
        _producer.Dispose();
        _schemaRegistry?.Dispose();
        return ValueTask.CompletedTask;
    }



    private static GenericRecord CreateGenericRecord(Avro.RecordSchema schema, IDictionary<string, object?> values)
    {
        var record = new GenericRecord(schema);
        foreach (var field in schema.Fields)
        {
            values.TryGetValue(field.Name, out var value);
            record.Add(field.Name, value);
        }
        return record;
    }

    private static IDictionary<string, object?> NormalizeJsonForAvro(IDictionary<string, object> source)
    {
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var kv in source)
            result[kv.Key] = NormalizeJsonValue(kv.Value);
        return result;
    }

    private static object? NormalizeJsonValue(object? value)
    {
        if (value is null) return null;

        if (value is JsonElement je)
        {
            switch (je.ValueKind)
            {
                case JsonValueKind.Null: return null;
                case JsonValueKind.True: return true;
                case JsonValueKind.False: return false;
                case JsonValueKind.String: return je.GetString();
                case JsonValueKind.Number:
                    if (je.TryGetInt64(out var i64)) return i64;
                    if (je.TryGetDouble(out var dbl)) return dbl;
                    return je.GetRawText();

                case JsonValueKind.Object:
                    {

                        var map = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                        foreach (var prop in je.EnumerateObject())
                            map[prop.Name] = NormalizeJsonValue(prop.Value);
                        return map;
                    }

                case JsonValueKind.Array:
                    {
                        var list = new List<object?>();
                        foreach (var el in je.EnumerateArray())
                            list.Add(NormalizeJsonValue(el));
                        return list;
                    }

                default:
                    return je.GetRawText();
            }
        }

        return value;
    }
}
