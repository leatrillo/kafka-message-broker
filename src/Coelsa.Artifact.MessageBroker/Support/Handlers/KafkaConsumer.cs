using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Avro.Generic;
using Coelsa.Artifact.MessageBroker.Models;
using Coelsa.Artifact.MessageBroker.Support.Helpers;

namespace Coelsa.Artifact.MessageBroker.Support.Handlers;

internal sealed class KafkaConsumer : IMessageConsumer
{
    private readonly IConsumer<string?, byte[]> _consumer;
    private readonly MessageBrokerSettings _settings;
    private readonly ISchemaRegistryClient? _schemaRegistry;

    public KafkaConsumer(MessageBrokerSettings settings, ISchemaRegistryClient? schemaRegistry = null)
    {
        _settings = settings;
        _schemaRegistry = schemaRegistry;

        var config = new ConsumerConfig
        {
            BootstrapServers = settings.BootstrapServers,
            GroupId = settings.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = settings.EnableAutoCommit
        };

        if (!string.IsNullOrWhiteSpace(settings.Username))
        {
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.Plain;
            config.SaslUsername = settings.Username;
            config.SaslPassword = settings.Password;
        }

        _consumer = new ConsumerBuilder<string?, byte[]>(config).Build();
    }

    public Task ConsumeAsync<TData>(Func<CloudEventMessage<TData>, string?, IReadOnlyDictionary<string, string>, Task> onMessage, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(_settings.Topic);

        return Task.Run(async () =>
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var cr = _consumer.Consume(cancellationToken);
                    var root = JsonDocument.Parse(cr.Message.Value).RootElement;

                    CloudEventMessage<TData> cloudEvent;

                    // AVRO (data_base64) + registry disponible
                    if (root.TryGetProperty("data_base64", out var base64Prop) && _schemaRegistry is not null)
                    {
                        var avroBytes = base64Prop.GetBytesFromBase64();
                        var avroDeserializer = new AvroDeserializer<GenericRecord>(_schemaRegistry);

                        var genericRecord = await avroDeserializer.DeserializeAsync(
                            avroBytes, isNull: false,
                            new SerializationContext(MessageComponentType.Value, _settings.Topic));

                        // Normalizar valores Avro a CLR
                        var dataMap = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                        foreach (var field in genericRecord.Schema.Fields)
                        {
                            object? fieldValue = genericRecord.TryGetValue(field.Name, out var tmp) ? tmp : null;
                            dataMap[field.Name] = NormalizeAvroValue(fieldValue);
                        }

                        var dataJson = JsonSerializer.Serialize(dataMap);
                        var dataObject = JsonSerializer.Deserialize<TData>(dataJson)!;

                        cloudEvent = new CloudEventMessage<TData>
                        {
                            SpecVersion = root.GetProperty("specversion").GetString()!,
                            Id = root.GetProperty("id").GetString()!,
                            Source = root.GetProperty("source").GetString()!,
                            Type = root.GetProperty("type").GetString()!,
                            Time = root.GetProperty("time").GetDateTimeOffset(),
                            DataContentType = "avro/binary",
                            Subject = root.TryGetProperty("subject", out var subj) ? subj.GetString() : null,
                            Data = dataObject,
                            Extensions = root.TryGetProperty("extensions", out var ext)
                                                ? JsonSerializer.Deserialize<Dictionary<string, object>>(ext.GetRawText())
                                                : null
                        };
                    }
                    else
                    {
                        // JSON puro
                        cloudEvent = JsonMessageSerializer.Deserialize<CloudEventMessage<TData>>(cr.Message.Value);
                    }

                    var key = cr.Message.Key;
                    var headers = cr.Message.Headers
                        .ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.GetValueBytes()));

                    await onMessage(cloudEvent, key, headers);
                }
            }
            catch (OperationCanceledException)
            {
                //logger.error("ex.Message");
                //throw;
            }
        }, cancellationToken);
    }

    public Task ConsumeBatchAsync<TData>(int maxBatchSize, TimeSpan maxWaitTime, Func<IReadOnlyList<(CloudEventMessage<TData> evt, string? key, IReadOnlyDictionary<string, string> headers)>, Task> onBatch, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(_settings.Topic);

        return Task.Run(async () =>
        {
            var buffer = new List<(CloudEventMessage<TData>, string?, IReadOnlyDictionary<string, string>)>(maxBatchSize);
            var batchStart = DateTimeOffset.UtcNow;

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var cr = _consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (cr is null)
                    {
                        if (buffer.Count > 0 && DateTimeOffset.UtcNow - batchStart >= maxWaitTime)
                        {
                            await onBatch(buffer);
                            buffer.Clear();
                            batchStart = DateTimeOffset.UtcNow;
                        }
                        continue;
                    }

                    var (evt, key, headers) = Deserialize<TData>(cr); // usa tu lógica existente
                    buffer.Add((evt, key, headers));

                    if (buffer.Count >= maxBatchSize || DateTimeOffset.UtcNow - batchStart >= maxWaitTime)
                    {
                        await onBatch(buffer);
                        buffer.Clear();
                        batchStart = DateTimeOffset.UtcNow;
                    }
                }
            }
            catch (OperationCanceledException) 
            {

            }
        }, cancellationToken);
    }


    public ValueTask DisposeAsync()
    {
        try { _consumer.Close(); } catch { }
        _consumer.Dispose();
        return ValueTask.CompletedTask;
    }

    // ---------- helpers ----------
    private (CloudEventMessage<TData> evt, string? key, IReadOnlyDictionary<string, string> headers)Deserialize<TData>(ConsumeResult<string?, byte[]> cr)
    {
        var root = JsonDocument.Parse(cr.Message.Value).RootElement;
        CloudEventMessage<TData> ce;

        if (root.TryGetProperty("data_base64", out var b64) && _schemaRegistry is not null)
        {
            var avroBytes = b64.GetBytesFromBase64();
            var deser = new AvroDeserializer<GenericRecord>(_schemaRegistry);
            var record = deser.DeserializeAsync(avroBytes, false, new SerializationContext(MessageComponentType.Value, _settings.Topic))
                            .ConfigureAwait(false).GetAwaiter().GetResult();

            var map = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var f in record.Schema.Fields)
            {
                var ok = record.TryGetValue(f.Name, out var tmp);
                map[f.Name] = NormalizeAvroValue(ok ? tmp : null);
            }
            var dataJson = JsonSerializer.Serialize(map);
            var dataObj = JsonSerializer.Deserialize<TData>(dataJson)!;

            ce = new CloudEventMessage<TData>
            {
                SpecVersion = root.GetProperty("specversion").GetString()!,
                Id = root.GetProperty("id").GetString()!,
                Source = root.GetProperty("source").GetString()!,
                Type = root.GetProperty("type").GetString()!,
                Time = root.GetProperty("time").GetDateTimeOffset(),
                DataContentType = "avro/binary",
                Subject = root.TryGetProperty("subject", out var subj) ? subj.GetString() : null,
                Data = dataObj,
                Extensions = root.TryGetProperty("extensions", out var ext)
                                    ? JsonSerializer.Deserialize<Dictionary<string, object>>(ext.GetRawText())
                                    : null
            };
        }
        else
        {
            ce = JsonMessageSerializer.Deserialize<CloudEventMessage<TData>>(cr.Message.Value);
        }

        var key = cr.Message.Key;
        var headers = cr.Message.Headers.ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.GetValueBytes()));
        return (ce, key, headers);
    }


    private static object? NormalizeAvroValue(object? value)
    {
        if (value is null) return null;

        // *** Fix: evitar referencia de tipo faltante usando reflexión / ToString ***
        var typeName = value.GetType().FullName;
        if (typeName == "Avro.Util.Utf8")
            return value.ToString();

        // Nested record -> dictionary
        if (value is GenericRecord nestedRecord)
        {
            var map = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var field in nestedRecord.Schema.Fields)
            {
                var inner = nestedRecord.TryGetValue(field.Name, out var tmp) ? tmp : null;
                map[field.Name] = NormalizeAvroValue(inner);
            }
            return map;
        }

        // Arrays / collections
        if (value is System.Collections.IEnumerable enumerable && value is not string)
        {
            var list = new List<object?>();
            foreach (var item in enumerable)
                list.Add(NormalizeAvroValue(item));
            return list;
        }

        return value;
    }
}
