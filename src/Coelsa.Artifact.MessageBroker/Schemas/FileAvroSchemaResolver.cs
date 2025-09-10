namespace Coelsa.Artifact.MessageBroker.Schemas;

public sealed class FileAvroSchemaResolver : IAvroSchemaResolver
{
    private readonly string _subjectPrefix;
    private readonly IDictionary<string, string> _map;

    public FileAvroSchemaResolver(MessageBrokerSettings options)
    {
        if (options.Avro is null)
            throw new InvalidOperationException("Avro options are not configured.");

        _subjectPrefix = options.Avro.SubjectPrefix ?? "coelsa";
        _map = options.Avro.SchemaMap ?? new Dictionary<string, string>();
    }

    public Task<(string subject, string schemaJson)> ResolveAsync(string eventType, CancellationToken ct = default)
    {
        if (!_map.TryGetValue(eventType, out var path))
            throw new InvalidOperationException($"No schema path configured for eventType '{eventType}'");

        if (!System.IO.File.Exists(path))
            throw new FileNotFoundException($"Schema file not found: {path}");

        var json = System.IO.File.ReadAllText(path);
        var subject = $"{_subjectPrefix}.{eventType}";
        return Task.FromResult((subject, json));
    }
}
