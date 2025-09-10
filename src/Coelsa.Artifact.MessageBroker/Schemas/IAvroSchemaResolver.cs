namespace Coelsa.Artifact.MessageBroker.Schemas;

public interface IAvroSchemaResolver
{
    Task<(string subject, string schemaJson)> ResolveAsync(string eventType, CancellationToken ct = default);
}
