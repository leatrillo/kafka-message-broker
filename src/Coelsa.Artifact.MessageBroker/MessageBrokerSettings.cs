namespace Coelsa.Artifact.MessageBroker;

public sealed record AvroOptions(
    string SubjectPrefix,
    IDictionary<string, string> SchemaMap
);

public sealed record MessageBrokerSettings(
    string BootstrapServers,
    string GroupId,
    string Topic,
    bool   EnableAutoCommit = true,
    string? Username = null,
    string? Password = null,
    string  SecurityProtocol = "Plaintext",
    string? SchemaRegistryUrl = null,
    AvroOptions? Avro = null
);
