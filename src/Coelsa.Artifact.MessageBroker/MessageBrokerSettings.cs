namespace Coelsa.Artifact.MessageBroker;

public sealed record MessageBrokerSettings(
    string BootstrapServers,
    string GroupId,
    string Topic,
    bool EnableAutoCommit = true,
    string? Username = null,
    string? Password = null,
    string? SchemaRegistryUrl = null,
    AvroSettings? Avro = null,
    DataBaseSettings? DataBase = null
);

public sealed record AvroSettings(
    string SubjectPrefix,
    IDictionary<string, string> SchemaMap
);

public sealed record DataBaseSettings(
    string Provider, // "SqlServer"
    string ConnectionString,
    string Schema = "dbo",
    string InboxTable = "Inbox",
    string OutboxTable = "Outbox"
);