using System.Text.Json.Serialization;

namespace Coelsa.Artifact.MessageBroker.Models;

public class CloudEventMessage<TData>
{
    [JsonPropertyName("specversion")] 
    public string SpecVersion { get; init; } = "1.0";

    [JsonPropertyName("id")]          
    public string Id { get; init; } = Guid.NewGuid().ToString();

    [JsonPropertyName("source")]      
    public string Source { get; init; } = default!;

    [JsonPropertyName("type")]        
    public string Type { get; init; } = default!;

    [JsonPropertyName("time")]        
    public DateTimeOffset Time { get; init; } = DateTimeOffset.UtcNow;

    [JsonPropertyName("dataContentType")] 
    public string DataContentType { get; init; } = "application/json";

    [JsonPropertyName("subject")]     
    public string? Subject { get; init; }

    [JsonPropertyName("data")]        
    public TData Data { get; init; } = default!;

    [JsonPropertyName("extensions")]  
    public IDictionary<string, object>? Extensions { get; init; }

    public static CloudEventMessage<TData> Create(
        TData data,
        string type,
        string source,
        string? subject = null,
        string dataContentType = "application/json",
        IDictionary<string, object>? extensions = null,
        string? id = null,
        DateTimeOffset? time = null) =>
        new()
        {
            SpecVersion = "1.0",
            Id = id ?? Guid.NewGuid().ToString(),
            Source = source,
            Type = type,
            Time = time ?? DateTimeOffset.UtcNow,
            DataContentType = dataContentType,
            Subject = subject,
            Data = data,
            Extensions = extensions
        };
}
