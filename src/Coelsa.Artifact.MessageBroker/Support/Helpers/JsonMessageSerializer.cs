using System.Text;
using System.Text.Json;

namespace Coelsa.Artifact.MessageBroker.Support.Helpers;

internal static class JsonMessageSerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public static byte[] Serialize<T>(T value) =>
        Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value, Options));

    public static T Deserialize<T>(byte[] data) =>
        JsonSerializer.Deserialize<T>(data, Options)!;
}
