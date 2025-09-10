using Coelsa.Artifact.MessageBroker.Models;

namespace Coelsa.Artifact.MessageBroker;

public interface IMessagePublisher
{
    Task PublishAsync<TData>(
        CloudEventMessage<TData> message,
        string? key = null,
        IDictionary<string, string>? headers = null,
        CancellationToken ct = default);
}
