using Coelsa.Artifact.MessageBroker.Models;

namespace Coelsa.Artifact.MessageBroker;

public interface IMessagePublisher : IAsyncDisposable
{
    Task PublishAsync<TData>(CloudEventMessage<TData> message, string? key = null, IDictionary<string, string>? headers = null, CancellationToken cancellationToken = default);

    Task PublishBatchAsync<TData>(IEnumerable<CloudEventMessage<TData>> messages, Func<CloudEventMessage<TData>, string?>? keySelector = null, CancellationToken cancellationToken = default);
}
