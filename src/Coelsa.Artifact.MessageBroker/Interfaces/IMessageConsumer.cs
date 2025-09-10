using Coelsa.Artifact.MessageBroker.Models;

namespace Coelsa.Artifact.MessageBroker;

public interface IMessageConsumer : IAsyncDisposable
{
    /// <summary>
    /// Start consuming messages from the configured Kafka topic.
    /// </summary>
    Task ConsumeAsync<TData>(Func<CloudEventMessage<TData>, string?, IReadOnlyDictionary<string, string>, Task> onMessage, CancellationToken cancellationToken);

}
