using Coelsa.Artifact.MessageBroker.Models;

namespace Coelsa.Artifact.MessageBroker;

public interface IMessageConsumer : IAsyncDisposable
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TData"></typeparam>
    /// <param name="onMessage"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task ConsumeAsync<TData>(Func<CloudEventMessage<TData>, string?, IReadOnlyDictionary<string, string>, Task> onMessage, CancellationToken cancellationToken);

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="TData"></typeparam>
    /// <param name="maxBatchSize"></param>
    /// <param name="maxWaitTime"></param>
    /// <param name="onBatch"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task ConsumeBatchAsync<TData>(int maxBatchSize, TimeSpan maxWaitTime, Func<IReadOnlyList<(CloudEventMessage<TData> evt, string? key, IReadOnlyDictionary<string, string> headers)>, Task> onBatch, CancellationToken cancellationToken);
}
