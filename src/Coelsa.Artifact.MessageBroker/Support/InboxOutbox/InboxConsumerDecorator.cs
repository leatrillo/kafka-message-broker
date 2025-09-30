using Coelsa.Artifact.MessageBroker.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Coelsa.Artifact.MessageBroker.Support.InboxOutbox
{
    public sealed class InboxConsumerDecorator : IMessageConsumer
    {
        private readonly IMessageConsumer _messageConsumer;
        private readonly IInboxStore _store;

        public InboxConsumerDecorator(IMessageConsumer messageConsumer, IInboxStore store)
        { 
            _messageConsumer = messageConsumer; 
            _store = store; 
        }

        public Task ConsumeAsync<TData>(Func<CloudEventMessage<TData>, string?, IReadOnlyDictionary<string, string>, Task> onMessage, CancellationToken cancellationToken)
        {
            return _messageConsumer.ConsumeAsync<TData>(async (evt, key, headers) =>
            {
                if (await _store.AlreadyProcessedAsync(evt.Id, cancellationToken)) return;
                await onMessage(evt, key, headers);
                await _store.MarkProcessedAsync(evt.Id, cancellationToken);
            }, cancellationToken);
        }

        public Task ConsumeBatchAsync<TData>(
            int maxBatchSize, TimeSpan maxWaitTime,
            Func<IReadOnlyList<(CloudEventMessage<TData> evt, string? key, IReadOnlyDictionary<string, string> headers)>, Task> onBatch,
            CancellationToken cancellationToken)
        {
            return _messageConsumer.ConsumeBatchAsync<TData>(maxBatchSize, maxWaitTime, async batch =>
            {
                var filtered = new List<(CloudEventMessage<TData>, string?, IReadOnlyDictionary<string, string>)>(batch.Count);
                foreach (var it in batch)
                {
                    if (!await _store.AlreadyProcessedAsync(it.evt.Id, cancellationToken))
                    {
                        filtered.Add(it);
                        await _store.MarkProcessedAsync(it.evt.Id, cancellationToken);
                    }
                }
                if (filtered.Count > 0) await onBatch(filtered);
            }, cancellationToken);
        }

        public ValueTask DisposeAsync() => _messageConsumer.DisposeAsync();
    }
}
