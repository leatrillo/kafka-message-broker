using Coelsa.Artifact.MessageBroker.Models;
using Coelsa.Artifact.MessageBroker.Support.Helpers;

namespace Coelsa.Artifact.MessageBroker.Support.InboxOutbox
{
    public sealed class OutboxPublisher : IMessagePublisher
    {
        private readonly IMessagePublisher _messagePublisher;
        private readonly IOutboxStore _store;
        private readonly MessageBrokerSettings _settings;

        public OutboxPublisher(IMessagePublisher messagePublisher, IOutboxStore store, MessageBrokerSettings settings)
        {
            _messagePublisher = messagePublisher; 
            _store = store; 
            _settings = settings;
        }

        public async Task PublishAsync<TData>(CloudEventMessage<TData> message, string? key = null,
            IDictionary<string, string>? headers = null, CancellationToken cancellationToken = default)
        {
            // Persistimos el rastro como CloudEvent JSON (si preferís Avro binario, cambiá el serializer)
            var bytes = JsonMessageSerializer.Serialize(message);
            await _store.SaveAsync(message.Id, _settings.Topic, key, bytes, cancellationToken);

            try
            {
                await _messagePublisher.PublishAsync(message, key, headers, cancellationToken);
                await _store.MarkSentAsync(message.Id, cancellationToken);
            }
            catch
            {
                throw; // sin retries aquí
            }
        }

        public Task PublishBatchAsync<TData>(IEnumerable<CloudEventMessage<TData>> messages,
            Func<CloudEventMessage<TData>, string?>? keySelector = null,
            CancellationToken cancellationToken = default)
            => _messagePublisher.PublishBatchAsync(messages, keySelector, cancellationToken);

        public ValueTask DisposeAsync() => _messagePublisher.DisposeAsync();
    }
}
