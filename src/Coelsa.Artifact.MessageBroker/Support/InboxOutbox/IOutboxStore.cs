

namespace Coelsa.Artifact.MessageBroker.Support.InboxOutbox
{
    public interface IOutboxStore
    {
        Task SaveAsync(string id, string topic, string? key, byte[] payload, CancellationToken ct = default);
        Task MarkSentAsync(string id, CancellationToken ct = default);
    }
}
