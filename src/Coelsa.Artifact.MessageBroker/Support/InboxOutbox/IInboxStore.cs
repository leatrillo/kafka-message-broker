

namespace Coelsa.Artifact.MessageBroker.Support.InboxOutbox
{
    public interface IInboxStore
    {
        Task<bool> AlreadyProcessedAsync(string messageId, CancellationToken ct = default);
        Task MarkProcessedAsync(string messageId, CancellationToken ct = default);
    }
}
