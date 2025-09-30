using System.Data.Common;
using Dapper;
using Coelsa.Artifact.MessageBroker;
using Coelsa.Artifact.MessageBroker.Support.InboxOutbox;
using System.Data.SqlClient;

namespace ProducerExample
{
    public sealed class SqlOutboxStore : IOutboxStore
    {
        private readonly DataBaseSettings _cfg;

        public SqlOutboxStore(MessageBrokerSettings brokerSettings)
        {
            _cfg = brokerSettings.DataBase ?? throw new InvalidOperationException("Kafka:DataBase missing");
        }

        private DbConnection Conn() => new SqlConnection(_cfg.ConnectionString);

        public async Task SaveAsync(string id, string topic, string? key, byte[] payload, CancellationToken ct = default)
        {
            using var con = Conn();
            var sql = $@"IF NOT EXISTS (SELECT 1 FROM [{_cfg.Schema}].[{_cfg.OutboxTable}] WHERE [Id]=@id)
                     INSERT INTO [{_cfg.Schema}].[{_cfg.OutboxTable}]([Id],[Topic],[Key],[Payload])
                     VALUES (@id,@topic,@key,@payload);";
            await con.ExecuteAsync(new CommandDefinition(sql, new { id, topic, key, payload }, cancellationToken: ct));
        }

        public async Task MarkSentAsync(string id, CancellationToken ct = default)
        {
            using var con = Conn();
            var sql = $@"UPDATE [{_cfg.Schema}].[{_cfg.OutboxTable}]
                     SET [SentAtUtc]=sysutcdatetime(), [Attempts]=[Attempts]+1, [LastError]=NULL
                     WHERE [Id]=@id;";
            await con.ExecuteAsync(new CommandDefinition(sql, new { id }, cancellationToken: ct));
        }
    }
}
