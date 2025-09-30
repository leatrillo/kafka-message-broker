using System.Data.Common;
using System.Data.SqlClient;
using Dapper;
using Coelsa.Artifact.MessageBroker;
using Coelsa.Artifact.MessageBroker.Support.InboxOutbox;

namespace ConsumerExample
{
    public sealed class SqlInboxStore : IInboxStore
    {
        private readonly DataBaseSettings _cfg;

        public SqlInboxStore(MessageBrokerSettings brokerSettings)
        {
            _cfg = brokerSettings.DataBase ?? throw new InvalidOperationException("Kafka:DataBase missing");
        }

        private DbConnection Conn() => new SqlConnection(_cfg.ConnectionString);

        public async Task<bool> AlreadyProcessedAsync(string messageId, CancellationToken ct = default)
        {
            using var con = Conn();
            var sql = $@"SELECT 1 FROM [{_cfg.Schema}].[{_cfg.InboxTable}] WHERE [MessageId]=@id";
            var res = await con.ExecuteScalarAsync<int?>(new CommandDefinition(sql, new { id = messageId }, cancellationToken: ct));
            return res.HasValue;
        }

        public async Task MarkProcessedAsync(string messageId, CancellationToken ct = default)
        {
            using var con = Conn();
            var sql = $@"IF NOT EXISTS (SELECT 1 FROM [{_cfg.Schema}].[{_cfg.InboxTable}] WHERE [MessageId]=@id)
                     INSERT INTO [{_cfg.Schema}].[{_cfg.InboxTable}]([MessageId]) VALUES (@id);";
            await con.ExecuteAsync(new CommandDefinition(sql, new { id = messageId }, cancellationToken: ct));
        }
    }
}
