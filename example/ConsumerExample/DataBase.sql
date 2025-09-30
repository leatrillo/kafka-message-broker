CREATE TABLE dbo.Inbox (
  MessageId       nvarchar(100) NOT NULL PRIMARY KEY,
  ReceivedAtUtc   datetime2     NOT NULL DEFAULT sysutcdatetime()
);

CREATE TABLE dbo.Outbox (
  Id              nvarchar(100) NOT NULL PRIMARY KEY,
  Topic           nvarchar(200) NOT NULL,
  [Key]           nvarchar(200) NULL,
  Payload         varbinary(max) NOT NULL,
  CreatedAtUtc    datetime2     NOT NULL DEFAULT sysutcdatetime(),
  SentAtUtc       datetime2     NULL,
  Attempts        int           NOT NULL DEFAULT 0,
  LastError       nvarchar(2000) NULL
);

CREATE INDEX IX_Outbox_SentNull ON dbo.Outbox (SentAtUtc) INCLUDE (Topic);