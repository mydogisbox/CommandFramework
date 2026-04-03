using System.Data;
using CommandFramework.Core;
using Dapper;
using Npgsql;

namespace CommandFramework.Postgres;

public class PostgresEventStore : IEventStore
{
    static PostgresEventStore()
    {
        SqlMapper.AddTypeHandler(new DateTimeOffsetHandler());
    }

    private sealed class DateTimeOffsetHandler : SqlMapper.TypeHandler<DateTimeOffset>
    {
        public override void SetValue(IDbDataParameter parameter, DateTimeOffset value)
            => parameter.Value = value;

        public override DateTimeOffset Parse(object value) => value switch
        {
            DateTimeOffset dto => dto,
            DateTime dt => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc)),
            _ => throw new InvalidCastException($"Cannot convert {value.GetType().Name} to DateTimeOffset")
        };
    }

    private readonly string _connectionString;

    public PostgresEventStore(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task<Result<IReadOnlyList<StoredEvent>>> AppendAsync(
        string streamId,
        int expectedSequence,
        IEnumerable<(string eventType, string payload)> events,
        EventProcessor? processor = null,
        IEnumerable<object>? domainEvents = null)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted);

        try
        {
            var currentSequence = await conn.ExecuteScalarAsync<int>(
                "SELECT COALESCE(MAX(sequence), -1) FROM events WHERE stream_id = @streamId",
                new { streamId }, tx);

            if (currentSequence != expectedSequence)
                return $"Concurrency conflict on stream '{streamId}': expected sequence {expectedSequence}, actual {currentSequence}.";

            var stored = new List<StoredEvent>();
            var nextSequence = expectedSequence + 1;

            foreach (var (eventType, payload) in events)
            {
                var occurredAt = DateTimeOffset.UtcNow;

                await conn.ExecuteAsync(@"
                    INSERT INTO events (stream_id, sequence, event_type, payload, occurred_at)
                    VALUES (@streamId, @sequence, @eventType, @payload::jsonb, @occurredAt)",
                    new { streamId, sequence = nextSequence, eventType, payload, occurredAt }, tx);

                stored.Add(new StoredEvent(streamId, nextSequence, eventType, payload, occurredAt));
                nextSequence++;
            }

            // Run event processors inside the same transaction
            if (processor != null && domainEvents != null)
                await processor.ProcessAsync(domainEvents, tx);

            await tx.CommitAsync();
            return Result<IReadOnlyList<StoredEvent>>.Ok(stored);
        }
        catch
        {
            await tx.RollbackAsync();
            throw;
        }
    }

    public async Task<IReadOnlyList<StoredEvent>> LoadAsync(string streamId)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        var events = await conn.QueryAsync<StoredEvent>(@"
            SELECT stream_id AS StreamId, sequence AS Sequence, event_type AS EventType,
                   payload AS Payload, occurred_at AS OccurredAt
            FROM events
            WHERE stream_id = @streamId
            ORDER BY sequence",
            new { streamId });

        return events.ToList();
    }
}
