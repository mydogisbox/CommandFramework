using System.Data;
using CommandFramework.Core;
using Npgsql;

namespace CommandFramework.Postgres;

public class PostgresEventStore : IEventStore
{
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
            var currentSequence = await GetCurrentSequenceAsync(conn, tx, streamId);

            if (currentSequence != expectedSequence)
                return $"Concurrency conflict on stream '{streamId}': expected sequence {expectedSequence}, actual {currentSequence}.";

            var stored = new List<StoredEvent>();
            var nextSequence = expectedSequence + 1;

            foreach (var (eventType, payload) in events)
            {
                var occurredAt = DateTimeOffset.UtcNow;

                await using var cmd = new NpgsqlCommand(@"
                    INSERT INTO events (stream_id, sequence, event_type, payload, occurred_at)
                    VALUES (@streamId, @sequence, @eventType, @payload::jsonb, @occurredAt)", conn, tx);

                cmd.Parameters.AddWithValue("streamId", streamId);
                cmd.Parameters.AddWithValue("sequence", nextSequence);
                cmd.Parameters.AddWithValue("eventType", eventType);
                cmd.Parameters.AddWithValue("payload", payload);
                cmd.Parameters.AddWithValue("occurredAt", occurredAt);

                await cmd.ExecuteNonQueryAsync();

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

        await using var cmd = new NpgsqlCommand(@"
            SELECT stream_id, sequence, event_type, payload, occurred_at
            FROM events
            WHERE stream_id = @streamId
            ORDER BY sequence", conn);

        cmd.Parameters.AddWithValue("streamId", streamId);

        var events = new List<StoredEvent>();
        await using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            events.Add(new StoredEvent(
                StreamId: reader.GetString(0),
                Sequence: reader.GetInt32(1),
                EventType: reader.GetString(2),
                Payload: reader.GetString(3),
                OccurredAt: reader.GetFieldValue<DateTimeOffset>(4)));
        }

        return events;
    }

    private static async Task<int> GetCurrentSequenceAsync(
        NpgsqlConnection conn,
        NpgsqlTransaction tx,
        string streamId)
    {
        await using var cmd = new NpgsqlCommand(@"
            SELECT COALESCE(MAX(sequence), -1)
            FROM events
            WHERE stream_id = @streamId", conn, tx);

        cmd.Parameters.AddWithValue("streamId", streamId);
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt32(result);
    }
}