using System.Text.Json;
using CommandFramework.Core;
using CommandFramework.Http;
using CommandFramework.Postgres;
using Xunit;

namespace CommandFramework.Tests;

[Collection("Database")]
public class CounterTests : IAsyncLifetime
{
    private readonly TestDatabase _db = new();
    private AggregateHandler<CounterState, CounterEvent> _handler = null!;

    public async Task InitializeAsync()
    {
        await _db.InitializeAsync();
        _handler = new AggregateHandler<CounterState, CounterEvent>(
            CounterAggregate.Definition,
            new PostgresEventStore(_db.ConnectionString),
            "counters");
    }

    public Task DisposeAsync() => Task.CompletedTask;

    // ── Increment ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Increment_succeeds()
    {
        var result = await ExecuteAsync(null, [
            new("Increment", Json(new { by = 5 }))
        ]);

        Assert.True(result.IsSuccess);
        Assert.Single(result.Value);
        Assert.Equal("CounterIncremented", result.Value[0].Events[0]);
    }

    [Fact]
    public async Task Increment_fails_with_non_positive_value()
    {
        var result = await ExecuteAsync(null, [
            new("Increment", Json(new { by = 0 }))
        ]);

        Assert.True(result.IsError);
        Assert.Contains("positive", result.Error);
    }

    // ── Decrement ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Decrement_succeeds()
    {
        var aggregateId = await IncrementAsync(10);

        var result = await ExecuteAsync(aggregateId, [
            new("Decrement", Json(new { by = 3 }))
        ]);

        Assert.True(result.IsSuccess);

        var state = await GetStateAsync(aggregateId);
        Assert.Equal(7, state!.Value);
    }

    [Fact]
    public async Task Decrement_fails_with_non_positive_value()
    {
        var aggregateId = await IncrementAsync(10);

        var result = await ExecuteAsync(aggregateId, [
            new("Decrement", Json(new { by = -1 }))
        ]);

        Assert.True(result.IsError);
        Assert.Contains("positive", result.Error);
    }

    // ── Reset ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task Reset_succeeds()
    {
        var aggregateId = await IncrementAsync(10);

        var result = await ExecuteAsync(aggregateId, [
            new("Reset", Json(new { }))
        ]);

        Assert.True(result.IsSuccess);

        var state = await GetStateAsync(aggregateId);
        Assert.Equal(0, state!.Value);
    }

    // ── Multi-command batches ─────────────────────────────────────────────────

    [Fact]
    public async Task Batch_increment_and_decrement_in_single_request()
    {
        var aggregateId = Guid.NewGuid().ToString();

        var result = await ExecuteAsync(aggregateId, [
            new("Increment", Json(new { by = 10 })),
            new("Decrement", Json(new { by = 3 }))
        ]);

        Assert.True(result.IsSuccess);
        Assert.Equal(2, result.Value.Count);

        var state = await GetStateAsync(aggregateId);
        Assert.Equal(7, state!.Value);
    }

    [Fact]
    public async Task Batch_rolls_back_if_any_command_fails()
    {
        var aggregateId = Guid.NewGuid().ToString();

        var result = await ExecuteAsync(aggregateId, [
            new("Increment", Json(new { by = 10 })),
            new("Decrement", Json(new { by = 0 }))  // fails — non-positive
        ]);

        Assert.True(result.IsError);

        var state = await GetStateAsync(aggregateId);
        Assert.Null(state);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private Task<Result<IReadOnlyList<CommandSuccess>>> ExecuteAsync(
        string? aggregateId,
        List<CommandEnvelope> commands)
        => _handler.ExecuteAsync(
            new CommandBatch(aggregateId, commands),
            CounterAggregate.DeserializeCommand,
            CounterAggregate.DeserializeEvent);

    private async Task<string> IncrementAsync(int by)
    {
        var aggregateId = Guid.NewGuid().ToString();
        var result = await ExecuteAsync(aggregateId, [new("Increment", Json(new { by }))]);
        Assert.True(result.IsSuccess);
        return aggregateId;
    }

    private async Task<CounterState?> GetStateAsync(string aggregateId)
    {
        var store = new PostgresEventStore(_db.ConnectionString);
        var stored = await store.LoadAsync($"counters/{aggregateId}");
        if (stored.Count == 0) return null;
        return Aggregate.Fold<CounterState, CounterEvent>(
            stored.Select(e => CounterAggregate.DeserializeEvent(e.EventType, e.Payload)),
            CounterAggregate.Apply);
    }

    private static JsonElement Json(object value)
        => JsonSerializer.SerializeToElement(value, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
}

[Collection("Database")]
public class EventProcessorTests : IAsyncLifetime
{
    private readonly TestDatabase _db = new();

    public async Task InitializeAsync() => await _db.InitializeAsync();
    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Sync_reaction_writes_to_outbox_in_same_transaction()
    {
        var aggregateId = Guid.NewGuid().ToString();
        var streamId = $"counters/{aggregateId}";

        var processor = new EventProcessor([
            EventReaction.On<CounterIncremented>(async (e, tx) =>
            {
                var conn = ((Npgsql.NpgsqlTransaction)tx).Connection!;
                await using var cmd = new Npgsql.NpgsqlCommand(@"
                    INSERT INTO outbox (stream_id, event_type, payload, created_at)
                    VALUES (@streamId, @eventType, @payload::jsonb, @createdAt)", conn, (Npgsql.NpgsqlTransaction)tx);

                cmd.Parameters.AddWithValue("streamId",  streamId);
                cmd.Parameters.AddWithValue("eventType", nameof(CounterIncremented));
                cmd.Parameters.AddWithValue("payload",   JsonSerializer.Serialize(e, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }));
                cmd.Parameters.AddWithValue("createdAt", DateTimeOffset.UtcNow);

                await cmd.ExecuteNonQueryAsync();
            })
        ]);

        var handler = new AggregateHandler<CounterState, CounterEvent>(
            CounterAggregate.Definition,
            new PostgresEventStore(_db.ConnectionString),
            "counters",
            processor);

        var result = await handler.ExecuteAsync(
            new CommandBatch(aggregateId, [new("Increment", Json(new { by = 1 }))]),
            CounterAggregate.DeserializeCommand,
            CounterAggregate.DeserializeEvent);

        Assert.True(result.IsSuccess);

        await using var conn = new Npgsql.NpgsqlConnection(_db.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM outbox WHERE stream_id = @streamId";
        cmd.Parameters.AddWithValue("streamId", streamId);
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task Sync_reaction_rolls_back_with_failed_append()
    {
        var store = new PostgresEventStore(_db.ConnectionString);
        var aggregateId = Guid.NewGuid().ToString();
        var streamId = $"counters/{aggregateId}";

        var processor = new EventProcessor([
            EventReaction.On<CounterIncremented>(async (e, tx) =>
            {
                var conn = ((Npgsql.NpgsqlTransaction)tx).Connection!;
                await using var cmd = new Npgsql.NpgsqlCommand(@"
                    INSERT INTO outbox (stream_id, event_type, payload, created_at)
                    VALUES (@streamId, @eventType, @payload::jsonb, @createdAt)", conn, (Npgsql.NpgsqlTransaction)tx);

                cmd.Parameters.AddWithValue("streamId",  streamId);
                cmd.Parameters.AddWithValue("eventType", nameof(CounterIncremented));
                cmd.Parameters.AddWithValue("payload",   JsonSerializer.Serialize(e, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }));
                cmd.Parameters.AddWithValue("createdAt", DateTimeOffset.UtcNow);

                await cmd.ExecuteNonQueryAsync();
            })
        ]);

        await store.AppendAsync(streamId, -1, [("CounterIncremented", "{\"by\":1}")]);

        var result = await store.AppendAsync(
            streamId, -1,
            [("CounterIncremented", "{\"by\":1}")],
            processor,
            [new CounterIncremented(1)]);

        Assert.True(result.IsError);

        await using var conn = new Npgsql.NpgsqlConnection(_db.ConnectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM outbox WHERE stream_id = @streamId";
        cmd.Parameters.AddWithValue("streamId", streamId);
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        Assert.Equal(0, count);
    }

    private static JsonElement Json(object value)
        => JsonSerializer.SerializeToElement(value, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
}

[Collection("Database")]
public class EdgeCaseTests : IAsyncLifetime
{
    private readonly TestDatabase _db = new();

    public async Task InitializeAsync() => await _db.InitializeAsync();
    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Reaction_that_throws_rolls_back_events()
    {
        var processor = new EventProcessor([
            EventReaction.On<CounterIncremented>((e, tx) =>
                throw new InvalidOperationException("reaction failed"))
        ]);

        var handler = new AggregateHandler<CounterState, CounterEvent>(
            CounterAggregate.Definition,
            new PostgresEventStore(_db.ConnectionString),
            "counters",
            processor);

        var aggregateId = Guid.NewGuid().ToString();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            handler.ExecuteAsync(
                new CommandBatch(aggregateId, [new("Increment", Json(new { by = 1 }))]),
                CounterAggregate.DeserializeCommand,
                CounterAggregate.DeserializeEvent));

        var store = new PostgresEventStore(_db.ConnectionString);
        var stored = await store.LoadAsync($"counters/{aggregateId}");
        Assert.Empty(stored);
    }

    [Fact]
    public async Task Appending_zero_events_is_a_noop()
    {
        var store = new PostgresEventStore(_db.ConnectionString);
        var streamId = $"counters/{Guid.NewGuid()}";

        var result = await store.AppendAsync(streamId, -1, []);

        Assert.True(result.IsSuccess);
        Assert.Empty(result.Value);

        var stored = await store.LoadAsync(streamId);
        Assert.Empty(stored);
    }

    [Fact]
    public async Task Unknown_command_type_throws_and_nothing_is_written()
    {
        var handler = new AggregateHandler<CounterState, CounterEvent>(
            CounterAggregate.Definition,
            new PostgresEventStore(_db.ConnectionString),
            "counters");

        var aggregateId = Guid.NewGuid().ToString();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            handler.ExecuteAsync(
                new CommandBatch(aggregateId, [
                    new("Increment",      Json(new { by = 1 })),
                    new("UnknownCommand", Json(new { }))
                ]),
                CounterAggregate.DeserializeCommand,
                CounterAggregate.DeserializeEvent));

        var store = new PostgresEventStore(_db.ConnectionString);
        var stored = await store.LoadAsync($"counters/{aggregateId}");
        Assert.Empty(stored);
    }

    [Fact]
    public async Task Malformed_payload_mid_batch_nothing_is_written()
    {
        var handler = new AggregateHandler<CounterState, CounterEvent>(
            CounterAggregate.Definition,
            new PostgresEventStore(_db.ConnectionString),
            "counters");

        var aggregateId = Guid.NewGuid().ToString();

        await Assert.ThrowsAnyAsync<Exception>(() =>
            handler.ExecuteAsync(
                new CommandBatch(aggregateId, [
                    new("Increment", Json(new { by = 1 })),
                    new("Decrement", JsonSerializer.SerializeToElement("not-an-object"))
                ]),
                CounterAggregate.DeserializeCommand,
                CounterAggregate.DeserializeEvent));

        var store = new PostgresEventStore(_db.ConnectionString);
        var stored = await store.LoadAsync($"counters/{aggregateId}");
        Assert.Empty(stored);
    }

    [Fact]
    public async Task Intra_batch_conflict_nothing_is_written()
    {
        var handler = new AggregateHandler<CounterState, CounterEvent>(
            CounterAggregate.Definition,
            new PostgresEventStore(_db.ConnectionString),
            "counters");

        var aggregateId = Guid.NewGuid().ToString();

        var result = await handler.ExecuteAsync(
            new CommandBatch(aggregateId, [
                new("Increment", Json(new { by = 5 })),
                new("Decrement", Json(new { by = 0 }))  // fails — non-positive
            ]),
            CounterAggregate.DeserializeCommand,
            CounterAggregate.DeserializeEvent);

        Assert.True(result.IsError);

        var store = new PostgresEventStore(_db.ConnectionString);
        var stored = await store.LoadAsync($"counters/{aggregateId}");
        Assert.Empty(stored);
    }

    [Fact]
    public async Task Concurrent_appends_to_same_stream_one_succeeds_one_fails()
    {
        var store = new PostgresEventStore(_db.ConnectionString);
        var streamId = $"counters/{Guid.NewGuid()}";

        var task1 = store.AppendAsync(streamId, -1, [("CounterIncremented", "{\"by\":1}")]);
        var task2 = store.AppendAsync(streamId, -1, [("CounterIncremented", "{\"by\":2}")]);

        var results = await Task.WhenAll(task1, task2);

        Assert.Equal(1, results.Count(r => r.IsSuccess));
        Assert.Equal(1, results.Count(r => r.IsError));

        var stored = await store.LoadAsync(streamId);
        Assert.Single(stored);
    }

    private static JsonElement Json(object value)
        => JsonSerializer.SerializeToElement(value, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
}