using System.Net;
using System.Text;
using System.Text.Json;
using CommandFramework.Core;
using CommandFramework.Http;
using CommandFramework.Postgres;
using Dapper;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
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
                var conn = ((NpgsqlTransaction)tx).Connection!;
                await conn.ExecuteAsync(@"
                    INSERT INTO outbox (stream_id, event_type, payload, created_at)
                    VALUES (@streamId, @eventType, @payload::jsonb, @createdAt)",
                    new { streamId, eventType = nameof(CounterIncremented), payload = JsonSerializer.Serialize(e, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }), createdAt = DateTimeOffset.UtcNow },
                    tx);
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

        await using var conn = new NpgsqlConnection(_db.ConnectionString);
        await conn.OpenAsync();
        var count = await conn.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM outbox WHERE stream_id = @streamId", new { streamId });
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
                var conn = ((NpgsqlTransaction)tx).Connection!;
                await conn.ExecuteAsync(@"
                    INSERT INTO outbox (stream_id, event_type, payload, created_at)
                    VALUES (@streamId, @eventType, @payload::jsonb, @createdAt)",
                    new { streamId, eventType = nameof(CounterIncremented), payload = JsonSerializer.Serialize(e, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase }), createdAt = DateTimeOffset.UtcNow },
                    tx);
            })
        ]);

        await store.AppendAsync(streamId, -1, [("CounterIncremented", "{\"by\":1}")]);

        var result = await store.AppendAsync(
            streamId, -1,
            [("CounterIncremented", "{\"by\":1}")],
            processor,
            [new CounterIncremented(1)]);

        Assert.True(result.IsError);

        await using var conn = new NpgsqlConnection(_db.ConnectionString);
        await conn.OpenAsync();
        var count = await conn.ExecuteScalarAsync<long>("SELECT COUNT(*) FROM outbox WHERE stream_id = @streamId", new { streamId });
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

    [Fact]
    public async Task Cross_aggregate_reaction_writes_both_streams_atomically()
    {
        var streamBId = $"counters/{Guid.NewGuid()}";

        var processor = new EventProcessor([
            EventReaction.On<CounterIncremented>(async (e, tx) =>
            {
                var npgsqlTx = (NpgsqlTransaction)tx;
                await npgsqlTx.Connection!.ExecuteAsync(@"
                    INSERT INTO events (stream_id, sequence, event_type, payload, occurred_at)
                    VALUES (@streamId, 0, @eventType, @payload::jsonb, @occurredAt)",
                    new { streamId = streamBId, eventType = nameof(CounterIncremented), payload = $"{{\"by\":{e.By}}}", occurredAt = DateTimeOffset.UtcNow },
                    npgsqlTx);
            })
        ]);

        var handler = new AggregateHandler<CounterState, CounterEvent>(
            CounterAggregate.Definition,
            new PostgresEventStore(_db.ConnectionString),
            "counters",
            processor);

        var aggregateIdA = Guid.NewGuid().ToString();

        var result = await handler.ExecuteAsync(
            new CommandBatch(aggregateIdA, [new("Increment", Json(new { by = 5 }))]),
            CounterAggregate.DeserializeCommand,
            CounterAggregate.DeserializeEvent);

        Assert.True(result.IsSuccess);

        var store = new PostgresEventStore(_db.ConnectionString);
        var streamAEvents = await store.LoadAsync($"counters/{aggregateIdA}");
        var streamBEvents = await store.LoadAsync(streamBId);

        Assert.Single(streamAEvents);
        Assert.Single(streamBEvents);
    }

    [Fact]
    public async Task Cross_aggregate_reaction_rolls_back_both_streams_on_failure()
    {
        var streamBId = $"counters/{Guid.NewGuid()}";

        var processor = new EventProcessor([
            EventReaction.On<CounterIncremented>(async (e, tx) =>
            {
                var npgsqlTx = (NpgsqlTransaction)tx;
                await npgsqlTx.Connection!.ExecuteAsync(@"
                    INSERT INTO events (stream_id, sequence, event_type, payload, occurred_at)
                    VALUES (@streamId, 0, @eventType, @payload::jsonb, @occurredAt)",
                    new { streamId = streamBId, eventType = nameof(CounterIncremented), payload = $"{{\"by\":{e.By}}}", occurredAt = DateTimeOffset.UtcNow },
                    npgsqlTx);
                throw new InvalidOperationException("downstream write failed");
            })
        ]);

        var handler = new AggregateHandler<CounterState, CounterEvent>(
            CounterAggregate.Definition,
            new PostgresEventStore(_db.ConnectionString),
            "counters",
            processor);

        var aggregateIdA = Guid.NewGuid().ToString();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            handler.ExecuteAsync(
                new CommandBatch(aggregateIdA, [new("Increment", Json(new { by = 5 }))]),
                CounterAggregate.DeserializeCommand,
                CounterAggregate.DeserializeEvent));

        var store = new PostgresEventStore(_db.ConnectionString);
        var streamAEvents = await store.LoadAsync($"counters/{aggregateIdA}");
        var streamBEvents = await store.LoadAsync(streamBId);

        Assert.Empty(streamAEvents);
        Assert.Empty(streamBEvents);
    }

    private static JsonElement Json(object value)
        => JsonSerializer.SerializeToElement(value, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
}

[Collection("Database")]
public class HttpLayerTests : IAsyncLifetime
{
    public async Task InitializeAsync() => await _db.InitializeAsync();
    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Malformed_json_body_returns_400()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddSingleton<IEventStore>(new StubEventStore());

        var handler = new AggregateHandler<CounterState, CounterEvent>(
            CounterAggregate.Definition,
            new StubEventStore(),
            "counters");

        var app = builder.Build();
        app.MapAggregate("counters", handler,
            CounterAggregate.DeserializeCommand,
            CounterAggregate.DeserializeEvent);

        await app.StartAsync();

        var client = app.GetTestClient();
        var response = await client.PostAsync("/counters/commands",
            new StringContent("this is not json", Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);

        await app.StopAsync();
    }

    [Fact]
    public async Task Get_returns_404_when_fold_produces_null()
    {
        var aggregateId = Guid.NewGuid().ToString();
        var streamId = $"nullcounters/{aggregateId}";

        // Seed one event directly so the stream exists
        var store = new PostgresEventStore(_db.ConnectionString);
        await store.AppendAsync(streamId, -1, [("CounterIncremented", "{\"by\":1}")]);

        // Apply always returns null — simulates a tombstoned aggregate
        var nullApply = new AggregateDefinition<CounterState, CounterEvent>(
            Dispatch: CounterAggregate.Dispatch,
            Apply: (_, _) => null!);

        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddSingleton<IEventStore>(store);

        var handler = new AggregateHandler<CounterState, CounterEvent>(
            nullApply, store, "nullcounters");

        var app = builder.Build();
        app.MapAggregate("nullcounters", handler,
            CounterAggregate.DeserializeCommand,
            CounterAggregate.DeserializeEvent);

        await app.StartAsync();

        var client = app.GetTestClient();
        var response = await client.GetAsync($"/nullcounters/{aggregateId}");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);

        await app.StopAsync();
    }

    private readonly TestDatabase _db = new();

    private sealed class StubEventStore : IEventStore
    {
        public Task<Result<IReadOnlyList<StoredEvent>>> AppendAsync(
            string streamId, int expectedSequence,
            IEnumerable<(string eventType, string payload)> events,
            EventProcessor? processor = null,
            IEnumerable<object>? domainEvents = null)
            => throw new NotImplementedException();

        public Task<IReadOnlyList<StoredEvent>> LoadAsync(string streamId)
            => throw new NotImplementedException();
    }
}