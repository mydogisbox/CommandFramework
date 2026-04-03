using System.Text.Json;
using CommandFramework.Core;
using CommandFramework.Http;
using CommandFramework.Postgres;
using Dapper;
using Npgsql;
using Xunit;

namespace CommandFramework.Tests;

[Collection("Database")]
public class ReadModelTests : IAsyncLifetime
{
    private readonly TestDatabase _db = new();

    public async Task InitializeAsync() => await _db.InitializeAsync();
    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task Concurrent_commands_each_produce_one_read_model_entry()
    {
        const int count = 20;

        var store = new PostgresEventStore(_db.ConnectionString);
        var processor = new EventProcessor([
            EventReaction.On<CounterIncremented>(async (e, tx) =>
            {
                var npgsqlTx = (NpgsqlTransaction)tx;
                await npgsqlTx.Connection!.ExecuteAsync(@"
                    INSERT INTO outbox (stream_id, event_type, payload, created_at)
                    VALUES (@streamId, @eventType, @payload::jsonb, @createdAt)",
                    new
                    {
                        streamId = Guid.NewGuid().ToString(),
                        eventType = nameof(CounterIncremented),
                        payload = $"{{\"by\":{e.By}}}",
                        createdAt = DateTimeOffset.UtcNow
                    },
                    npgsqlTx);
            })
        ]);

        var handler = new AggregateHandler<CounterState, CounterEvent>(
            CounterAggregate.Definition,
            store,
            "counters",
            processor);

        var tasks = Enumerable.Range(0, count).Select(_ =>
            handler.ExecuteAsync(
                new CommandBatch(null, [new("Increment", Json(new { by = 1 }))]),
                CounterAggregate.DeserializeCommand,
                CounterAggregate.DeserializeEvent));

        var results = await Task.WhenAll(tasks);

        Assert.All(results, r => Assert.True(r.IsSuccess));

        await using var conn = new NpgsqlConnection(_db.ConnectionString);
        await conn.OpenAsync();
        var entryCount = await conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM outbox");
        Assert.Equal(count, entryCount);
    }

    private static JsonElement Json(object value)
        => JsonSerializer.SerializeToElement(value, new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        });
}
