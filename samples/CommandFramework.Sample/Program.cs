using CommandFramework.Core;
using CommandFramework.Http;
using CommandFramework.Postgres;
using CommandFramework.Sample.Orders;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("Postgres")
    ?? throw new InvalidOperationException("Connection string 'Postgres' is not configured.");

var app = builder.Build();

var eventStore = new PostgresEventStore(connectionString);

var eventProcessor = new EventProcessor(OrderSummariesReactions.All);

var handler = new AggregateHandler<OrderState, OrderEvent>(
    OrderAggregate.Definition,
    eventStore,
    "orders",
    eventProcessor);

app.MapAggregate(
    name: "orders",
    handler: handler,
    deserializeCommand: OrderAggregate.DeserializeCommand,
    deserializeEvent: OrderAggregate.DeserializeEvent);

app.MapGet("/orders", async () =>
{
    await using var conn = new Npgsql.NpgsqlConnection(connectionString);
    await conn.OpenAsync();

    await using var cmd = conn.CreateCommand();
    cmd.CommandText = "SELECT order_id, customer_id, status, items, placed_at, updated_at FROM order_summaries ORDER BY placed_at DESC";

    var summaries = new List<object>();
    await using var reader = await cmd.ExecuteReaderAsync();

    while (await reader.ReadAsync())
    {
        summaries.Add(new
        {
            orderId = reader.GetString(0),
            customerId = reader.GetString(1),
            status = reader.GetString(2),
            items = System.Text.Json.JsonSerializer.Deserialize<List<string>>(reader.GetString(3)),
            placedAt = reader.GetFieldValue<DateTimeOffset>(4),
            updatedAt = reader.GetFieldValue<DateTimeOffset>(5)
        });
    }

    return Results.Ok(summaries);
});

app.Run();

public partial class Program { }