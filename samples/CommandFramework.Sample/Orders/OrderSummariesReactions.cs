using System.Text.Json;
using CommandFramework.Core;
using Dapper;
using Npgsql;
using static CommandFramework.Sample.Orders.OrderEvent;

namespace CommandFramework.Sample.Orders;

public static class OrderSummariesReactions
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public static readonly IReadOnlyList<EventReaction> All =
    [
        EventReaction.On<OrderPlaced>(async (e, tx) =>
        {
            var conn = ((NpgsqlTransaction)tx).Connection!;
            await conn.ExecuteAsync(@"
                INSERT INTO order_summaries (order_id, customer_id, status, items, placed_at, updated_at)
                VALUES (@orderId, @customerId, 'placed', @items::jsonb, @now, @now)
                ON CONFLICT (order_id) DO NOTHING",
                new { orderId = e.OrderId, customerId = e.CustomerId, items = JsonSerializer.Serialize(e.Items, JsonOptions), now = DateTimeOffset.UtcNow },
                tx);
        }),

        EventReaction.On<OrderCancelled>(async (e, tx) =>
        {
            var conn = ((NpgsqlTransaction)tx).Connection!;
            await conn.ExecuteAsync(@"
                UPDATE order_summaries
                SET status = 'cancelled', updated_at = @now
                WHERE order_id = @orderId",
                new { orderId = e.OrderId, now = DateTimeOffset.UtcNow },
                tx);
        }),

        EventReaction.On<OrderShipped>(async (e, tx) =>
        {
            var conn = ((NpgsqlTransaction)tx).Connection!;
            await conn.ExecuteAsync(@"
                UPDATE order_summaries
                SET status = 'shipped', updated_at = @now
                WHERE order_id = @orderId",
                new { orderId = e.OrderId, now = DateTimeOffset.UtcNow },
                tx);
        })
    ];
}
