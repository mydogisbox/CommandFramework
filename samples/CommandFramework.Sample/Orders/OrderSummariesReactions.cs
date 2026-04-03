using System.Data;
using System.Text.Json;
using CommandFramework.Core;
using Npgsql;

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
            await using var cmd = new NpgsqlCommand(@"
                INSERT INTO order_summaries (order_id, customer_id, status, items, placed_at, updated_at)
                VALUES (@orderId, @customerId, 'placed', @items::jsonb, @now, @now)
                ON CONFLICT (order_id) DO NOTHING", conn, (NpgsqlTransaction)tx);

            cmd.Parameters.AddWithValue("orderId",    e.OrderId);
            cmd.Parameters.AddWithValue("customerId", e.CustomerId);
            cmd.Parameters.AddWithValue("items",      JsonSerializer.Serialize(e.Items, JsonOptions));
            cmd.Parameters.AddWithValue("now",        DateTimeOffset.UtcNow);

            await cmd.ExecuteNonQueryAsync();
        }),

        EventReaction.On<OrderCancelled>(async (e, tx) =>
        {
            var conn = ((NpgsqlTransaction)tx).Connection!;
            await using var cmd = new NpgsqlCommand(@"
                UPDATE order_summaries
                SET status = 'cancelled', updated_at = @now
                WHERE order_id = @orderId", conn, (NpgsqlTransaction)tx);

            cmd.Parameters.AddWithValue("orderId", e.OrderId);
            cmd.Parameters.AddWithValue("now",     DateTimeOffset.UtcNow);

            await cmd.ExecuteNonQueryAsync();
        }),

        EventReaction.On<OrderShipped>(async (e, tx) =>
        {
            var conn = ((NpgsqlTransaction)tx).Connection!;
            await using var cmd = new NpgsqlCommand(@"
                UPDATE order_summaries
                SET status = 'shipped', updated_at = @now
                WHERE order_id = @orderId", conn, (NpgsqlTransaction)tx);

            cmd.Parameters.AddWithValue("orderId", e.OrderId);
            cmd.Parameters.AddWithValue("now",     DateTimeOffset.UtcNow);

            await cmd.ExecuteNonQueryAsync();
        })
    ];
}