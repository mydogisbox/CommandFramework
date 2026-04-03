using System.Text.Json;
using CommandFramework.Core;

namespace CommandFramework.Sample.Orders;

// ── State ─────────────────────────────────────────────────────────────────────

public record OrderState(
    string Id,
    string CustomerId,
    string Status,
    List<string> Items);

// ── Events ────────────────────────────────────────────────────────────────────

public abstract record OrderEvent;
public record OrderPlaced(string OrderId, string CustomerId, List<string> Items) : OrderEvent;
public record OrderCancelled(string OrderId, string Reason) : OrderEvent;
public record OrderShipped(string OrderId, string TrackingNumber) : OrderEvent;

// ── Commands ──────────────────────────────────────────────────────────────────

public record PlaceOrder(string CustomerId, List<string> Items);
public record CancelOrder(string AggregateId, string Reason);
public record ShipOrder(string AggregateId, string TrackingNumber);

// ── Aggregate ─────────────────────────────────────────────────────────────────

public static class OrderAggregate
{
    private static readonly HashSet<string> KnownCustomers = ["cust-1", "cust-2", "cust-3"];

    public static Result<IEnumerable<OrderEvent>> Handle(OrderState? state, PlaceOrder cmd)
    {
        if (state != null)
            return "Order has already been placed.";

        if (!KnownCustomers.Contains(cmd.CustomerId))
            return $"Customer '{cmd.CustomerId}' does not exist.";

        if (cmd.Items.Count == 0)
            return "An order must contain at least one item.";

        OrderEvent[] events = [new OrderPlaced(Guid.NewGuid().ToString(), cmd.CustomerId, cmd.Items)];
        return events;
    }

    public static Result<IEnumerable<OrderEvent>> Handle(OrderState state, CancelOrder cmd)
    {
        if (state.Status == "cancelled")
            return $"Order '{state.Id}' has already been cancelled.";

        if (state.Status == "shipped")
            return $"Order '{state.Id}' has already been shipped and cannot be cancelled.";

        OrderEvent[] events = [new OrderCancelled(state.Id, cmd.Reason)];
        return events;
    }

    public static Result<IEnumerable<OrderEvent>> Handle(OrderState state, ShipOrder cmd)
    {
        if (state.Status == "cancelled")
            return $"Order '{state.Id}' has been cancelled and cannot be shipped.";

        if (state.Status == "shipped")
            return $"Order '{state.Id}' has already been shipped.";

        OrderEvent[] events = [new OrderShipped(state.Id, cmd.TrackingNumber)];
        return events;
    }

    public static OrderState Apply(OrderState? state, OrderEvent e)
        => e switch
        {
            OrderPlaced evt => new OrderState(evt.OrderId, evt.CustomerId, "placed", evt.Items),
            OrderCancelled _ => state! with { Status = "cancelled" },
            OrderShipped _ => state! with { Status = "shipped" },
            _ => throw new InvalidOperationException($"Unknown event: {e.GetType().Name}")
        };

    public static Result<IEnumerable<OrderEvent>> Dispatch(OrderState? state, object command)
        => command switch
        {
            PlaceOrder cmd => Handle(state, cmd),
            CancelOrder cmd => Handle(state!, cmd),
            ShipOrder cmd => Handle(state!, cmd),
            _ => $"Unknown command '{command.GetType().Name}'."
        };

    // ── Boundary functions ────────────────────────────────────────────────────

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

    public static object DeserializeCommand(string type, JsonElement payload)
        => type switch
        {
            nameof(PlaceOrder) => payload.Deserialize<PlaceOrder>(JsonOptions)!,
            nameof(CancelOrder) => payload.Deserialize<CancelOrder>(JsonOptions)!,
            nameof(ShipOrder) => payload.Deserialize<ShipOrder>(JsonOptions)!,
            _ => throw new InvalidOperationException($"Unknown command type '{type}'.")
        };

    public static OrderEvent DeserializeEvent(string type, string payload)
        => type switch
        {
            nameof(OrderPlaced) => JsonSerializer.Deserialize<OrderPlaced>(payload, JsonOptions)!,
            nameof(OrderCancelled) => JsonSerializer.Deserialize<OrderCancelled>(payload, JsonOptions)!,
            nameof(OrderShipped) => JsonSerializer.Deserialize<OrderShipped>(payload, JsonOptions)!,
            _ => throw new InvalidOperationException($"Unknown event type '{type}'.")
        };

    public static readonly AggregateDefinition<OrderState, OrderEvent> Definition = new(
        Dispatch: Dispatch,
        Apply: Apply);
}