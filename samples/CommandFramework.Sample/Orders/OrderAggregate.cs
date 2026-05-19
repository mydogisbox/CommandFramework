using CommandFramework.Core;
using static CommandFramework.Sample.Orders.OrderEvent;
using static CommandFramework.Sample.Orders.OrderCommands;

namespace CommandFramework.Sample.Orders;

// ── State ─────────────────────────────────────────────────────────────────────

public record OrderState(
    string Id,
    string CustomerId,
    string Status,
    List<string> Items);

// ── Events ────────────────────────────────────────────────────────────────────

public abstract record OrderEvent
{
    public record OrderPlaced(string OrderId, string CustomerId, List<string> Items) : OrderEvent;
    public record OrderCancelled(string OrderId, string Reason) : OrderEvent;
    public record OrderShipped(string OrderId, string TrackingNumber) : OrderEvent;
}

// ── Commands ──────────────────────────────────────────────────────────────────

public abstract record OrderCommands
{
    public record PlaceOrder(string CustomerId, List<string> Items) : OrderCommands;
    public record CancelOrder(string AggregateId, string Reason) : OrderCommands;
    public record ShipOrder(string AggregateId, string TrackingNumber) : OrderCommands;
}

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

    public static readonly AggregateDefinition<OrderState, OrderEvent> Definition = new(
        Dispatch: Dispatch,
        Apply: Apply);
}