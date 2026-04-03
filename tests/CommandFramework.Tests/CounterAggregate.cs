using System.Text.Json;
using CommandFramework.Core;

namespace CommandFramework.Tests;

// ── State ─────────────────────────────────────────────────────────────────────

public record CounterState(int Value);

// ── Events ────────────────────────────────────────────────────────────────────

public abstract record CounterEvent;
public record CounterIncremented(int By) : CounterEvent;
public record CounterDecremented(int By) : CounterEvent;
public record CounterReset() : CounterEvent;

// ── Commands ──────────────────────────────────────────────────────────────────

public record Increment(int By);
public record Decrement(int By);
public record Reset();

// ── Aggregate ─────────────────────────────────────────────────────────────────

public static class CounterAggregate
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

    public static Result<IEnumerable<CounterEvent>> Dispatch(CounterState? state, object command)
        => command switch
        {
            Increment cmd when cmd.By <= 0 => "Increment must be positive.",
            Increment cmd => new CounterEvent[] { new CounterIncremented(cmd.By) },
            Decrement cmd when cmd.By <= 0 => "Decrement must be positive.",
            Decrement cmd => new CounterEvent[] { new CounterDecremented(cmd.By) },
            Reset => new CounterEvent[] { new CounterReset() },
            _ => throw new InvalidOperationException($"Unknown command '{command.GetType().Name}'.")
        };

    public static CounterState Apply(CounterState? state, CounterEvent e)
        => e switch
        {
            CounterIncremented evt => new CounterState((state?.Value ?? 0) + evt.By),
            CounterDecremented evt => new CounterState((state?.Value ?? 0) - evt.By),
            CounterReset => new CounterState(0),
            _ => throw new InvalidOperationException($"Unknown event '{e.GetType().Name}'.")
        };

    public static object DeserializeCommand(string type, JsonElement payload)
        => type switch
        {
            nameof(Increment) => payload.Deserialize<Increment>(JsonOptions)!,
            nameof(Decrement) => payload.Deserialize<Decrement>(JsonOptions)!,
            nameof(Reset) => new Reset(),
            _ => throw new InvalidOperationException($"Unknown command type '{type}'.")
        };

    public static CounterEvent DeserializeEvent(string type, string payload)
        => type switch
        {
            nameof(CounterIncremented) => JsonSerializer.Deserialize<CounterIncremented>(payload, JsonOptions)!,
            nameof(CounterDecremented) => JsonSerializer.Deserialize<CounterDecremented>(payload, JsonOptions)!,
            nameof(CounterReset) => new CounterReset(),
            _ => throw new InvalidOperationException($"Unknown event type '{type}'.")
        };

    public static readonly AggregateDefinition<CounterState, CounterEvent> Definition = new(
        Dispatch: Dispatch,
        Apply: Apply);
}