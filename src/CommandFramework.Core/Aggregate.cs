using System.Text.Json;

namespace CommandFramework.Core;

/// <summary>
/// Routes a deserialized command to the correct handler.
/// State is null for creation commands.
/// </summary>
public delegate Result<IEnumerable<TEvent>> Dispatch<TState, TEvent>(TState? state, object command)
    where TState : class
    where TEvent : class;

/// <summary>
/// Folds a single event into state.
/// State is null when applying the first event.
/// </summary>
public delegate TState Apply<TState, TEvent>(TState? state, TEvent @event)
    where TState : class
    where TEvent : class;

public static class Aggregate
{
    /// <summary>
    /// Replays a sequence of events to rebuild aggregate state from scratch.
    /// Returns null if the sequence is empty.
    /// </summary>
    public static TState? Fold<TState, TEvent>(
        IEnumerable<TEvent> events,
        Apply<TState, TEvent> apply)
        where TState : class
        where TEvent : class
    {
        TState? state = null;
        foreach (var e in events)
            state = apply(state, e);
        return state;
    }
}
