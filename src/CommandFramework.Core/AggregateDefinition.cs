using System.Text.Json;

namespace CommandFramework.Core;

/// <summary>
/// Groups the two pure functions that define an aggregate.
/// No serialization, no HTTP knowledge.
/// </summary>
public record AggregateDefinition<TState, TEvent>(
    Dispatch<TState, TEvent> Dispatch,
    Apply<TState, TEvent> Apply)
    where TState : class
    where TEvent : class;
