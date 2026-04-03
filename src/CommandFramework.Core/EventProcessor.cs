using System.Data;

namespace CommandFramework.Core;

/// <summary>
/// A reaction to a specific event type, run inside the append transaction.
/// </summary>
public record EventReaction(
    Type EventType,
    Func<object, IDbTransaction, Task> Handle)
{
    public static EventReaction On<TEvent>(Func<TEvent, IDbTransaction, Task> handle)
        where TEvent : class
        => new(
            typeof(TEvent),
            (e, tx) => handle((TEvent)e, tx));
}

/// <summary>
/// Runs registered reactions for events produced by an aggregate, inside the append transaction.
/// </summary>
public class EventProcessor(IReadOnlyList<EventReaction> reactions)
{
    public async Task ProcessAsync(IEnumerable<object> events, IDbTransaction transaction)
    {
        foreach (var e in events)
        {
            var eventType = e.GetType();
            foreach (var reaction in reactions.Where(r => r.EventType.IsAssignableFrom(eventType)))
                await reaction.Handle(e, transaction);
        }
    }
}