using System.Data;

namespace CommandFramework.Core;

public record StoredEvent(
    string StreamId,
    int Sequence,
    string EventType,
    string Payload,
    DateTimeOffset OccurredAt);

public interface IEventStore
{
    /// <summary>
    /// Appends events and runs the processor inside the same transaction.
    /// expectedSequence is the last known sequence number — use -1 for a new stream.
    /// </summary>
    Task<Result<IReadOnlyList<StoredEvent>>> AppendAsync(
        string streamId,
        int expectedSequence,
        IEnumerable<(string eventType, string payload)> events,
        EventProcessor? processor = null,
        IEnumerable<object>? domainEvents = null);

    Task<IReadOnlyList<StoredEvent>> LoadAsync(string streamId);
}