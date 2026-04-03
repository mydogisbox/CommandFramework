using System.Text.Json;
using CommandFramework.Core;

namespace CommandFramework.Http;

public class AggregateHandler<TState, TEvent>
    where TState : class
    where TEvent : class
{
    private readonly AggregateDefinition<TState, TEvent> _definition;
    private readonly IEventStore _eventStore;
    private readonly string _aggregateName;
    private readonly EventProcessor? _eventProcessor;

    public Apply<TState, TEvent> Apply => _definition.Apply;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

    public AggregateHandler(
        AggregateDefinition<TState, TEvent> definition,
        IEventStore eventStore,
        string aggregateName,
        EventProcessor? eventProcessor = null)
    {
        _definition = definition;
        _eventStore = eventStore;
        _aggregateName = aggregateName;
        _eventProcessor = eventProcessor;
    }

    public async Task<Result<IReadOnlyList<CommandSuccess>>> ExecuteAsync(
        CommandBatch batch,
        Func<string, JsonElement, object> deserializeCommand,
        Func<string, string, TEvent> deserializeEvent)
    {
        var aggregateId = batch.AggregateId ?? Guid.NewGuid().ToString();
        var streamId = $"{_aggregateName}/{aggregateId}";

        var stored = await _eventStore.LoadAsync(streamId);
        var currentSequence = stored.Count == 0 ? -1 : stored[^1].Sequence;
        var state = stored.Count == 0
            ? null
            : Aggregate.Fold(
                stored.Select(e => deserializeEvent(e.EventType, e.Payload)),
                _definition.Apply);

        var results = new List<CommandSuccess>();
        var pendingEvents = new List<(string type, string payload)>();
        var domainEvents = new List<object>();

        for (var i = 0; i < batch.Commands.Count; i++)
        {
            var envelope = batch.Commands[i];
            var command = deserializeCommand(envelope.Type, envelope.Payload);

            var result = _definition.Dispatch(state, command);
            if (result.IsError)
                return $"Command {i} ('{envelope.Type}') failed: {result.Error}";

            var serialized = result.Value
                .Select(e => (
                    type: e.GetType().Name,
                    payload: JsonSerializer.Serialize(e, e.GetType(), JsonOptions)))
                .ToList();

            foreach (var e in result.Value)
            {
                state = _definition.Apply(state, e);
                domainEvents.Add(e);
            }

            pendingEvents.AddRange(serialized);

            results.Add(new CommandSuccess(
                Index: i,
                AggregateId: aggregateId,
                Events: serialized.Select(e => e.type).ToList()));
        }

        var appendResult = await _eventStore.AppendAsync(
            streamId,
            currentSequence,
            pendingEvents,
            _eventProcessor,
            domainEvents);

        if (appendResult.IsError)
            return appendResult.Error;

        return Result<IReadOnlyList<CommandSuccess>>.Ok(results);
    }
}