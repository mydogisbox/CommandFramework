using System.Text.Json;

namespace CommandFramework.Http;

public static class ReflectionDeserializer
{
    private static readonly JsonSerializerOptions DefaultOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

    public static Func<string, string, TEvent> ForEvents<TEvent>(JsonSerializerOptions? options = null)
        where TEvent : class
    {
        var opts = options ?? DefaultOptions;
        var lookup = typeof(TEvent).GetNestedTypes().ToDictionary(t => t.Name);

        return (type, payload) =>
        {
            if (!lookup.TryGetValue(type, out var eventType))
                throw new InvalidOperationException($"Unknown event type '{type}'.");
            return (TEvent)JsonSerializer.Deserialize(payload, eventType, opts)!;
        };
    }

    public static Func<string, JsonElement, object> ForCommands<TCommands>(JsonSerializerOptions? options = null)
    {
        var opts = options ?? DefaultOptions;
        var lookup = typeof(TCommands).GetNestedTypes().ToDictionary(t => t.Name);

        return (type, payload) =>
        {
            if (!lookup.TryGetValue(type, out var commandType))
                throw new InvalidOperationException($"Unknown command type '{type}'.");
            return payload.Deserialize(commandType, opts)!;
        };
    }
}
