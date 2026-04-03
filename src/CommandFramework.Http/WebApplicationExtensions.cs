using System.Text.Json;
using CommandFramework.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;

namespace CommandFramework.Http;

public static class WebApplicationExtensions
{
    public static IEndpointRouteBuilder MapAggregate<TState, TEvent>(
        this IEndpointRouteBuilder app,
        string name,
        AggregateHandler<TState, TEvent> handler,
        Func<string, JsonElement, object> deserializeCommand,
        Func<string, string, TEvent> deserializeEvent)
        where TState : class
        where TEvent : class
    {
        app.MapPost($"/{name}/commands", async (CommandBatch batch) =>
        {
            if (batch.Commands is null || batch.Commands.Count == 0)
                return Results.BadRequest("Batch must contain at least one command.");

            var result = await handler.ExecuteAsync(batch, deserializeCommand, deserializeEvent);

            return result.IsSuccess
                ? Results.Ok(result.Value)
                : Results.UnprocessableEntity(new CommandFailure(result.Error));
        });

        app.MapGet($"/{name}/{{aggregateId}}", async (string aggregateId, IEventStore eventStore) =>
        {
            var stored = await eventStore.LoadAsync($"{name}/{aggregateId}");

            if (stored.Count == 0)
                return Results.NotFound();

            var state = Aggregate.Fold(
                stored.Select(e => deserializeEvent(e.EventType, e.Payload)),
                handler.Apply);

            return Results.Ok(state);
        });

        return app;
    }
}
