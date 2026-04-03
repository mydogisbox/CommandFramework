using System.Text.Json;

namespace CommandFramework.Http;

/// <summary>
/// A single command within a batch request.
/// </summary>
public record CommandEnvelope(
    string Type,
    JsonElement Payload);

/// <summary>
/// The request body for POST /{aggregate}/commands.
/// All commands in the batch target the same aggregate instance.
/// AggregateId is null for creation batches.
/// </summary>
public record CommandBatch(
    string? AggregateId,
    List<CommandEnvelope> Commands);

/// <summary>
/// The result of a single successfully executed command.
/// </summary>
public record CommandSuccess(
    int Index,
    string AggregateId,
    IReadOnlyList<string> Events);

/// <summary>
/// Returned when any command in the batch fails.
/// The entire batch is rolled back.
/// </summary>
public record CommandFailure(string Error);