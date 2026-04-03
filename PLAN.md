# CommandFramework — Project Plan

## What it is

An opinionated event-sourced write API framework for ASP.NET Core. Aggregates are pure functions. Commands are batched. Events are stored in Postgres. The framework generates the HTTP endpoint from a dispatch function and an apply function.

## Architecture

```
POST /{name}/commands
        │
        ▼
MapAggregate          ← validates input, deserializes commands, maps result to HTTP
        │
        ▼
AggregateHandler      ← loads stream, folds state, dispatches commands, appends events
        │
        ├── IEventStore.LoadAsync
        ├── Aggregate.Fold
        ├── AggregateDefinition.Dispatch
        ├── IEventStore.AppendAsync
        └── EventProcessor.ProcessAsync  ← runs inside the append transaction

GET /{name}/{id}
        │
        ▼
MapAggregate          ← loads stream, folds state, returns current state
```

### Projects

| Project | Purpose |
|---|---|
| `CommandFramework.Core` | `Result<T>`, `Aggregate`, `AggregateDefinition`, `IEventStore`, `EventProcessor` |
| `CommandFramework.Postgres` | `PostgresEventStore` — Npgsql, append-only, optimistic concurrency |
| `CommandFramework.Http` | `AggregateHandler`, `MapAggregate`, DTOs |
| `CommandFramework.Sample` | Orders aggregate — reference implementation with read model |
| `CommandFramework.Tests` | Integration tests against real Postgres |

### Request shape

```json
POST /orders/commands
{
  "aggregateId": "optional — omit for creation commands",
  "commands": [
    { "type": "PlaceOrder", "payload": { "customerId": "cust-1", "items": ["widget"] } }
  ]
}
```

All commands in a batch target the same aggregate. State is loaded once, commands execute sequentially against it, events are appended in a single write.

### Error shape

```json
HTTP 422
{ "error": "Customer 'cust-999' does not exist." }
```

## Milestones

### M1 — Core types ✅
- `Result<T>` with implicit string error conversion
- `Dispatch<TState, TEvent>` and `Apply<TState, TEvent>` delegate types
- `Aggregate.Fold`
- `AggregateDefinition<TState, TEvent>`

### M2 — Postgres event store ✅
- `IEventStore` with `AppendAsync` and `LoadAsync`
- `PostgresEventStore` — Npgsql, optimistic concurrency via sequence number
- Migration: `events` table with JSONB payload

### M3 — HTTP layer ✅
- `AggregateHandler` — executes a command batch against an aggregate
- `MapAggregate` — mounts `POST /{name}/commands` and `GET /{name}/{id}`
- Deserialization boundary: `deserializeCommand` and `deserializeEvent` passed by consumer

### M4 — Sync reactions ✅
- `EventReaction` — typed handler for a specific event, runs inside the append transaction
- `EventProcessor` — runs registered reactions after append, within the same transaction
- `AggregateHandler` accepts optional `EventProcessor`
- Sample: `OrderSummariesReactions` builds a read-optimized `order_summaries` table
- Tests: reaction runs atomically, rolls back with a failed append

### M5 — Async reactions
- Outbox table already in place (`002_CreateOutbox.sql`)
- `AsyncEventReaction` — writes to outbox inside the transaction instead of executing inline
- Background worker — polls outbox, dispatches to registered handlers, marks processed
- Use case: reactions that could form cyclic dependencies between aggregates

### M6 — NuGet packaging
- Split into publishable packages
- Versioning strategy

## Test Coverage Plan

Ordered by dependency — each item assumes the previous ones pass.

### T1 — Reaction that throws rolls back cleanly ✅
Register a reaction that throws. Verify no events were written to the stream.

### T2 — Appending zero events ✅
`AppendAsync` with an empty event list should be a no-op — no error, no rows written.

### T3 — Unknown command type in batch ✅
`DeserializeCommand` receives an unrecognised type. Currently throws `InvalidOperationException` — test that the batch fails cleanly and nothing is written.

### T4 — Malformed payload that fails deserialization mid-batch ✅
Second command in a batch has a payload that fails deserialization. Verify the first command's events were not written.

### T5 — Batch with conflicting intra-batch commands ✅
`PlaceOrder` followed by a second `PlaceOrder` in the same batch. State threading should cause the second to fail. Verify nothing is written.

### T6 — Concurrency — two simultaneous appends to the same stream ✅
Fire two requests in parallel targeting the same aggregate. Assert one succeeds and one returns a concurrency error. Verify the stream has the correct number of events.

### T7 — Reaction dispatches to another aggregate inside the transaction
Wire two handlers together. A reaction on aggregate A dispatches a command to aggregate B inside the same transaction. Verify both streams are written atomically — if either fails, both roll back.

### T8 — Malformed JSON body (HTTP layer)
`POST /orders/commands` with invalid JSON. Should return 400.

### T9 — GET where fold returns null despite events existing
Aggregate where `Apply` always returns null even with events. `GET /{name}/{id}` should return 404.

### T10 — Concurrent writes to read model
Fire many parallel commands targeting different aggregates simultaneously. Verify `order_summaries` is consistent and has one row per aggregate afterward.