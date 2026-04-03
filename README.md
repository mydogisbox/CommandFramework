# CommandFramework

## Prerequisites

- .NET 8 SDK
- Docker (or Podman)

## Setup

### 1. Start Postgres

```bash
docker run -d \
  --name cf-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=commandframework_test \
  -p 5432:5432 \
  postgres:16-alpine
```

### 2. Create databases

```bash
docker exec -i cf-postgres psql -U postgres -c "CREATE DATABASE commandframework;"
docker exec -i cf-postgres psql -U postgres -c "CREATE DATABASE commandframework_test;"
```

### 3. Run migrations

If you need to reset existing tables first:
```bash
docker exec -i cf-postgres psql -U postgres -d commandframework \
  -c "DROP TABLE IF EXISTS order_summaries, outbox, events;"
docker exec -i cf-postgres psql -U postgres -d commandframework_test \
  -c "DROP TABLE IF EXISTS outbox, events;"
```

Sample database:
```bash
for migration in 001_CreateEvents 002_CreateOutbox 003_CreateOrderSummaries; do
  docker exec -i cf-postgres psql -U postgres -d commandframework \
    < samples/CommandFramework.Sample/Migrations/$migration.sql
done
```

Test database:
```bash
for migration in 001_CreateEvents 002_CreateOutbox; do
  docker exec -i cf-postgres psql -U postgres -d commandframework_test \
    < src/CommandFramework.Postgres/Migrations/$migration.sql
done
```

### 3. Build

```bash
dotnet build CommandFramework.slnx
```

### 4. Run tests

```bash
dotnet test CommandFramework.slnx
```

### 5. Run the sample

```bash
dotnet run --project samples/CommandFramework.Sample
```

### 6. View events

```bash
dotnet fsi scripts/view-events.fsx
```