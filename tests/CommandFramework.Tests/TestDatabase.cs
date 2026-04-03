using Microsoft.Extensions.Configuration;
using Npgsql;
using Xunit;

namespace CommandFramework.Tests;

public class TestDatabase : IAsyncLifetime
{
    public string ConnectionString { get; } = new ConfigurationBuilder()
        .AddJsonFile("appsettings.json")
        .Build()
        .GetConnectionString("Postgres")!;

    public async Task InitializeAsync()
    {
        await using var conn = new NpgsqlConnection(ConnectionString);
        await conn.OpenAsync();

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "TRUNCATE TABLE events, outbox;";
        await cmd.ExecuteNonQueryAsync();
    }

    public Task DisposeAsync() => Task.CompletedTask;
}