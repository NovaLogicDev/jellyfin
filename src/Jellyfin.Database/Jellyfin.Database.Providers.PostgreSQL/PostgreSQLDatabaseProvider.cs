using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Jellyfin.Database.Implementations;
using Jellyfin.Database.Implementations.DbConfiguration;
using MediaBrowser.Common.Configuration;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace Jellyfin.Database.Providers.PostgreSQL;

/// <summary>
/// Configures jellyfin to use a PostgreSQL database.
/// </summary>
[JellyfinDatabaseProviderKey("Jellyfin-PostgreSQL")]
public sealed class PostgreSQLDatabaseProvider : IJellyfinDatabaseProvider
{
    private readonly ILogger<PostgreSQLDatabaseProvider> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgreSQLDatabaseProvider"/> class.
    /// </summary>
    /// <param name="logger">A logger.</param>
    public PostgreSQLDatabaseProvider(ILogger<PostgreSQLDatabaseProvider> logger)
    {
        _logger = logger;
    }

    /// <inheritdoc/>
    public IDbContextFactory<JellyfinDbContext>? DbContextFactory { get; set; }

    /// <inheritdoc/>
    public void Initialise(DbContextOptionsBuilder options, DatabaseConfigurationOptions databaseConfiguration)
    {
        static T? GetOption<T>(ICollection<CustomDatabaseOption>? options, string key, Func<string, T> converter, Func<T>? defaultValue = null)
        {
            if (options is null)
            {
                return defaultValue is not null ? defaultValue() : default;
            }

            var value = options.FirstOrDefault(e => e.Key.Equals(key, StringComparison.OrdinalIgnoreCase));
            if (value is null)
            {
                return defaultValue is not null ? defaultValue() : default;
            }

            return converter(value.Value);
        }

        var customOptions = databaseConfiguration.CustomProviderOptions?.Options;

        var connectionStringBuilder = new NpgsqlConnectionStringBuilder();
        // Basic connection parameters - assuming they are passed via custom options or env vars for now.
        // In a real scenario, we might want to map these from specific config fields.
        connectionStringBuilder.Host = GetOption(customOptions, "host", s => s, () => "localhost");
        connectionStringBuilder.Port = GetOption(customOptions, "port", int.Parse, () => 5432);
        connectionStringBuilder.Database = GetOption(customOptions, "database", s => s, () => "jellyfin");
        connectionStringBuilder.Username = GetOption(customOptions, "username", s => s, () => "jellyfin");
        connectionStringBuilder.Password = GetOption(customOptions, "password", s => s, () => "jellyfin");

        var connectionString = connectionStringBuilder.ToString();

        _logger.LogInformation("PostgreSQL connection string: {ConnectionString}", connectionStringBuilder.ToString()); // Be careful logging passwords in prod

        options.UseNpgsql(
            connectionString,
            npgsqlOptions => npgsqlOptions.MigrationsAssembly(GetType().Assembly));
    }

    /// <inheritdoc/>
    public Task RunScheduledOptimisation(CancellationToken cancellationToken)
    {
        // PostgreSQL handles vacuuming automatically usually, but we could trigger it here if needed.
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public void OnModelCreating(ModelBuilder modelBuilder)
    {
        // PostgreSQL specific model configuration if needed
    }

    /// <inheritdoc/>
    public Task RunShutdownTask(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public void ConfigureConventions(ModelConfigurationBuilder configurationBuilder)
    {
        // Configure conventions if needed
    }

    /// <inheritdoc />
    public Task<string> MigrationBackupFast(CancellationToken cancellationToken)
    {
        throw new NotSupportedException("Fast backup is not supported for PostgreSQL provider.");
    }

    /// <inheritdoc />
    public Task RestoreBackupFast(string key, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("Fast restore is not supported for PostgreSQL provider.");
    }

    /// <inheritdoc />
    public Task DeleteBackup(string key)
    {
        throw new NotSupportedException("Backup deletion is not supported for PostgreSQL provider.");
    }

    /// <inheritdoc/>
    public async Task PurgeDatabase(JellyfinDbContext dbContext, IEnumerable<string>? tableNames)
    {
        ArgumentNullException.ThrowIfNull(tableNames);

        var deleteQueries = new List<string>();
        foreach (var tableName in tableNames)
        {
            deleteQueries.Add($"TRUNCATE TABLE \"{tableName}\" CASCADE;");
        }

        var deleteAllQuery = string.Join('\n', deleteQueries);

        await dbContext.Database.ExecuteSqlRawAsync(deleteAllQuery).ConfigureAwait(false);
    }
}
