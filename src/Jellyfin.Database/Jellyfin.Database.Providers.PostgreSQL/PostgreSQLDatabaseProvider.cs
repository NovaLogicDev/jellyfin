using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Jellyfin.Database.Implementations;
using Jellyfin.Database.Implementations.DbConfiguration;
using MediaBrowser.Common.Configuration;
using MediaBrowser.Model.Configuration;
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
    private readonly IApplicationPaths _appPaths;
    private ConnectionInfo _connectionInfo = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="PostgreSQLDatabaseProvider"/> class.
    /// </summary>
    /// <param name="logger">A logger.</param>
    /// <param name="appPaths">The application paths.</param>
    public PostgreSQLDatabaseProvider(ILogger<PostgreSQLDatabaseProvider> logger, IApplicationPaths appPaths)
    {
        _logger = logger;
        _appPaths = appPaths;
    }

    /// <inheritdoc/>
    public IDbContextFactory<JellyfinDbContext>? DbContextFactory { get; set; }

    /// <inheritdoc/>
    public void Initialise(DbContextOptionsBuilder options, DatabaseConfigurationOptions databaseConfiguration)
    {
        var customOptions = databaseConfiguration.CustomProviderOptions?.Options;

        _connectionInfo = new ConnectionInfo
        {
            Host = GetOption(customOptions, "host", s => s, () => "localhost"),
            Port = GetOption(customOptions, "port", int.Parse, () => 5432),
            Database = GetOption(customOptions, "database", s => s, () => "jellyfin"),
            Username = GetOption(customOptions, "username", s => s, () => "jellyfin"),
            Password = GetOption(customOptions, "password", s => s, () => "jellyfin")
        };

        var connectionStringBuilder = new NpgsqlConnectionStringBuilder();
        connectionStringBuilder.Host = _connectionInfo.Host;
        connectionStringBuilder.Port = _connectionInfo.Port;
        connectionStringBuilder.Database = _connectionInfo.Database;
        connectionStringBuilder.Username = _connectionInfo.Username;
        connectionStringBuilder.Password = _connectionInfo.Password;

        var connectionString = connectionStringBuilder.ToString();

        _logger.LogInformation("PostgreSQL connection string: {ConnectionString}", connectionStringBuilder.ToString()); // Be careful logging passwords in prod

        options.UseNpgsql(
            connectionString,
            npgsqlOptions => npgsqlOptions.MigrationsAssembly(GetType().Assembly));
    }

    private static T GetOption<T>(ICollection<CustomDatabaseOption>? options, string key, Func<string, T> converter, Func<T>? defaultValue = null)
    {
        if (options is null)
        {
            return defaultValue is not null ? defaultValue() : default!;
        }

        var value = options.FirstOrDefault(e => e.Key.Equals(key, StringComparison.OrdinalIgnoreCase));
        if (value is null)
        {
            return defaultValue is not null ? defaultValue() : default!;
        }

        return converter(value.Value);
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
        var key = DateTime.UtcNow.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);
        var backupFolder = Path.Combine(_appPaths.DataPath, "backups");
        Directory.CreateDirectory(backupFolder);
        var backupFile = Path.Combine(backupFolder, $"{key}_jellyfin.dump");

        _logger.LogInformation("Creating PostgreSQL backup at {Path}", backupFile);

        RunProcess("pg_dump", $"-h {_connectionInfo.Host} -p {_connectionInfo.Port} -U {_connectionInfo.Username} -F c -b -v -f \"{backupFile}\" {_connectionInfo.Database}", _connectionInfo.Password);

        return Task.FromResult(key);
    }

    /// <inheritdoc />
    public Task RestoreBackupFast(string key, CancellationToken cancellationToken)
    {
        var backupFile = Path.Combine(_appPaths.DataPath, "backups", $"{key}_jellyfin.dump");

        if (!File.Exists(backupFile))
        {
            _logger.LogCritical("Tried to restore a backup that does not exist: {Key}", key);
            return Task.CompletedTask;
        }

        _logger.LogInformation("Restoring PostgreSQL backup from {Path}", backupFile);

        // -c: Clean (drop) database objects before creating them
        RunProcess("pg_restore", $"-h {_connectionInfo.Host} -p {_connectionInfo.Port} -U {_connectionInfo.Username} -d {_connectionInfo.Database} -c -v \"{backupFile}\"", _connectionInfo.Password);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task DeleteBackup(string key)
    {
        var backupFile = Path.Combine(_appPaths.DataPath, "backups", $"{key}_jellyfin.dump");

        if (!File.Exists(backupFile))
        {
            _logger.LogCritical("Tried to delete a backup that does not exist: {Key}", key);
            return Task.CompletedTask;
        }

        File.Delete(backupFile);
        return Task.CompletedTask;
    }

    private void RunProcess(string fileName, string arguments, string password)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = fileName,
            Arguments = arguments,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        if (!string.IsNullOrEmpty(password))
        {
            startInfo.EnvironmentVariables["PGPASSWORD"] = password;
        }

        using var process = new Process { StartInfo = startInfo };
        process.Start();
        process.WaitForExit();

        if (process.ExitCode != 0)
        {
            var error = process.StandardError.ReadToEnd();
            throw new InvalidOperationException($"{fileName} failed with exit code {process.ExitCode}: {error}");
        }
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

    private class ConnectionInfo
    {
        public string Host { get; set; } = string.Empty;

        public int Port { get; set; }

        public string Database { get; set; } = string.Empty;

        public string Username { get; set; } = string.Empty;

        public string Password { get; set; } = string.Empty;
    }
}
