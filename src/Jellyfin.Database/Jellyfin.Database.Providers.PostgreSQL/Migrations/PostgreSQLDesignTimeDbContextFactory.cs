using Jellyfin.Database.Implementations;
using Jellyfin.Database.Implementations.Locking;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.Extensions.Logging.Abstractions;

namespace Jellyfin.Database.Providers.PostgreSQL.Migrations;

/// <summary>
/// The design time factory for <see cref="JellyfinDbContext"/>.
/// This is only used for the creation of migrations and not during runtime.
/// </summary>
internal sealed class PostgreSQLDesignTimeDbContextFactory : IDesignTimeDbContextFactory<JellyfinDbContext>
{
    public JellyfinDbContext CreateDbContext(string[] args)
    {
        var optionsBuilder = new DbContextOptionsBuilder<JellyfinDbContext>();

        // Default design-time connection string.
        // This doesn't need to be valid for connecting to a real DB if we are just generating migrations,
        // BUT Npgsql might check connection validity.
        // For safety, we use a standard local connection string.
        var connectionString = "Host=localhost;Database=jellyfin;Username=jellyfin;Password=jellyfin";

        optionsBuilder.UseNpgsql(connectionString, f => f.MigrationsAssembly(GetType().Assembly));

        return new JellyfinDbContext(
            optionsBuilder.Options,
            NullLogger<JellyfinDbContext>.Instance,
            new PostgreSQLDatabaseProvider(NullLogger<PostgreSQLDatabaseProvider>.Instance),
            new NoLockBehavior(NullLogger<NoLockBehavior>.Instance));
    }
}
