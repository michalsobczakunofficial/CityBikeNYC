using CitiBikeNYC.Domain;
using Microsoft.EntityFrameworkCore;

namespace CitiBikeNYC.Data;

public sealed class AppDbContext : DbContext
{
    public DbSet<Station> Stations => Set<Station>();
    public DbSet<Ride> Rides => Set<Ride>();
    public DbSet<ImportError> ImportErrors => Set<ImportError>();

    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Station>(e =>
        {
            e.ToTable("Stations");
            e.HasKey(x => x.StationId);

            e.Property(x => x.StationId).HasMaxLength(64);
            e.Property(x => x.Name).HasMaxLength(256);

            e.HasIndex(x => x.Name);
        });

        modelBuilder.Entity<Ride>(e =>
        {
            e.ToTable("Rides");
            e.HasKey(x => x.RideId);

            e.Property(x => x.RideId).HasMaxLength(64);
            e.Property(x => x.RideableType).HasMaxLength(32);

            e.Property(x => x.MemberType)
                .HasConversion<string>()
                .HasMaxLength(16);

            e.HasOne(x => x.StartStation)
                .WithMany(x => x.StartRides)
                .HasForeignKey(x => x.StartStationId)
                .OnDelete(DeleteBehavior.Restrict);

            e.HasOne(x => x.EndStation)
                .WithMany(x => x.EndRides)
                .HasForeignKey(x => x.EndStationId)
                .OnDelete(DeleteBehavior.Restrict);

            e.HasIndex(x => x.StartedAt);
            e.HasIndex(x => new { x.StartStationId, x.StartedAt });
            e.HasIndex(x => new { x.EndStationId, x.StartedAt });
        });

        modelBuilder.Entity<ImportError>(e =>
        {
            e.ToTable("ImportErrors");
            e.HasKey(x => x.Id);

            e.Property(x => x.SourceFile).HasMaxLength(260);
            e.Property(x => x.RawLine).HasMaxLength(2000);
            e.Property(x => x.RideId).HasMaxLength(64);

            e.Property(x => x.ErrorCode)
                .HasConversion<string>()
                .HasMaxLength(64);

            e.HasIndex(x => x.OccurredAtUtc);
            e.HasIndex(x => x.ErrorCode);
        });
    }
}
