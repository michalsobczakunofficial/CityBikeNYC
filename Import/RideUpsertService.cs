using CitiBikeNYC.Csv;
using CitiBikeNYC.Data;
using CitiBikeNYC.Domain;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace CitiBikeNYC.Import;

public sealed class RideUpsertService
{
    private readonly AppDbContext _db;
    private readonly ILogger<RideUpsertService> _logger;

    private const int SqliteParamSafeChunk = 900;

    public RideUpsertService(AppDbContext db, ILogger<RideUpsertService> logger)
    {
        _db = db;
        _logger = logger;
    }

    public async Task LogErrorAsync(
        string sourceFile,
        int rowNumber,
        ImportErrorCode code,
        string rawLine,
        string? rideId,
        CancellationToken ct)
    {
        _db.ImportErrors.Add(new ImportError
        {
            SourceFile = sourceFile,
            RowNumber = rowNumber,
            ErrorCode = code,
            RawLine = rawLine,
            OccurredAtUtc = DateTime.UtcNow,
            RideId = rideId
        });

        await _db.SaveChangesAsync(ct);
        _db.ChangeTracker.Clear();
    }

    public async Task UpsertBatchAsync(IEnumerable<RideCsvRow> rows, string sourceFile, CancellationToken ct)
    {
        var rowList = rows.ToList();
        if (rowList.Count == 0) return;

        var stationCandidates = BuildStationCandidates(rowList);

        await using var tx = await _db.Database.BeginTransactionAsync(ct);

        var prevAutoDetect = _db.ChangeTracker.AutoDetectChangesEnabled;
        _db.ChangeTracker.AutoDetectChangesEnabled = false;

        try
        {
            await UpsertStationsAsync(stationCandidates, ct);
            await UpsertRidesAsync(rowList, ct);

            await _db.SaveChangesAsync(ct);
            await tx.CommitAsync(ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Batch failed for {File}. Rolling back.", sourceFile);
            await tx.RollbackAsync(ct);
            throw;
        }
        finally
        {
            _db.ChangeTracker.AutoDetectChangesEnabled = prevAutoDetect;
            _db.ChangeTracker.Clear();
        }
    }

    private sealed record StationCandidate(
        string StationId,
        string? Name,
        double? Lat,
        double? Lng,
        DateTime FirstSeenAt,
        DateTime LastSeenAt
    );

    private static Dictionary<string, StationCandidate> BuildStationCandidates(List<RideCsvRow> rows)
    {
        var dict = new Dictionary<string, StationCandidate>(StringComparer.Ordinal);

        void Merge(string id, string? name, double? lat, double? lng, DateTime seenAt)
        {
            if (dict.TryGetValue(id, out var existing))
            {
                dict[id] = new StationCandidate(
                    StationId: id,
                    Name: !string.IsNullOrWhiteSpace(name) ? name : existing.Name,
                    Lat: lat ?? existing.Lat,
                    Lng: lng ?? existing.Lng,
                    FirstSeenAt: seenAt < existing.FirstSeenAt ? seenAt : existing.FirstSeenAt,
                    LastSeenAt: seenAt > existing.LastSeenAt ? seenAt : existing.LastSeenAt
                );
                return;
            }

            dict[id] = new StationCandidate(
                StationId: id,
                Name: name,
                Lat: lat,
                Lng: lng,
                FirstSeenAt: seenAt,
                LastSeenAt: seenAt
            );
        }

        foreach (var r in rows)
        {
            if (!string.IsNullOrWhiteSpace(r.StartStationId))
                Merge(r.StartStationId!, r.StartStationName, r.StartLat, r.StartLng, r.StartedAt);

            if (!string.IsNullOrWhiteSpace(r.EndStationId))
                Merge(r.EndStationId!, r.EndStationName, r.EndLat, r.EndLng, r.EndedAt);
        }

        return dict;
    }

    private async Task UpsertStationsAsync(Dictionary<string, StationCandidate> candidates, CancellationToken ct)
    {
        if (candidates.Count == 0) return;

        var stationIds = candidates.Keys.ToList();
        var existingById = new Dictionary<string, Station>(StringComparer.Ordinal);

        foreach (var chunk in stationIds.Chunk(SqliteParamSafeChunk))
        {
            var found = await _db.Stations
                .Where(s => chunk.Contains(s.StationId))
                .ToListAsync(ct);

            foreach (var s in found)
                existingById[s.StationId] = s;
        }

        foreach (var cand in candidates.Values)
        {
            if (!existingById.TryGetValue(cand.StationId, out var station))
            {
                station = new Station
                {
                    StationId = cand.StationId,
                    Name = cand.Name,
                    Lat = cand.Lat,
                    Lng = cand.Lng,
                    FirstSeenAt = cand.FirstSeenAt,
                    LastSeenAt = cand.LastSeenAt
                };
                _db.Stations.Add(station);
                continue;
            }

            if (!string.IsNullOrWhiteSpace(cand.Name))
                station.Name = cand.Name;

            if (cand.Lat is not null) station.Lat = cand.Lat;
            if (cand.Lng is not null) station.Lng = cand.Lng;

            if (cand.FirstSeenAt < station.FirstSeenAt) station.FirstSeenAt = cand.FirstSeenAt;
            if (cand.LastSeenAt > station.LastSeenAt) station.LastSeenAt = cand.LastSeenAt;
        }
    }

    private async Task UpsertRidesAsync(List<RideCsvRow> rows, CancellationToken ct)
    {
        var rideIds = rows.Select(r => r.RideId).Distinct(StringComparer.Ordinal).ToList();
        var existingById = new Dictionary<string, Ride>(StringComparer.Ordinal);

        foreach (var chunk in rideIds.Chunk(SqliteParamSafeChunk))
        {
            var found = await _db.Rides
                .Where(r => chunk.Contains(r.RideId))
                .ToListAsync(ct);

            foreach (var r in found)
                existingById[r.RideId] = r;
        }

        foreach (var row in rows)
        {
            if (!existingById.TryGetValue(row.RideId, out var ride))
            {
                ride = new Ride { RideId = row.RideId };
                _db.Rides.Add(ride);
                existingById[row.RideId] = ride;
            }

            if (!string.IsNullOrWhiteSpace(row.RideableType))
                ride.RideableType = row.RideableType;

            ride.StartedAt = row.StartedAt;
            ride.EndedAt = row.EndedAt;

            ride.MemberType = MemberTypeExtensions.FromCsv(row.MemberCasual);

            if (!string.IsNullOrWhiteSpace(row.StartStationId))
                ride.StartStationId = row.StartStationId;

            if (!string.IsNullOrWhiteSpace(row.EndStationId))
                ride.EndStationId = row.EndStationId;

            if (row.StartLat is not null) ride.StartLat = row.StartLat;
            if (row.StartLng is not null) ride.StartLng = row.StartLng;
            if (row.EndLat is not null) ride.EndLat = row.EndLat;
            if (row.EndLng is not null) ride.EndLng = row.EndLng;
        }
    }
}
