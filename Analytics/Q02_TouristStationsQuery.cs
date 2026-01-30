using CitiBikeNYC.Data;
using CitiBikeNYC.Domain;
using Microsoft.EntityFrameworkCore;

namespace CitiBikeNYC.Analytics;

public sealed class Q02_TouristStationsQuery
{
    public sealed record Result(
        string StationId,
        string? StationName,
        int TotalRides,
        int CasualRides,
        double CasualShare
    );

    public async Task<IReadOnlyList<Result>> ExecuteAsync(
        AppDbContext db,
        int minRides = 200,
        int topN = 20,
        CancellationToken ct = default)
    {
        var rides = await db.Rides
            .Where(r => r.StartStationId != null)
            .Select(r => new
            {
                StationId = r.StartStationId!,
                r.MemberType,
                r.StartedAt
            })
            .ToListAsync(ct);

        var window = rides.Where(r =>
            (r.StartedAt.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday) &&
            r.StartedAt.Hour >= 10 && r.StartedAt.Hour < 18);

        var stationNames = await db.Stations
            .Select(s => new { s.StationId, s.Name })
            .ToDictionaryAsync(x => x.StationId, x => x.Name, ct);

        return window
            .GroupBy(r => r.StationId)
            .Select(g =>
            {
                int total = g.Count();
                int casual = g.Count(x => x.MemberType == MemberType.Casual);

                return new Result(
                    StationId: g.Key,
                    StationName: stationNames.TryGetValue(g.Key, out var name) ? name : null,
                    TotalRides: total,
                    CasualRides: casual,
                    CasualShare: total == 0 ? 0 : (double)casual / total
                );
            })
            .Where(x => x.TotalRides >= minRides)
            .OrderByDescending(x => x.CasualShare)
            .ThenByDescending(x => x.TotalRides)
            .Take(topN)
            .ToList();
    }
}
