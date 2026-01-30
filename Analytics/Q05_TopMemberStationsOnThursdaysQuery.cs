using CitiBikeNYC.Data;
using CitiBikeNYC.Domain;
using Microsoft.EntityFrameworkCore;

namespace CitiBikeNYC.Analytics;

public sealed class Q05_TopMemberStationsOnThursdaysQuery
{
    public sealed record Result(
        string StationId,
        string? StationName,
        int TotalRides,
        int MemberRides,
        double MemberShare
    );

    public async Task<IReadOnlyList<Result>> ExecuteAsync(
        AppDbContext db,
        int topN = 20,
        int minRides = 200,
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

        var thursday = rides.Where(r => r.StartedAt.DayOfWeek == DayOfWeek.Thursday);

        var stationNames = await db.Stations
            .Select(s => new { s.StationId, s.Name })
            .ToDictionaryAsync(x => x.StationId, x => x.Name, ct);

        return thursday
            .GroupBy(r => r.StationId)
            .Select(g =>
            {
                int total = g.Count();
                int members = g.Count(x => x.MemberType == MemberType.Member);

                return new Result(
                    StationId: g.Key,
                    StationName: stationNames.TryGetValue(g.Key, out var name) ? name : null,
                    TotalRides: total,
                    MemberRides: members,
                    MemberShare: total == 0 ? 0 : (double)members / total
                );
            })
            .Where(x => x.TotalRides >= minRides)
            .OrderByDescending(x => x.MemberShare)
            .ThenByDescending(x => x.TotalRides)
            .Take(topN)
            .ToList();
    }
}
