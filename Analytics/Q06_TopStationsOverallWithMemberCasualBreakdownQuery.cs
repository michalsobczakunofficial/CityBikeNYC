using CitiBikeNYC.Data;
using CitiBikeNYC.Domain;
using Microsoft.EntityFrameworkCore;

namespace CitiBikeNYC.Analytics;

public sealed class Q06_TopStationsOverallWithMemberCasualBreakdownQuery
{
    public sealed record Result(
        string StationId,
        string? StationName,
        int TotalRides,
        int MemberRides,
        int CasualRides,
        int UnknownRides,
        double MemberShare,
        double CasualShare
    );

    public async Task<IReadOnlyList<Result>> ExecuteAsync(
        AppDbContext db,
        int topN = 20,
        int minRides = 1,
        CancellationToken ct = default)
    {
        var rides = await db.Rides
            .Where(r => r.StartStationId != null)
            .Select(r => new { StationId = r.StartStationId!, r.MemberType })
            .ToListAsync(ct);

        var stationNames = await db.Stations
            .Select(s => new { s.StationId, s.Name })
            .ToDictionaryAsync(x => x.StationId, x => x.Name, ct);

        return rides
            .GroupBy(r => r.StationId)
            .Select(g =>
            {
                int total = g.Count();
                int members = g.Count(x => x.MemberType == MemberType.Member);
                int casual = g.Count(x => x.MemberType == MemberType.Casual);
                int unknown = total - members - casual;

                return new Result(
                    StationId: g.Key,
                    StationName: stationNames.TryGetValue(g.Key, out var name) ? name : null,
                    TotalRides: total,
                    MemberRides: members,
                    CasualRides: casual,
                    UnknownRides: unknown,
                    MemberShare: total == 0 ? 0 : (double)members / total,
                    CasualShare: total == 0 ? 0 : (double)casual / total
                );
            })
            .Where(x => x.TotalRides >= minRides)
            .OrderByDescending(x => x.TotalRides)
            .ThenByDescending(x => x.MemberRides)
            .Take(topN)
            .ToList();
    }
}
