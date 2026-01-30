using CitiBikeNYC.Data;
using CitiBikeNYC.Domain;
using Microsoft.EntityFrameworkCore;

namespace CitiBikeNYC.Analytics;

public sealed class Q07_RouteWithBiggestMemberMondaySundayDifferenceQuery
{
    public sealed record Result(
        string StartStationId,
        string? StartStationName,
        string EndStationId,
        string? EndStationName,
        int MemberMondayCount,
        int MemberSundayCount,
        int NetDifference,
        int AbsDifference,
        int TotalMemberCount
    );

    public async Task<Result?> ExecuteAsync(
        AppDbContext db,
        int minTotalMondaySundayMembers = 50,
        CancellationToken ct = default)
    {
        var rides = await db.Rides
            .Where(r => r.MemberType == MemberType.Member
                        && r.StartStationId != null
                        && r.EndStationId != null)
            .Select(r => new
            {
                StartId = r.StartStationId!,
                EndId = r.EndStationId!,
                r.StartedAt
            })
            .ToListAsync(ct);

        var monSun = rides.Where(r =>
            r.StartedAt.DayOfWeek is DayOfWeek.Monday or DayOfWeek.Sunday);

        var perRoute = monSun
            .GroupBy(r => (r.StartId, r.EndId))
            .Select(g =>
            {
                int monday = g.Count(x => x.StartedAt.DayOfWeek == DayOfWeek.Monday);
                int sunday = g.Count(x => x.StartedAt.DayOfWeek == DayOfWeek.Sunday);
                int total = monday + sunday;

                return new
                {
                    g.Key.StartId,
                    g.Key.EndId,
                    Monday = monday,
                    Sunday = sunday,
                    Total = total,
                    Net = monday - sunday,
                    Abs = Math.Abs(monday - sunday)
                };
            })
            .Where(x => x.Total >= minTotalMondaySundayMembers)
            .OrderByDescending(x => x.Abs)
            .ThenByDescending(x => x.Total)
            .FirstOrDefault();

        if (perRoute is null)
            return null;

        var stationNames = await db.Stations
            .Select(s => new { s.StationId, s.Name })
            .ToDictionaryAsync(x => x.StationId, x => x.Name, ct);

        return new Result(
            StartStationId: perRoute.StartId,
            StartStationName: stationNames.TryGetValue(perRoute.StartId, out var sn) ? sn : null,
            EndStationId: perRoute.EndId,
            EndStationName: stationNames.TryGetValue(perRoute.EndId, out var en) ? en : null,
            MemberMondayCount: perRoute.Monday,
            MemberSundayCount: perRoute.Sunday,
            NetDifference: perRoute.Net,
            AbsDifference: perRoute.Abs,
            TotalMemberCount: perRoute.Total
        );
    }
}
