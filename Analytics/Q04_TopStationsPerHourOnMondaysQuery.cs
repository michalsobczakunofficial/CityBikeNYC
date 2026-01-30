using CitiBikeNYC.Data;
using Microsoft.EntityFrameworkCore;

namespace CitiBikeNYC.Analytics;

public sealed class Q04_TopStationsPerHourOnMondaysQuery
{
    public sealed record ResultRow(
        int Hour,
        int Rank,
        string StationId,
        string? StationName,
        int RideCount
    );

    public async Task<IReadOnlyList<ResultRow>> ExecuteAsync(
        AppDbContext db,
        int topPerHour = 3,
        int minRidesPerHourStation = 1,
        CancellationToken ct = default)
    {
        var rows = await db.Rides
            .Where(r => r.StartStationId != null)
            .Select(r => new { StationId = r.StartStationId!, r.StartedAt })
            .ToListAsync(ct);

        var monday = rows
            .Where(r => r.StartedAt.DayOfWeek == DayOfWeek.Monday)
            .Select(r => new { r.StationId, Hour = r.StartedAt.Hour });

        var counts = monday
            .GroupBy(x => (x.Hour, x.StationId))
            .Select(g => new { g.Key.Hour, g.Key.StationId, Count = g.Count() })
            .Where(x => x.Count >= minRidesPerHourStation)
            .ToList();

        var stationNames = await db.Stations
            .Select(s => new { s.StationId, s.Name })
            .ToDictionaryAsync(x => x.StationId, x => x.Name, ct);

        var result = new List<ResultRow>();

        foreach (var hourGroup in counts.GroupBy(x => x.Hour).OrderBy(g => g.Key))
        {
            var top = hourGroup
                .OrderByDescending(x => x.Count)
                .ThenBy(x => x.StationId, StringComparer.Ordinal)
                .Take(topPerHour)
                .ToList();

            for (int i = 0; i < top.Count; i++)
            {
                var t = top[i];
                result.Add(new ResultRow(
                    Hour: hourGroup.Key,
                    Rank: i + 1,
                    StationId: t.StationId,
                    StationName: stationNames.TryGetValue(t.StationId, out var name) ? name : null,
                    RideCount: t.Count
                ));
            }
        }

        return result;
    }
}
