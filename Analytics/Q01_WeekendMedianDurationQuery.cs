using CitiBikeNYC.Analytics.Common;
using CitiBikeNYC.Data;
using CitiBikeNYC.Domain;
using Microsoft.EntityFrameworkCore;

namespace CitiBikeNYC.Analytics;

public sealed class Q01_WeekendMedianDurationQuery
{
    public sealed record Result(
        MemberType MemberType,
        int RideCount,
        double AverageSeconds,
        double MedianSeconds
    );

    public async Task<IReadOnlyList<Result>> ExecuteAsync(AppDbContext db, CancellationToken ct = default)
    {
        var rows = await db.Rides
            .Select(r => new { r.MemberType, r.StartedAt, r.EndedAt })
            .ToListAsync(ct);

        var weekend = rows
            .Where(r => r.StartedAt.DayOfWeek is DayOfWeek.Saturday or DayOfWeek.Sunday)
            .Select(r => new
            {
                r.MemberType,
                DurationSeconds = (r.EndedAt - r.StartedAt).TotalSeconds
            })
            .Where(x => x.DurationSeconds >= 0);

        return weekend
            .GroupBy(x => x.MemberType)
            .Select(g =>
            {
                var durations = g.Select(x => x.DurationSeconds);
                return new Result(
                    MemberType: g.Key,
                    RideCount: g.Count(),
                    AverageSeconds: durations.Average(),
                    MedianSeconds: durations.Median()
                );
            })
            .OrderBy(r => r.MemberType)
            .ToList();
    }
}
