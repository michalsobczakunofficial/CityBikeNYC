using CitiBikeNYC.Data;
using CitiBikeNYC.Domain;
using Microsoft.EntityFrameworkCore;

namespace CitiBikeNYC.Analytics;

public sealed class Q08_MostMemberAndCasualHoursOfWeekQuery
{
    public sealed record Slot(
        DayOfWeek DayOfWeek,
        int Hour,
        int TotalRides,
        int MemberRides,
        int CasualRides,
        int UnknownRides,
        double MemberShare,
        double CasualShare
    );

    public sealed record Result(
        IReadOnlyList<Slot> MostMemberHeavy,
        IReadOnlyList<Slot> MostCasualHeavy
    );

    public async Task<Result> ExecuteAsync(
        AppDbContext db,
        int topN = 5,
        int minTotalRidesPerSlot = 500,
        CancellationToken ct = default)
    {
        var rows = await db.Rides
            .Select(r => new { r.MemberType, r.StartedAt })
            .ToListAsync(ct);

        var perSlot = rows
            .GroupBy(r => (r.StartedAt.DayOfWeek, Hour: r.StartedAt.Hour))
            .Select(g =>
            {
                int total = g.Count();
                int members = g.Count(x => x.MemberType == MemberType.Member);
                int casual = g.Count(x => x.MemberType == MemberType.Casual);
                int unknown = total - members - casual;

                double memberShare = total == 0 ? 0 : (double)members / total;
                double casualShare = total == 0 ? 0 : (double)casual / total;

                return new Slot(
                    DayOfWeek: g.Key.DayOfWeek,
                    Hour: g.Key.Hour,
                    TotalRides: total,
                    MemberRides: members,
                    CasualRides: casual,
                    UnknownRides: unknown,
                    MemberShare: memberShare,
                    CasualShare: casualShare
                );
            })
            .Where(s => s.TotalRides >= minTotalRidesPerSlot)
            .ToList();

        var mostMember = perSlot
            .OrderByDescending(s => s.MemberShare)
            .ThenByDescending(s => s.TotalRides)
            .Take(topN)
            .ToList();

        var mostCasual = perSlot
            .OrderBy(s => s.MemberShare)
            .ThenByDescending(s => s.TotalRides)
            .Take(topN)
            .ToList();

        return new Result(mostMember, mostCasual);
    }
}
