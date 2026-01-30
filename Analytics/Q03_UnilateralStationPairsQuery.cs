using CitiBikeNYC.Data;
using Microsoft.EntityFrameworkCore;

namespace CitiBikeNYC.Analytics;

public sealed class Q03_UnilateralStationPairsQuery
{
    public sealed record Result(
        string FromStationId,
        string? FromStationName,
        string ToStationId,
        string? ToStationName,
        int TripsFromTo,
        int TripsToFrom,
        int NetFlow,
        double SkewFromTo
    );

    public async Task<IReadOnlyList<Result>> ExecuteAsync(
        AppDbContext db,
        int topN = 20,
        int minTotalTrips = 50,
        CancellationToken ct = default)
    {
        var pairs = await db.Rides
            .Where(r => r.StartStationId != null && r.EndStationId != null)
            .Select(r => new { From = r.StartStationId!, To = r.EndStationId! })
            .ToListAsync(ct);

        var counts = pairs
            .GroupBy(p => (p.From, p.To))
            .Select(g => new { g.Key.From, g.Key.To, Count = g.Count() })
            .ToList();

        var countByPair = counts.ToDictionary(x => (x.From, x.To), x => x.Count);

        var stationNames = await db.Stations
            .Select(s => new { s.StationId, s.Name })
            .ToDictionaryAsync(x => x.StationId, x => x.Name, ct);

        var seenUndirected = new HashSet<(string A, string B)>(new UndirectedPairComparer());
        var results = new List<Result>();

        foreach (var kv in countByPair)
        {
            var from = kv.Key.Item1;
            var to = kv.Key.Item2;

            var undirected = StringComparer.Ordinal.Compare(from, to) <= 0 ? (from, to) : (to, from);
            if (!seenUndirected.Add(undirected))
                continue;

            int ab = countByPair.TryGetValue((from, to), out var abCount) ? abCount : 0;
            int ba = countByPair.TryGetValue((to, from), out var baCount) ? baCount : 0;
            int total = ab + ba;

            if (total < minTotalTrips)
                continue;

            string strongFrom, strongTo;
            int strong, weak;

            if (ab >= ba)
            {
                strongFrom = from; strongTo = to;
                strong = ab; weak = ba;
            }
            else
            {
                strongFrom = to; strongTo = from;
                strong = ba; weak = ab;
            }

            int net = strong - weak;
            double skew = total == 0 ? 0 : (double)strong / total;

            results.Add(new Result(
                FromStationId: strongFrom,
                FromStationName: stationNames.TryGetValue(strongFrom, out var nf) ? nf : null,
                ToStationId: strongTo,
                ToStationName: stationNames.TryGetValue(strongTo, out var nt) ? nt : null,
                TripsFromTo: strong,
                TripsToFrom: weak,
                NetFlow: net,
                SkewFromTo: skew
            ));
        }

        return results
            .OrderByDescending(r => r.NetFlow)
            .ThenByDescending(r => r.TripsFromTo + r.TripsToFrom)
            .Take(topN)
            .ToList();
    }

    private sealed class UndirectedPairComparer : IEqualityComparer<(string A, string B)>
    {
        public bool Equals((string A, string B) x, (string A, string B) y)
            => StringComparer.Ordinal.Equals(x.A, y.A)
               && StringComparer.Ordinal.Equals(x.B, y.B);

        public int GetHashCode((string A, string B) obj)
            => HashCode.Combine(
                StringComparer.Ordinal.GetHashCode(obj.A),
                StringComparer.Ordinal.GetHashCode(obj.B)
            );
    }
}
