using CitiBikeNYC.Analytics;
using CitiBikeNYC.Data;
using CitiBikeNYC.Import;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CitiBikeNYC;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var options = CliOptions.TryParse(args);
        if (options is null)
        {
            CliOptions.PrintUsage();
            return 2;
        }

        var builder = Host.CreateApplicationBuilder(args);

        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(o =>
        {
            o.SingleLine = true;
            o.TimestampFormat = "HH:mm:ss ";
        });

        builder.Services.AddDbContext<AppDbContext>(opt =>
            opt.UseSqlite($"Data Source={options.DbPath}")
        );

        // Import services
        builder.Services.AddScoped<RideUpsertService>();
        builder.Services.AddScoped<ZipRideImporter>();

        // Analytics queries
        builder.Services.AddScoped<Q01_WeekendMedianDurationQuery>();
        builder.Services.AddScoped<Q02_TouristStationsQuery>();
        builder.Services.AddScoped<Q03_UnilateralStationPairsQuery>();
        builder.Services.AddScoped<Q04_TopStationsPerHourOnMondaysQuery>();
        builder.Services.AddScoped<Q05_TopMemberStationsOnThursdaysQuery>();
        builder.Services.AddScoped<Q06_TopStationsOverallWithMemberCasualBreakdownQuery>();
        builder.Services.AddScoped<Q07_RouteWithBiggestMemberMondaySundayDifferenceQuery>();
        builder.Services.AddScoped<Q08_MostMemberAndCasualHoursOfWeekQuery>();

        using var host = builder.Build();
        using var scope = host.Services.CreateScope();

        var logger = scope.ServiceProvider.GetRequiredService<ILoggerFactory>()
            .CreateLogger("CitiBikeNYC");

        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        await db.Database.EnsureCreatedAsync();

        logger.LogInformation("DB: {DbPath}", options.DbPath);

        if (options.Command == "import")
        {
            var importer = scope.ServiceProvider.GetRequiredService<ZipRideImporter>();

            logger.LogInformation("ZIP: {ZipPath}", options.ZipPath);
            logger.LogInformation("Batch size: {BatchSize}", options.BatchSize);

            await importer.ImportZipAsync(options.ZipPath!, options.BatchSize, CancellationToken.None);
            logger.LogInformation("Import done.");
            return 0;
        }

        if (options.Command == "stats")
        {
            if (options.StatsList)
            {
                PrintAvailableQueries();
                return 0;
            }

            var toRun = ResolveQueriesToRun(options);
            if (toRun.Count == 0)
            {
                Console.WriteLine("No queries selected.");
                PrintAvailableQueries();
                return 2;
            }

            foreach (var q in toRun)
            {
                Console.WriteLine();
                await RunOneQueryAsync(q, scope.ServiceProvider, db, options);
            }

            Console.WriteLine();
            return 0;
        }

        CliOptions.PrintUsage();
        return 2;
    }

    private static void PrintAvailableQueries()
    {
        Console.WriteLine(@"Available queries:
  1 - Weekend duration stats (avg + median) by MemberType
  2 - Most tourist stations (weekend 10-18) by Casual share
  3 - Most unilateral station pairs (A->B >> B->A)
  4 - Top 3 start stations per hour on Mondays
  5 - Top 20 member-heavy start stations on Thursdays (by Member share)
  6 - Top 20 stations overall (starts) with Member/Casual breakdown
  7 - Route with biggest Member difference: Monday vs Sunday
  8 - Most member-heavy and most casual-heavy time slots (DayOfWeek + Hour)

Use:
  stats --q 1
  stats --q 1,2,8
  stats --all
");
    }

    private static List<int> ResolveQueriesToRun(CliOptions options)
    {
        var known = new HashSet<int> { 1, 2, 3, 4, 5, 6, 7, 8 };

        if (options.StatsAll)
            return known.OrderBy(x => x).ToList();

        return options.StatsQueries.Where(known.Contains).ToList();
    }

    private static async Task RunOneQueryAsync(int q, IServiceProvider sp, AppDbContext db, CliOptions options)
    {
        switch (q)
        {
            case 1:
            {
                var query = sp.GetRequiredService<Q01_WeekendMedianDurationQuery>();
                var res = await query.ExecuteAsync(db);

                Console.WriteLine("Q01: Weekend duration stats (avg + median)");
                foreach (var r in res)
                    Console.WriteLine($"  {r.MemberType,-8}  count={r.RideCount,8}  avg={r.AverageSeconds,10:F1}s  median={r.MedianSeconds,10:F1}s");

                break;
            }
            case 2:
            {
                var query = sp.GetRequiredService<Q02_TouristStationsQuery>();
                var res = await query.ExecuteAsync(db, minRides: options.MinRides, topN: options.TopN);

                Console.WriteLine($"Q02: Most tourist stations (weekend 10-18), minRides={options.MinRides}, top={options.TopN}");
                foreach (var s in res)
                    Console.WriteLine($"  {s.CasualShare,6:P1}  total={s.TotalRides,7}  casual={s.CasualRides,7}  {s.StationId}  {s.StationName}");

                break;
            }
            case 3:
            {
                var query = sp.GetRequiredService<Q03_UnilateralStationPairsQuery>();
                var res = await query.ExecuteAsync(db, topN: 20, minTotalTrips: 50);

                Console.WriteLine("Q03: Most unilateral station pairs (A->B >> B->A)");
                foreach (var r in res)
                {
                    var total = r.TripsFromTo + r.TripsToFrom;
                    Console.WriteLine($"  net={r.NetFlow,6}  skew={r.SkewFromTo,6:P1}  total={total,7}  {r.FromStationId} -> {r.ToStationId}  ({r.TripsFromTo} vs {r.TripsToFrom})  {r.FromStationName} -> {r.ToStationName}");
                }

                break;
            }
            case 4:
            {
                var query = sp.GetRequiredService<Q04_TopStationsPerHourOnMondaysQuery>();
                var res = await query.ExecuteAsync(db, topPerHour: 3);

                Console.WriteLine("Q04: Top 3 start stations per hour on Mondays");
                foreach (var r in res.OrderBy(x => x.Hour).ThenBy(x => x.Rank))
                    Console.WriteLine($"  {r.Hour:00}:00  #{r.Rank}  count={r.RideCount,7}  {r.StationId}  {r.StationName}");

                break;
            }
            case 5:
            {
                var query = sp.GetRequiredService<Q05_TopMemberStationsOnThursdaysQuery>();
                var res = await query.ExecuteAsync(db, topN: 20, minRides: options.MinRides);

                Console.WriteLine($"Q05: Top 20 member-heavy start stations on Thursdays, minRides={options.MinRides}");
                foreach (var s in res)
                    Console.WriteLine($"  {s.MemberShare,6:P1}  total={s.TotalRides,7}  members={s.MemberRides,7}  {s.StationId}  {s.StationName}");

                break;
            }
            case 6:
            {
                var query = sp.GetRequiredService<Q06_TopStationsOverallWithMemberCasualBreakdownQuery>();
                var res = await query.ExecuteAsync(db, topN: 20, minRides: options.MinRides);

                Console.WriteLine($"Q06: Top stations overall (starts), minRides={options.MinRides}");
                foreach (var s in res)
                {
                    Console.WriteLine($"  total={s.TotalRides,8}  members={s.MemberRides,8} ({s.MemberShare,6:P1})  casual={s.CasualRides,8} ({s.CasualShare,6:P1})  unk={s.UnknownRides,6}  {s.StationId}  {s.StationName}");
                }

                break;
            }
            case 7:
            {
                var query = sp.GetRequiredService<Q07_RouteWithBiggestMemberMondaySundayDifferenceQuery>();
                var res = await query.ExecuteAsync(db, minTotalMondaySundayMembers: 50);

                Console.WriteLine("Q07: Biggest Member route difference (Monday vs Sunday)");
                if (res is null)
                {
                    Console.WriteLine("  No route matched the criteria (try lowering the threshold).");
                    break;
                }

                Console.WriteLine($"  {res.StartStationId} -> {res.EndStationId}  {res.StartStationName} -> {res.EndStationName}\n  MondayMembers={res.MemberMondayCount:n0}  SundayMembers={res.MemberSundayCount:n0}\n  NetDiff(Mon-Sun)={res.NetDifference:n0}  AbsDiff={res.AbsDifference:n0}  Total(Mon+Sun)={res.TotalMemberCount:n0}");
                break;
            }
            case 8:
            {
                var query = sp.GetRequiredService<Q08_MostMemberAndCasualHoursOfWeekQuery>();
                var res = await query.ExecuteAsync(db, topN: 5, minTotalRidesPerSlot: 500);

                Console.WriteLine("Q08: Most member-heavy time slots (DayOfWeek + Hour)");
                foreach (var s in res.MostMemberHeavy)
                    Console.WriteLine($"  {s.DayOfWeek,-9} {s.Hour:00}:00  member={s.MemberShare,6:P1}  casual={s.CasualShare,6:P1}  total={s.TotalRides,8}");

                Console.WriteLine();
                Console.WriteLine("Q08: Most casual-heavy time slots (DayOfWeek + Hour)");
                foreach (var s in res.MostCasualHeavy)
                    Console.WriteLine($"  {s.DayOfWeek,-9} {s.Hour:00}:00  member={s.MemberShare,6:P1}  casual={s.CasualShare,6:P1}  total={s.TotalRides,8}");

                break;
            }
            default:
                Console.WriteLine($"Unknown query: {q}");
                break;
        }
    }
}
