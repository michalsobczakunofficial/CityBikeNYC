namespace CitiBikeNYC;

public sealed class CliOptions
{
    public required string Command { get; init; } // "import" | "stats"

    // import
    public string? ZipPath { get; init; }
    public required int BatchSize { get; init; }

    // shared
    public required string DbPath { get; init; }

    // stats selection
    public bool StatsList { get; init; }
    public bool StatsAll { get; init; }
    public IReadOnlyList<int> StatsQueries { get; init; } = Array.Empty<int>();

    // stats params
    public int MinRides { get; init; }
    public int TopN { get; init; }

    public static CliOptions? TryParse(string[] args)
    {
        if (args.Length == 0) return null;

        var cmd = args[0].Trim().ToLowerInvariant();
        if (cmd is not ("import" or "stats")) return null;

        // defaults
        string dbPath = "citibike.db";
        int batchSize = 10_000;

        int minRides = 200;
        int topN = 20;

        bool statsList = false;
        bool statsAll = false;
        List<int> statsQueries = new();

        if (cmd == "import")
        {
            if (args.Length < 2) return null;
            var zipPath = args[1];

            for (int i = 2; i < args.Length; i++)
            {
                var a = args[i];

                if (a.Equals("--db", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
                {
                    dbPath = args[++i];
                    continue;
                }

                if (a.Equals("--batch", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
                {
                    if (!int.TryParse(args[++i], out batchSize) || batchSize <= 0) return null;
                    continue;
                }

                return null;
            }

            return new CliOptions
            {
                Command = "import",
                ZipPath = zipPath,
                DbPath = dbPath,
                BatchSize = batchSize,
                MinRides = minRides,
                TopN = topN,
                StatsList = false,
                StatsAll = false,
                StatsQueries = Array.Empty<int>()
            };
        }

        // stats
        for (int i = 1; i < args.Length; i++)
        {
            var a = args[i];

            if (a.Equals("--db", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                dbPath = args[++i];
                continue;
            }

            if (a.Equals("--minrides", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                if (!int.TryParse(args[++i], out minRides) || minRides < 0) return null;
                continue;
            }

            if (a.Equals("--top", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                if (!int.TryParse(args[++i], out topN) || topN <= 0) return null;
                continue;
            }

            if (a.Equals("--list", StringComparison.OrdinalIgnoreCase))
            {
                statsList = true;
                continue;
            }

            if (a.Equals("--all", StringComparison.OrdinalIgnoreCase))
            {
                statsAll = true;
                continue;
            }

            if (a.Equals("--q", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            {
                var raw = args[++i];
                var parts = raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                foreach (var p in parts)
                {
                    if (!int.TryParse(p, out var q) || q <= 0) return null;
                    statsQueries.Add(q);
                }
                continue;
            }

            return null;
        }

        // If nothing selected
        if (!statsList && !statsAll && statsQueries.Count == 0)
            statsList = true;

        return new CliOptions
        {
            Command = "stats",
            ZipPath = null,
            DbPath = dbPath,
            BatchSize = batchSize,
            MinRides = minRides,
            TopN = topN,
            StatsList = statsList,
            StatsAll = statsAll,
            StatsQueries = statsQueries.Distinct().OrderBy(x => x).ToArray()
        };
    }

    public static void PrintUsage()
    {
        Console.WriteLine(@"
CitiBikeNYC (.NET 9)

Commands:
  import <path-to-csv-zip> [--db <db-path>] [--batch <n>]

  stats [--db <db-path>] [--list] [--all] [--q <n|n,n,...>] [--minrides <n>] [--top <n>]

Examples:
  dotnet run -- import ""data\202407-citibike-tripdata.zip"" --db citibike.db --batch 30000

  dotnet run -- stats --list --db citibike.db
  dotnet run -- stats --q 1 --db citibike.db
  dotnet run -- stats --q 1,2,8 --db citibike.db
  dotnet run -- stats --all --db citibike.db
");
    }
}
