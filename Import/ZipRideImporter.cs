using CitiBikeNYC.Csv;
using CitiBikeNYC.Domain;
using CitiBikeNYC.Utils;
using CsvHelper;
using CsvHelper.Configuration;
using Microsoft.Extensions.Logging;
using System.Globalization;
using System.IO.Compression;

namespace CitiBikeNYC.Import;

public sealed class ZipRideImporter
{
    private readonly RideUpsertService _upsert;
    private readonly ILogger<ZipRideImporter> _logger;

    public ZipRideImporter(RideUpsertService upsert, ILogger<ZipRideImporter> logger)
    {
        _upsert = upsert;
        _logger = logger;
    }

    public async Task ImportZipAsync(string zipPath, int batchSize, CancellationToken ct)
    {
        if (!File.Exists(zipPath))
            throw new FileNotFoundException("ZIP not found", zipPath);

        await using var zipStream = File.OpenRead(zipPath);
        using var archive = new ZipArchive(zipStream, ZipArchiveMode.Read, leaveOpen: false);

        var csvEntries = archive.Entries
            .Where(e => e.FullName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase) && e.Length > 0)
            .ToList();

        if (csvEntries.Count == 0)
        {
            _logger.LogWarning("No CSV entries in ZIP.");
            return;
        }

        _logger.LogInformation("CSV files in ZIP: {Count}", csvEntries.Count);

        foreach (var entry in csvEntries)
        {
            ct.ThrowIfCancellationRequested();
            _logger.LogInformation("Importing: {Entry} ({Size:n0} bytes)", entry.FullName, entry.Length);

            await ImportSingleCsvAsync(entry, batchSize, ct);
        }
    }

    private async Task ImportSingleCsvAsync(ZipArchiveEntry entry, int batchSize, CancellationToken ct)
    {
        await using var entryStream = entry.Open();
        using var reader = new StreamReader(entryStream);

        var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = true,
            IgnoreBlankLines = true,
            TrimOptions = TrimOptions.Trim,
            MissingFieldFound = null,
            HeaderValidated = null,
            BadDataFound = null
        };

        using var csv = new CsvReader(reader, csvConfig);
        csv.Context.RegisterClassMap<RideCsvMap>();

        var buffer = new Dictionary<string, RideCsvRow>(StringComparer.Ordinal);
        int processedSinceFlush = 0;

        long totalRows = 0;
        long totalErrors = 0;

        try
        {
            await csv.ReadAsync();
            csv.ReadHeader();

            while (await csv.ReadAsync())
            {
                ct.ThrowIfCancellationRequested();

                totalRows++;
                processedSinceFlush++;

                RideCsvRow row;
                try
                {
                    row = csv.GetRecord<RideCsvRow>();
                }
                catch
                {
                    totalErrors++;
                    await _upsert.LogErrorAsync(
                        sourceFile: entry.FullName,
                        rowNumber: (int)csv.Parser.Row,
                        code: ImportErrorCode.CsvBadData,
                        rawLine: StringUtil.Truncate(csv.Parser.RawRecord ?? "", 2000),
                        rideId: null,
                        ct: ct
                    );
                    continue;
                }

                row.RideId = row.RideId?.Trim() ?? "";
                row.RideableType = row.RideableType?.Trim() ?? "";

                row.StartStationId = StringUtil.NullIfWhiteSpace(row.StartStationId);
                row.EndStationId = StringUtil.NullIfWhiteSpace(row.EndStationId);
                row.StartStationName = StringUtil.NullIfWhiteSpace(row.StartStationName);
                row.EndStationName = StringUtil.NullIfWhiteSpace(row.EndStationName);
                row.MemberCasual = StringUtil.NullIfWhiteSpace(row.MemberCasual);

                if (string.IsNullOrWhiteSpace(row.RideId))
                {
                    totalErrors++;
                    await _upsert.LogErrorAsync(entry.FullName, (int)csv.Parser.Row,
                        ImportErrorCode.MissingRideId,
                        StringUtil.Truncate(csv.Parser.RawRecord ?? "", 2000),
                        rideId: null, ct);
                    continue;
                }

                if (row.EndedAt < row.StartedAt)
                {
                    totalErrors++;
                    await _upsert.LogErrorAsync(entry.FullName, (int)csv.Parser.Row,
                        ImportErrorCode.EndBeforeStart,
                        StringUtil.Truncate(csv.Parser.RawRecord ?? "", 2000),
                        rideId: row.RideId, ct);
                    continue;
                }

                buffer[row.RideId] = row;

                if (processedSinceFlush >= batchSize)
                {
                    await _upsert.UpsertBatchAsync(buffer.Values, entry.FullName, ct);
                    buffer.Clear();
                    processedSinceFlush = 0;

                    if (totalRows % 100_000 == 0)
                        _logger.LogInformation("Progress {File}: {Rows:n0} rows, {Errors:n0} errors",
                            entry.FullName, totalRows, totalErrors);
                }
            }
        }
        finally
        {
            if (buffer.Count > 0)
            {
                await _upsert.UpsertBatchAsync(buffer.Values, entry.FullName, ct);
                buffer.Clear();
            }
        }

        _logger.LogInformation("Finished {File}: {Rows:n0} rows, {Errors:n0} errors",
            entry.FullName, totalRows, totalErrors);
    }
}
