namespace CitiBikeNYC.Domain;

public sealed class ImportError
{
    public long Id { get; set; }

    public string SourceFile { get; set; } = default!;
    public int RowNumber { get; set; }

    public ImportErrorCode ErrorCode { get; set; }

    public string RawLine { get; set; } = default!;
    public DateTime OccurredAtUtc { get; set; }

    public string? RideId { get; set; }
}
