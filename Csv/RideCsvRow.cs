namespace CitiBikeNYC.Csv;

public sealed class RideCsvRow
{
    public string RideId { get; set; } = default!;
    public string RideableType { get; set; } = default!;

    public DateTime StartedAt { get; set; }
    public DateTime EndedAt { get; set; }

    public string? StartStationName { get; set; }
    public string? StartStationId { get; set; }

    public string? EndStationName { get; set; }
    public string? EndStationId { get; set; }

    public double? StartLat { get; set; }
    public double? StartLng { get; set; }

    public double? EndLat { get; set; }
    public double? EndLng { get; set; }

    public string? MemberCasual { get; set; }
}
