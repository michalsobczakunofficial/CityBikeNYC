namespace CitiBikeNYC.Domain;

public sealed class Ride
{
    public string RideId { get; set; } = default!;
    public string? RideableType { get; set; }

    public DateTime StartedAt { get; set; }
    public DateTime EndedAt { get; set; }

    public string? StartStationId { get; set; }
    public Station? StartStation { get; set; }

    public string? EndStationId { get; set; }
    public Station? EndStation { get; set; }

    public double? StartLat { get; set; }
    public double? StartLng { get; set; }
    public double? EndLat { get; set; }
    public double? EndLng { get; set; }

    public MemberType MemberType { get; set; }

    public long DurationSeconds => (long)(EndedAt - StartedAt).TotalSeconds;
}
