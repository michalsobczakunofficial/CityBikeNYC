namespace CitiBikeNYC.Domain;

public sealed class Station
{
    public string StationId { get; set; } = default!;
    public string? Name { get; set; }

    public double? Lat { get; set; }
    public double? Lng { get; set; }

    public DateTime FirstSeenAt { get; set; }
    public DateTime LastSeenAt { get; set; }

    public List<Ride> StartRides { get; set; } = new();
    public List<Ride> EndRides { get; set; } = new();
}
