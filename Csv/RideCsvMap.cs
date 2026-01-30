using CsvHelper.Configuration;

namespace CitiBikeNYC.Csv;

public sealed class RideCsvMap : ClassMap<RideCsvRow>
{
    public RideCsvMap()
    {
        Map(m => m.RideId).Name("ride_id");
        Map(m => m.RideableType).Name("rideable_type");

        Map(m => m.StartedAt).Name("started_at").TypeConverter<LenientDateTimeConverter>();
        Map(m => m.EndedAt).Name("ended_at").TypeConverter<LenientDateTimeConverter>();

        Map(m => m.StartStationName).Name("start_station_name");
        Map(m => m.StartStationId).Name("start_station_id");

        Map(m => m.EndStationName).Name("end_station_name");
        Map(m => m.EndStationId).Name("end_station_id");

        Map(m => m.StartLat).Name("start_lat").TypeConverter<NullableDoubleConverter>();
        Map(m => m.StartLng).Name("start_lng").TypeConverter<NullableDoubleConverter>();

        Map(m => m.EndLat).Name("end_lat").TypeConverter<NullableDoubleConverter>();
        Map(m => m.EndLng).Name("end_lng").TypeConverter<NullableDoubleConverter>();

        Map(m => m.MemberCasual).Name("member_casual");
    }
}
