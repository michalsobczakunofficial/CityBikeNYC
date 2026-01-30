namespace CitiBikeNYC.Domain;

public enum ImportErrorCode
{
    Unknown = 0,
    MissingRideId,
    DateParseFailed,
    EndBeforeStart,
    CsvBadData
}
