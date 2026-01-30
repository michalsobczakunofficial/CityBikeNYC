using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.TypeConversion;
using System.Globalization;

namespace CitiBikeNYC.Csv;

public sealed class LenientDateTimeConverter : DefaultTypeConverter
{
    private static readonly string[] Formats = new[]
    {
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss.FFF",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.FFF",
        "yyyy-MM-dd'T'HH:mm:ssK",
        "yyyy-MM-dd'T'HH:mm:ss.FFFK"
    };

    public override object? ConvertFromString(string? text, IReaderRow row, MemberMapData memberMapData)
    {
        if (string.IsNullOrWhiteSpace(text))
            throw new TypeConverterException(this, memberMapData, text, row.Context, "Empty datetime");

        var s = text.Trim();

        if (DateTime.TryParseExact(s, Formats, CultureInfo.InvariantCulture,
                DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.RoundtripKind, out var dtExact))
            return dtExact;

        if (DateTime.TryParse(s, CultureInfo.InvariantCulture,
                DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.RoundtripKind, out var dt))
            return dt;

        throw new TypeConverterException(this, memberMapData, text, row.Context, $"Failed to parse datetime: '{text}'");
    }
}
