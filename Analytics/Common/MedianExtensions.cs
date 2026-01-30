namespace CitiBikeNYC.Analytics.Common;

public static class MedianExtensions
{
    public static double Median(this IEnumerable<double> source)
    {
        var data = source.OrderBy(x => x).ToArray();
        if (data.Length == 0) return double.NaN;

        int mid = data.Length / 2;
        return (data.Length % 2 == 1)
            ? data[mid]
            : (data[mid - 1] + data[mid]) / 2.0;
    }
}
