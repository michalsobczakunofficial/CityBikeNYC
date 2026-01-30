namespace CitiBikeNYC.Utils;

public static class StringUtil
{
    public static string? NullIfWhiteSpace(string? s)
        => string.IsNullOrWhiteSpace(s) ? null : s.Trim();

    public static string Truncate(string s, int maxLen)
        => s.Length <= maxLen ? s : s.Substring(0, maxLen);
}
