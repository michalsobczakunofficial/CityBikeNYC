namespace CitiBikeNYC.Domain;

public static class MemberTypeExtensions
{
    public static MemberType FromCsv(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return MemberType.Unknown;

        return value.Trim().ToLowerInvariant() switch
        {
            "member" => MemberType.Member,
            "casual" => MemberType.Casual,
            _ => MemberType.Unknown
        };
    }
}
