namespace IIS.WMS.Consumer.Infrastructure.BlobStorage;

/// <summary>Bound from the <c>BlobStorage</c> configuration section (integration-resiliency.instructions.md §5).</summary>
public sealed class BlobStorageOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "BlobStorage";

    /// <summary>Blob Storage account URI.</summary>
    public string AccountUri { get; init; } = default!;

    /// <summary>Feature flag for the optional cold-tier request/response audit log - off unless explicitly enabled per environment.</summary>
    public bool RequestAuditEnabled { get; init; }
}
