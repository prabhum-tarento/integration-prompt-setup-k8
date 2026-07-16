namespace IIS.WMS.Consumer.Infrastructure.NexusServices;

/// <summary>
/// Bound from the <c>Nexus:Deduplication</c> configuration section - settings for the external Nexus
/// deduplication API called from the Kafka consumer's dedup check
/// (integration-resiliency.instructions.md §1). <see cref="ClientSecret"/> is never set in
/// <c>appsettings.json</c> - local development reads it from user-secrets, every other environment
/// from Azure Key Vault, per engineering-standards.instructions.md §6. Whether the check runs at all
/// is controlled solely by <see cref="Messaging.Kafka.ConsumerOptions.DeduplicationCheckEnabled"/>
/// (<c>Kafka:DeduplicationCheckEnabled</c>) - there is no separate on/off switch here. This facade's
/// identity, sent as the <c>App-Id</c> header on every request and folded into the composite
/// deduplication id (see <c>NexusDeduplicationService</c>), is <see cref="ApplicationOptions.AppId"/>
/// (<c>Application:AppId</c>) - again, no separate copy here.
/// </summary>
public sealed class NexusDeduplicationOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "Nexus:Deduplication";

    /// <summary>Base address of the Nexus deduplication API - the dedup check is a single <c>POST</c> to this address.</summary>
    public string BaseUrl { get; init; } = default!;

    /// <summary>Nexus OAuth2 client-credentials token endpoint.</summary>
    public string OAuthEndpoint { get; init; } = default!;

    /// <summary>OAuth2 client id for the client-credentials grant.</summary>
    public string ClientId { get; init; } = default!;

    /// <summary>OAuth2 client secret for the client-credentials grant - user-secrets/Key Vault only, see type-level remarks.</summary>
    public string ClientSecret { get; init; } = default!;

    /// <summary>OAuth2 scope requested for the token used against the deduplication API.</summary>
    public string Scope { get; init; } = default!;
}
