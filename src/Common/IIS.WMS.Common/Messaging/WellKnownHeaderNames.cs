namespace IIS.WMS.Common.Messaging;

/// <summary>
/// Well-known message header names shared across every transport this repo consumes/produces -
/// matching the header names the same upstream Nexus/WMS event producers already use elsewhere (see
/// <c>ApplicationHeaders</c> in the sibling iis-reflex-wms-facade repo), not this repo's own
/// invention. Originally Kafka-only (<c>KafkaHeaderNames</c>), moved here and renamed because these
/// names are already read/written outside any Kafka context - e.g. <c>NexusDeduplicationService</c>
/// sends <see cref="AppId"/> as an outbound HTTP header - and because the dynamic-validation script
/// contract (<c>IIS.WMS.Common.DynamicValidation</c>) needs them nameable without a Kafka reference.
/// </summary>
public static class WellKnownHeaderNames
{
    /// <summary>Correlation id carried end to end across Kafka → Service Bus (integration-resiliency.instructions.md §4).</summary>
    public const string CorrelationId = "Correlation-Id";

    /// <summary>Deterministic id used for the deduplication check (integration-resiliency.instructions.md §1) - required for the dedup check to run at all.</summary>
    public const string DeduplicationId = "Deduplication-Id";

    /// <summary>Identifies the upstream application/facade that produced the message.</summary>
    public const string AppId = "App-Id";

    public const string EventKey = "Id";

    /// <summary>Discriminates the event's type/schema.</summary>
    public const string Type = "Type";
}
