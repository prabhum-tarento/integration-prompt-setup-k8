namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Kafka message header names this consumer reads from/writes to the upstream producer's contract -
/// matching the header names the same upstream Nexus/WMS event producers already use elsewhere (see
/// <c>ApplicationHeaders</c> in the sibling iis-reflex-wms-facade repo), not this repo's own
/// invention. <see cref="CorrelationId"/> replaces the previous ad hoc <c>"correlationId"</c> header
/// name <see cref="ConsumerHostedService"/> read before this change - that name didn't match
/// what the producer actually sends, so the correlation id fallback (a fresh GUID) was silently
/// firing on every message.
/// </summary>
public static class KafkaHeaderNames
{
    /// <summary>Correlation id carried end to end across Kafka → Service Bus (integration-resiliency.instructions.md §4).</summary>
    public const string CorrelationId = "Correlation-Id";

    /// <summary>Deterministic id used for the deduplication check (integration-resiliency.instructions.md §1) - required for the dedup check to run at all.</summary>
    public const string DeduplicationId = "Deduplication-Id";

    /// <summary>Identifies the upstream application/facade that produced the message.</summary>
    public const string AppId = "App-Id";

    /// <summary>Discriminates the event's type/schema.</summary>
    public const string Type = "Type";
}
