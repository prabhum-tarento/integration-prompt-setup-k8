namespace IIS.WMS.Consumer.Application.Common;

/// <summary>
/// Checks whether an inbound Kafka message has already been processed, before it is deserialized or
/// relayed to Service Bus (integration-resiliency.instructions.md §1). Backed by the external Nexus
/// deduplication API - see <c>NexusDeduplicationService</c> in Infrastructure.
/// </summary>
public interface IDeduplicationService
{
    /// <summary>
    /// Checks whether a message has already been processed. The implementation owns how
    /// <paramref name="consumerName"/>, <paramref name="deduplicationId"/>, and
    /// <paramref name="correlationId"/> combine into whatever key the backing dedup store actually
    /// uses - callers only need to supply values that are stable across redelivery of the same
    /// logical event, not construct the key themselves.
    /// </summary>
    /// <param name="consumerName">Display name of the calling consumer - scopes the dedup key so two different consumers don't collide on the same raw id.</param>
    /// <param name="deduplicationId">
    /// Deterministic id for this message from the Kafka message headers (e.g. a
    /// <c>Deduplication-Id</c> header) - never derived from the deserialized body, since the dedup
    /// check runs before deserialization.
    /// </param>
    /// <param name="correlationId">Correlation id for the message being checked, carried through for tracing on the dedup service's side.</param>
    /// <param name="cancellationToken">Token to cancel the call.</param>
    /// <returns><see langword="true"/> if this id has already been seen (a duplicate); otherwise <see langword="false"/>.</returns>
    Task<bool> IsDuplicateAsync(
        string consumerName, string deduplicationId, string correlationId, CancellationToken cancellationToken = default);
}
