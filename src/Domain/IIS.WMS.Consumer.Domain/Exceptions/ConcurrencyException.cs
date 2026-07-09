namespace IIS.WMS.Consumer.Domain.Exceptions;

/// <summary>
/// Raised when a write's ETag no longer matches the stored item - another writer updated the
/// aggregate first (see cosmos-db.instructions.md §9). For a message-driven write, the caller
/// re-reads and re-applies against the fresh ETag (integration-resiliency.instructions.md §2)
/// rather than treating this as fatal; for an HTTP write, the Api's global exception handler
/// maps it to <c>409 Conflict</c>.
/// </summary>
public sealed class ConcurrencyException : DomainException
{
    /// <summary>Builds the exception with a message identifying the conflicting item and the ETag the caller expected.</summary>
    /// <param name="id">Id of the item that failed the concurrency check.</param>
    /// <param name="expectedETag">ETag the caller expected to still match.</param>
    public ConcurrencyException(string id, string expectedETag)
        : base($"Concurrent modification detected for item '{id}' (expected ETag '{expectedETag}').")
    {
        Id = id;
        ExpectedETag = expectedETag;
    }

    /// <summary>Id of the item that failed the concurrency check.</summary>
    public string Id { get; }

    /// <summary>ETag the caller expected to still match.</summary>
    public string ExpectedETag { get; }
}
