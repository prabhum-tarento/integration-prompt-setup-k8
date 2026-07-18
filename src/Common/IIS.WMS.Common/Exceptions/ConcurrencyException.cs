namespace IIS.WMS.Common.Exceptions;

/// <summary>
/// Signals an optimistic-concurrency conflict at a storage write boundary (e.g. a Cosmos DB ETag
/// mismatch) - a cross-cutting infrastructure-conflict signal, not a Domain-owned business invariant
/// violation (see dotnet-architecture-good-practices.instructions.md), which is why it lives here
/// alongside <see cref="IIS.WMS.Common.DynamicValidation.TemplateCompilationException"/> rather than
/// in the Domain layer.
/// </summary>
public sealed class ConcurrencyException : Exception
{
    public ConcurrencyException(string id, string expectedETag)
        : base($"Concurrent modification detected for item '{id}' (expected ETag '{expectedETag}').")
    {
        Id = id;
        ExpectedETag = expectedETag;
    }

    public string Id { get; }

    public string ExpectedETag { get; }
}
