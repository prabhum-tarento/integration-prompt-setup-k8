namespace IIS.WMS.Consumer.Application.Exceptions;

/// <summary>Raised when a use case can't locate the requested resource. Mapped to <c>404 Not Found</c> at the Api boundary.</summary>
/// <param name="resourceName">Name of the resource type that was not found (e.g. <c>nameof(InventoryEvent)</c>).</param>
/// <param name="resourceId">Identifier that was looked up and not found.</param>
public sealed class NotFoundException(string resourceName, string resourceId)
    : Exception($"{resourceName} '{resourceId}' was not found.")
{
    /// <summary>Name of the resource type that was not found.</summary>
    public string ResourceName { get; } = resourceName;

    /// <summary>Identifier that was looked up and not found.</summary>
    public string ResourceId { get; } = resourceId;
}
