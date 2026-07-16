namespace IIS.WMS.Consumer.Application.Exceptions;

/// <summary>Raised when a use case can't create a resource because one already exists under the same identity. Mapped to <c>409 Conflict</c> at the Api boundary.</summary>
/// <param name="resourceName">Name of the resource type that already exists.</param>
/// <param name="resourceId">Identifier the conflicting resource already exists under.</param>
public sealed class ConflictException(string resourceName, string resourceId)
    : Exception($"{resourceName} '{resourceId}' already exists.")
{
    /// <summary>Name of the resource type that already exists.</summary>
    public string ResourceName { get; } = resourceName;

    /// <summary>Identifier the conflicting resource already exists under.</summary>
    public string ResourceId { get; } = resourceId;
}
