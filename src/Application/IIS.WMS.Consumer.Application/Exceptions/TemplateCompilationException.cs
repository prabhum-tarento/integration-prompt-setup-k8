namespace IIS.WMS.Consumer.Application.Exceptions;

/// <summary>
/// Raised when an event validation template's C# code fails to compile - either while saving a
/// template through the API (mapped to <c>400 Bad Request</c> at the Api boundary, with
/// <see cref="Errors"/> in the response so the caller can fix the code) or while loading a stored
/// template in the Kafka consumer's dynamic-validation step (a hard validation failure - the message
/// dead-letters, same as any validator throw).
/// </summary>
/// <param name="templateName">The template the code belongs to, e.g. <c>InventoryStateChangedEvent/inventory.InventoryStateChanged</c>.</param>
/// <param name="errors">One entry per compiler error diagnostic.</param>
public sealed class TemplateCompilationException(string templateName, IReadOnlyList<string> errors)
    : Exception($"Validation template '{templateName}' failed to compile: {string.Join("; ", errors)}")
{
    /// <summary>The template the failing code belongs to.</summary>
    public string TemplateName { get; } = templateName;

    /// <summary>One entry per compiler error diagnostic.</summary>
    public IReadOnlyList<string> Errors { get; } = errors;
}
