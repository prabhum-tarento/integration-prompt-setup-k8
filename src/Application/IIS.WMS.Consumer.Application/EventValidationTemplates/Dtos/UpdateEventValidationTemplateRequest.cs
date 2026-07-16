namespace IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;

/// <summary>Request to replace an existing event validation template's code - the template's identity comes from the route.</summary>
/// <param name="Code">The replacement C# validation script - must compile against the script contract (<c>x</c>, <c>header</c>, <c>_log</c> globals) before it is stored.</param>
public sealed record UpdateEventValidationTemplateRequest(string Code);
