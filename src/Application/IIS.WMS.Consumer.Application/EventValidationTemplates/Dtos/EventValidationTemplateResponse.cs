namespace IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;

/// <summary>One stored event validation template, including its C# code.</summary>
/// <param name="Transport">The transport the template applies to - the folder segment of the blob path.</param>
/// <param name="Identifier">The transport-specific lookup key (Kafka <c>Type</c> header value, or Service Bus queue name) - the file segment of the blob path.</param>
/// <param name="Code">The template's C# validation script.</param>
public sealed record EventValidationTemplateResponse(string Transport, string Identifier, string Code);
