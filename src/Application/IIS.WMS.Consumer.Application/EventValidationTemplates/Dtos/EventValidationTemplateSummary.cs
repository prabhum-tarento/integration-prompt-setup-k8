namespace IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;

/// <summary>One stored event validation template's identity, without its code - the list-endpoint shape.</summary>
/// <param name="Transport">The transport the template applies to - the folder segment of the blob path.</param>
/// <param name="Identifier">The transport-specific lookup key (Kafka <c>Type</c> header value, or Service Bus queue name) - the file segment of the blob path.</param>
public sealed record EventValidationTemplateSummary(string Transport, string Identifier);
