namespace IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;

/// <summary>One stored event validation template, including its C# code.</summary>
/// <param name="SchemaName">Schema the template applies to - the folder segment of the blob path.</param>
/// <param name="EventType">Kafka <c>Type</c> header value the template applies to - the file segment of the blob path.</param>
/// <param name="Code">The template's C# validation script.</param>
public sealed record EventValidationTemplateResponse(string SchemaName, string EventType, string Code);
