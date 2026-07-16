namespace IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;

/// <summary>One stored event validation template's identity, without its code - the list-endpoint shape.</summary>
/// <param name="SchemaName">Schema the template applies to - the folder segment of the blob path.</param>
/// <param name="EventType">Kafka <c>Type</c> header value the template applies to - the file segment of the blob path.</param>
public sealed record EventValidationTemplateSummary(string SchemaName, string EventType);
