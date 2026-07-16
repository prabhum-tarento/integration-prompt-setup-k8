namespace IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;

/// <summary>Request to create a new event validation template.</summary>
/// <param name="SchemaName">Schema the template applies to - becomes the folder segment of the <c>{SchemaName}/{EventType}.cs</c> blob path.</param>
/// <param name="EventType">Kafka <c>Type</c> header value the template applies to (e.g. <c>inventory.InventoryStateChanged</c>) - becomes the file segment of the blob path.</param>
/// <param name="Code">The C# validation script - must compile against the script contract (<c>x</c>, <c>header</c>, <c>_log</c> globals) before it is stored.</param>
public sealed record CreateEventValidationTemplateRequest(string SchemaName, string EventType, string Code);
