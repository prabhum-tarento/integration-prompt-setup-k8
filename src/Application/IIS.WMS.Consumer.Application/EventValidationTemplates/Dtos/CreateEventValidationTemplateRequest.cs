namespace IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;

/// <summary>Request to create a new event validation template.</summary>
/// <param name="Transport">The transport the template applies to (<c>Kafka</c> or <c>ServiceBus</c>) - becomes the folder segment of the <c>{Transport}/{Identifier}.cs</c> blob path.</param>
/// <param name="Identifier">The transport-specific lookup key - the Kafka <c>Type</c> header value (e.g. <c>inventory.InventoryStateChanged</c>) for a Kafka template, or the queue name for a Service Bus template - becomes the file segment of the blob path.</param>
/// <param name="Code">The C# validation script - must compile against the script contract (<c>x</c>, <c>header</c>, <c>_log</c> globals) before it is stored.</param>
public sealed record CreateEventValidationTemplateRequest(string Transport, string Identifier, string Code);
