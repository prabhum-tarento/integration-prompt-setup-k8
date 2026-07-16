namespace IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;

/// <summary>One worked example of a validation template - served by the examples endpoint as living documentation of the script contract.</summary>
/// <param name="Title">Short name of the pattern the example demonstrates.</param>
/// <param name="Description">What the example shows, including which injected script objects (<c>x</c>, <c>header</c>, <c>_log</c>) it uses and how its outcome is handled by the consumer.</param>
/// <param name="Code">Ready-to-store C# script - every example compiles against the real script contract.</param>
public sealed record EventValidationTemplateExample(string Title, string Description, string Code);
