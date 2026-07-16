using FluentValidation;
using IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;

namespace IIS.WMS.Consumer.Application.EventValidationTemplates.Validators;

/// <summary>Shape validation for <see cref="CreateEventValidationTemplateRequest"/>, run by <c>ValidationFilter</c> before the controller action executes.</summary>
public sealed class CreateEventValidationTemplateRequestValidator : AbstractValidator<CreateEventValidationTemplateRequest>
{
    /// <summary>
    /// Blob-path-segment shape both identity fields must satisfy: starts with a letter or digit,
    /// then letters/digits/dots/hyphens/underscores. No <c>/</c> - each field is exactly one segment
    /// of the <c>{SchemaName}/{EventType}.cs</c> blob path, so a slash would let a request escape
    /// into another template's path.
    /// </summary>
    internal const string PathSegmentPattern = "^[A-Za-z0-9][A-Za-z0-9._-]*$";

    /// <summary>Longest accepted script, in characters - far below Blob Storage's own limits; a validation rule has no business being this large.</summary>
    internal const int MaxCodeLength = 65_536;

    /// <summary>Declares the field-level rules: schema/event type are required single path segments; code is required and bounded.</summary>
    public CreateEventValidationTemplateRequestValidator()
    {
        RuleFor(x => x.SchemaName).NotEmpty().MaximumLength(128).Matches(PathSegmentPattern);
        RuleFor(x => x.EventType).NotEmpty().MaximumLength(256).Matches(PathSegmentPattern);
        RuleFor(x => x.Code).NotEmpty().MaximumLength(MaxCodeLength);
    }
}
