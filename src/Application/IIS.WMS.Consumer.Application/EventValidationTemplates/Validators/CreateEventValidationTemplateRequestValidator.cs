using FluentValidation;
using IIS.WMS.Common.DynamicValidation;
using IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;

namespace IIS.WMS.Consumer.Application.EventValidationTemplates.Validators;

/// <summary>Shape validation for <see cref="CreateEventValidationTemplateRequest"/>, run by <c>ValidationFilter</c> before the controller action executes.</summary>
public sealed class CreateEventValidationTemplateRequestValidator : AbstractValidator<CreateEventValidationTemplateRequest>
{
    /// <summary>
    /// Blob-path-segment shape <see cref="CreateEventValidationTemplateRequest.Identifier"/> must
    /// satisfy: starts with a letter or digit, then letters/digits/dots/hyphens/underscores. No
    /// <c>/</c> - it's exactly one segment of the <c>{Transport}/{Identifier}.cs</c> blob path, so a
    /// slash would let a request escape into another template's path.
    /// </summary>
    internal const string PathSegmentPattern = "^[A-Za-z0-9][A-Za-z0-9._-]*$";

    /// <summary>Longest accepted script, in characters - far below Blob Storage's own limits; a validation rule has no business being this large.</summary>
    internal const int MaxCodeLength = 65_536;

    /// <summary>
    /// Declares the field-level rules: <c>Transport</c> must be one of
    /// <see cref="DynamicValidationTransports.All"/> - a template stored under any other folder would
    /// never be looked up by either consumer - <c>Identifier</c> is a required single path segment,
    /// and code is required and bounded.
    /// </summary>
    public CreateEventValidationTemplateRequestValidator()
    {
        RuleFor(x => x.Transport).NotEmpty().Must(DynamicValidationTransports.All.Contains)
            .WithMessage($"'Transport' must be one of: {string.Join(", ", DynamicValidationTransports.All)}.");
        RuleFor(x => x.Identifier).NotEmpty().MaximumLength(256).Matches(PathSegmentPattern);
        RuleFor(x => x.Code).NotEmpty().MaximumLength(MaxCodeLength);
    }
}
