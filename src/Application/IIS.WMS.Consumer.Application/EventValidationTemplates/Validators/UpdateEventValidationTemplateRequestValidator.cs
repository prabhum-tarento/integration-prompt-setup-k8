using FluentValidation;
using IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;

namespace IIS.WMS.Consumer.Application.EventValidationTemplates.Validators;

/// <summary>Shape validation for <see cref="UpdateEventValidationTemplateRequest"/>, run by <c>ValidationFilter</c> before the controller action executes.</summary>
public sealed class UpdateEventValidationTemplateRequestValidator : AbstractValidator<UpdateEventValidationTemplateRequest>
{
    /// <summary>Declares the field-level rules: code is required and bounded - the template's identity is validated by the service against the route values.</summary>
    public UpdateEventValidationTemplateRequestValidator()
    {
        RuleFor(x => x.Code).NotEmpty().MaximumLength(CreateEventValidationTemplateRequestValidator.MaxCodeLength);
    }
}
