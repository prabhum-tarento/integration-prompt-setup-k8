using FluentValidation;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

namespace IIS.WMS.Consumer.Application.InventoryEvents.Validators;

/// <summary>Shape validation for <see cref="CreateInventoryEventRequest"/>, run by <c>ValidationFilter</c> before the controller action executes.</summary>
public sealed class CreateInventoryEventRequestValidator : AbstractValidator<CreateInventoryEventRequest>
{
    /// <summary>Declares the field-level rules: warehouse/SKU are required and bounded in length; initial quantity can't be negative.</summary>
    public CreateInventoryEventRequestValidator()
    {
        RuleFor(x => x.WarehouseId).NotEmpty().MaximumLength(64);
        RuleFor(x => x.Sku).NotEmpty().MaximumLength(64);
        RuleFor(x => x.InitialQuantity).GreaterThanOrEqualTo(0);
    }
}
