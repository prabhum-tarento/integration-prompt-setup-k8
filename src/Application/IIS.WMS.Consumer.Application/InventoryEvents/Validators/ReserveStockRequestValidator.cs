using FluentValidation;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

namespace IIS.WMS.Consumer.Application.InventoryEvents.Validators;

/// <summary>Shape validation for <see cref="ReserveStockRequest"/>, run by <c>ValidationFilter</c> before the controller action executes.</summary>
public sealed class ReserveStockRequestValidator : AbstractValidator<ReserveStockRequest>
{
    /// <summary>Declares the field-level rules: reservation id is required and bounded in length; quantity must be positive.</summary>
    public ReserveStockRequestValidator()
    {
        RuleFor(x => x.ReservationId).NotEmpty().MaximumLength(128);
        RuleFor(x => x.Quantity).GreaterThan(0);
    }
}
