using FluentValidation;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka.AvroContracts;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka.Validators;

/// <summary>
/// Field-level validation for one deserialized <see cref="BulkInventoryImportEvent"/>, run by
/// <see cref="BulkInventoryImportConsumerHostedService"/>'s <c>ValidateAsync</c> override - this is
/// the single validation point for bulk-import data (integration-resiliency.instructions.md §1); the
/// Application-layer request is not re-validated a second time downstream, since by the time it
/// reaches there this has already passed. A message failing these rules is handled like any other
/// non-fatal failure - hot-tier dead-letter, offset committed forward, never blocking the partition.
/// </summary>
public sealed class BulkInventoryImportEventValidator : AbstractValidator<BulkInventoryImportEvent>
{
    /// <summary>Declares the field-level rules.</summary>
    public BulkInventoryImportEventValidator()
    {
        RuleFor(x => x.EventId).NotEmpty();
        RuleFor(x => x.WarehouseId).NotEmpty().MaximumLength(64);
        RuleFor(x => x.Sku).NotEmpty().MaximumLength(64);
        RuleFor(x => x.Quantity).GreaterThanOrEqualTo(0);
        RuleFor(x => x.SourceSystem).NotEmpty().MaximumLength(64);
        RuleFor(x => x.LastUpdatedUtcMillis).GreaterThan(0);
    }
}
