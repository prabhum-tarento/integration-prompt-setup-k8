namespace IIS.WMS.Consumer.Domain.Enums;

/// <summary>
/// Domain-owned mirror of the shipment confirmation type used by internal-hallmarking's PICKED-path
/// consolidated-shipment logic (docs/events/inventory.InternalHallmarkingStatusChanged.md §3.3). Values
/// match the <c>ConfirmationType</c> enum in the unrelated <c>b2b.sales.ConsolidatedOrderShipped</c>
/// Avro schema - kept as a separate Domain-level enum rather than referencing that Avro-generated type
/// directly, since Domain must not depend on Infrastructure/other-event wire contracts.
/// </summary>
public enum ConfirmationType
{
    UNKNOWN,
    PRELIMINARY,
    STANDARD,
    STANDARD_FOLLOWING_PRELIMINARY,
    PRELIMINARY_INVOICE,
    PRELIMINARY_EXPORT
}
