using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderStatusChanged;

/// <summary>
/// This consumer's own decoupled wire contract for an <c>OrderStatusChanged</c> event - mirrors
/// <c>net.pandora.nexus.event.b2b.sales.OrderStatusChanged</c> (the Avro-generated SpecificRecord from
/// the NexusFacades.Common.AvroSchemas package) field-for-field, but as a plain type with no Avro
/// codegen ties, same rationale as <see cref="InventoryStateChanged.InventoryStateChangedEvent"/>.
/// Reuses <see cref="InventoryEventChannel"/> for <see cref="Channel"/> since the underlying Avro
/// <c>net.pandora.nexus.shared.Channel</c> shape is identical to the one <c>InventoryStateChanged</c>
/// already consumes (docs/events/b2b.sales.OrderStatusChanged.md §7).
/// </summary>
public sealed record OrderStatusChangedEvent(
    InventoryEventChannel Channel,
    string? Market,
    string? SellingLegalEntity,
    string OrderId,
    string? BackOrderId,
    string? PickingRouteId,
    OrderStatusCode Status,
    string WarehouseCode,
    bool? IsReturn,
    DateTime ChangeDate,
    string? CancelReason,
    string? SourceOrderReferenceId);

/// <summary>This event's own OMS status code (docs/events/b2b.sales.OrderStatusChanged.md §3.3) - distinct from <see cref="Application.OrderTracking.Dtos.OrderTrackingStatus"/>, which is what it's mapped to.</summary>
public enum OrderStatusCode
{
    Unknown,
    Deactivated,
    NotRun,
    Run,
    CollectionStarted,
    CollectionPerformed,
    PreparationInProgress,
    ToPackage,
    Completed,
    Despatched,
    Cancelled,
    Deleted,
    OrderCanceled,
    CreditBlocked,
    CreditUnblocked,
}
