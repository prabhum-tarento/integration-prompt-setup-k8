using IIS.WMS.Consumer.Domain.Common;

namespace IIS.WMS.Consumer.Domain.Aggregates;

/// <summary>
/// Read side of the order-tracking record consulted by the DEECOMDC e-commerce engraving workflow
/// to resolve a shipment's <see cref="CustomerId"/> (docs/events/b2b.sales.ConsolidatedOrderShipped.md
/// §3.3 step 1, §5.4). Owned by a downstream consumer of the published order-tracking request - this
/// event only reads it, so no mutator methods are exposed here.
/// </summary>
public sealed class OrderTracking : AggregateRoot
{
    /// <summary>The composite key - matches the Cosmos partition key.</summary>
    public string Category { get; private init; } = default!;

    /// <summary>Customer identifier resolved for this order (compared against the ECOMDCLIST allow-list / TDCCustomerId).</summary>
    public string? CustomerId { get; private init; }

    /// <summary>Order this tracking record belongs to.</summary>
    public string OrderId { get; private init; } = default!;

    /// <summary>Shipment this tracking record belongs to.</summary>
    public string? ShipmentId { get; private init; }

    /// <summary>Current tracking status.</summary>
    public string? Status { get; private init; }

    /// <summary>Parameterless so the object initializer in <see cref="Rehydrate"/> can set the init-only properties.</summary>
    private OrderTracking()
    {
    }

    /// <summary>Rehydrates an aggregate from persisted state - the repository mapper's entry point.</summary>
    public static OrderTracking Rehydrate(
        string id, string category, string orderId, string? customerId, string? shipmentId, string? status) => new()
    {
        Id = id,
        Category = category,
        OrderId = orderId,
        CustomerId = customerId,
        ShipmentId = shipmentId,
        Status = status,
    };
}
