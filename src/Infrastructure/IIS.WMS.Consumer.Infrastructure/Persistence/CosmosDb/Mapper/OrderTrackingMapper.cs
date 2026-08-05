using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;

/// <summary>Maps between the <see cref="OrderTracking"/> Domain aggregate and its Cosmos persistence document - the only place either type's shape needs to be known together.</summary>
internal static class OrderTrackingMapper
{
    /// <summary>
    /// Projects an aggregate's current state into the persistence shape - required by
    /// <c>CosmosRepository&lt;,&gt;</c>'s abstract contract, but never exercised in practice since this
    /// event's <see cref="OrderTracking"/> access is read-only (§5.4; the status write belongs to a
    /// downstream consumer).
    /// </summary>
    public static OrderTrackingDocument ToDocument(OrderTracking aggregate) => new()
    {
        Id = aggregate.Id,
        Category = aggregate.Category,
        OrderId = aggregate.OrderId,
        CustomerId = aggregate.CustomerId,
        ShipmentId = aggregate.ShipmentId,
        Status = aggregate.Status,
    };

    /// <summary>Rehydrates a Domain aggregate from a document read back from Cosmos.</summary>
    public static OrderTracking ToDomain(OrderTrackingDocument document) =>
        OrderTracking.Rehydrate(
            document.Id,
            document.Category,
            document.OrderId,
            document.CustomerId,
            document.ShipmentId,
            document.Status);
}
