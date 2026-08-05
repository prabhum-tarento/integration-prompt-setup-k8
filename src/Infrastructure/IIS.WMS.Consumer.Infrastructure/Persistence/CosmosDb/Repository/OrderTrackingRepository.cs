using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Mapper;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Shared;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="IOrderTrackingRepository"/>
public sealed class OrderTrackingRepository : CosmosRepository<OrderTracking, OrderTrackingDocument>, IOrderTrackingRepository
{
    public OrderTrackingRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<OrderTrackingRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <summary>
    /// Resolves the per-fulfilment-code container for <paramref name="category"/>, whose first
    /// <c>:</c>-delimited segment is expected to be the fulfilment/warehouse code - mirrors
    /// <see cref="ItemStockInventoryRepository"/>.
    /// TODO(ai): unresolved precedence conflict - docs/events/b2b.sales.ConsolidatedOrderShipped.md
    /// §5.4 documents OrderTracking as a point read keyed by "CustomerId, OrderId, ShipmentId, Status"
    /// but never states the exact Id/Category composite key shape. Assuming the caller builds
    /// category as "{WarehouseCode}:{ParentOrderId}" (mirroring ItemStockInventory.BuildId's
    /// fulfilment-code-first convention) so this repository's per-fulfilment-code container split
    /// (Q3) can route correctly - review against the actual OrderTracking container schema before
    /// shipping.
    /// </summary>
    protected override string ResolveContainerName(string? category) =>
        category is null
            ? throw new NotSupportedException(
                $"{nameof(OrderTrackingRepository)} has no single container to scan across " +
                "fulfilment codes - cross-partition queries are not supported.")
            : CosmosContainerNames.GetOrderTrackingContainerName(ExtractFulfilmentCode(category));

    /// <summary>Extracts the fulfilment/warehouse code - the first <c>:</c>-delimited segment - from the category/id.</summary>
    private static string ExtractFulfilmentCode(string category)
    {
        var separatorIndex = category.IndexOf(':');
        return separatorIndex < 0 ? category : category[..separatorIndex];
    }

    /// <inheritdoc />
    protected override OrderTrackingDocument ToDocument(OrderTracking domain) => OrderTrackingMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override OrderTracking ToDomain(OrderTrackingDocument document) => OrderTrackingMapper.ToDomain(document);
}
