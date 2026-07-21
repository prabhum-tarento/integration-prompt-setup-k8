using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="IInventoryEventRepository"/>
/// <remarks>
/// Not <see langword="sealed"/>: an integration-test-only subclass overrides
/// <see cref="CosmosRepository{TDomain,TDocument}.ReadNextPageAsync{T}"/> to run the paged/projected query
/// methods against an in-memory <see cref="ICosmosContainerFactory"/> fake (integration-resiliency.instructions.md
/// §9) - production behavior is unaffected, since that method's default implementation is unchanged.
/// </remarks>
public class InventoryEventRepository : CosmosRepository<InventoryEvent, InventoryEventDocument>, IInventoryEventRepository
{
    public InventoryEventRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<InventoryEventRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(CosmosContainerNames.InventoryEvents, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <inheritdoc />
    protected override InventoryEventDocument ToDocument(InventoryEvent domain) => InventoryEventMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override InventoryEvent ToDomain(InventoryEventDocument document) => InventoryEventMapper.ToDomain(document);
}
