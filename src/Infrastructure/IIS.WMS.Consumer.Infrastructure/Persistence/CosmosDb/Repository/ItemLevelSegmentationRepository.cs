using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="IItemLevelSegmentationRepository"/>
/// <remarks>
/// Not <see langword="sealed"/>: an integration-test-only subclass overrides
/// <see cref="CosmosRepository{TDomain,TDocument}.ReadNextPageAsync{T}"/> to run the paged/projected query
/// methods against an in-memory <see cref="ICosmosContainerFactory"/> fake (integration-resiliency.instructions.md
/// §9) - production behavior is unaffected, since that method's default implementation is unchanged.
/// </remarks>
public class ItemLevelSegmentationRepository : CosmosRepository<ItemLevelSegmentation, ItemLevelSegmentationDocument>, IItemLevelSegmentationRepository
{
    public ItemLevelSegmentationRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<ItemLevelSegmentationRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(CosmosContainerNames.Rules, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <inheritdoc />
    public async Task<ItemLevelSegmentation?> GetItemLevelFulfilmentyByCategory(
        string fulfilment, string hallMarkType, string itemCode, string coo)
    {
        var category = $"SEG_ITEM_{fulfilment}_{hallMarkType}";
        var id = $"{itemCode}_{coo}";
        return await GetAsync(category, id);
    }

    /// <inheritdoc />
    protected override ItemLevelSegmentationDocument ToDocument(ItemLevelSegmentation domain) =>
        ItemLevelSegmentationMapper.ToDocument(domain);

    /// <inheritdoc />
    protected override ItemLevelSegmentation ToDomain(ItemLevelSegmentationDocument document) =>
        ItemLevelSegmentationMapper.ToDomain(document);
}
