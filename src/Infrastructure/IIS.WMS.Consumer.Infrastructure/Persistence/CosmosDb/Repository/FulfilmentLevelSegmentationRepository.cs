using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Audit;
using IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Entity;

namespace IIS.WMS.Consumer.Infrastructure.Persistence.CosmosDb.Repository;

/// <inheritdoc cref="IFulfilmentLevelSegmentationRepository"/>
public sealed class FulfilmentLevelSegmentationRepository : CosmosRepository<FulfilmentLevelSegmentation, FulfilmentLevelSegmentationDocument>, IFulfilmentLevelSegmentationRepository
{
    public FulfilmentLevelSegmentationRepository(
        ICosmosContainerFactory containerFactory,
        ILogger<FulfilmentLevelSegmentationRepository> logger,
        ICorrelationContext correlationContext,
        IAuditTrailWriter auditTrailWriter)
        : base(CosmosContainerNames.Rules, containerFactory, logger, correlationContext, auditTrailWriter)
    {
    }

    /// <summary>Partition key for a fulfilment-level segmentation rule - one partition per fulfilment/hallmark combination (cosmos-db.instructions.md §4).</summary>
    private static string Category(string fulfilment, string hallmarkType) => $"SEG_FU_{fulfilment}_{hallmarkType}";
    private static string Category(FulfilmentLevelSegmentation entity) => Category(entity.FulfilmentCode, entity.HallmarkCode);

    /// <inheritdoc/>
    public async Task<FulfilmentLevelSegmentationStoreLeveragePercentage?> GetFulfilmentLevelFulfilmentyByCategory(
        string fulfilment, string hallMarkType, CancellationToken cancellationToken = default)
    {
        var entity = new FulfilmentLevelSegmentation
        {
            FulfilmentCode = fulfilment,
            HallmarkCode = hallMarkType,
        };
        var category = Category(entity);
        // Selective-column projection (cosmos-db.instructions.md §8): only StoreLeveragePercentage/IsActive
        // are fetched server-side, and the FulfilmentLevelSegmentation -> DTO mapping is defined right here
        // on the query itself, rather than relying on a full-document ToDomain round trip through the base
        // class. A partition (fulfilment + hallmark) can hold more than one item/country-of-origin rule, so
        // ItemCode/CountryOfOriginCode are filtered explicitly - without this, the query would return
        // whichever record happens to be paged back first, not the one the caller asked for.
        var options = new QueryOptions<FulfilmentLevelSegmentation, FulfilmentLevelSegmentationStoreLeveragePercentage>
        {
            Selector = x => new FulfilmentLevelSegmentationStoreLeveragePercentage
            {
                StoreLeveragePercentage = x.StoreLeveragePercentage,
                IsActive = x.IsActive,
            },
            Category = category,
            PageSize = 1,
        };

        var page = await QueryAsync(options, cancellationToken);

        return page.Items.Count > 0 ? page.Items[0] : null;
    }

    /// <summary>
    /// This repository has no write path - fulfilment-level segmentation rules are seeded/maintained out
    /// of band, never created or replaced through this service. Not implemented, rather than mapped, so a
    /// write accidentally routed through here fails loudly instead of silently persisting a bogus document.
    /// </summary>
    protected override FulfilmentLevelSegmentationDocument ToDocument(FulfilmentLevelSegmentation domain) =>
        throw new NotSupportedException($"{nameof(FulfilmentLevelSegmentationRepository)} is read-only and has no document mapping for a write.");

    /// <summary>
    /// Never called: the only read this repository exposes is the selective-column projection in
    /// <see cref="GetFulfilmentLevelFulfilmentyByCategory"/> above, which maps straight to its own DTO via
    /// <c>QueryOptions.Selector</c> and never materializes a full <see cref="FulfilmentLevelSegmentation"/>.
    /// </summary>
    protected override FulfilmentLevelSegmentation ToDomain(FulfilmentLevelSegmentationDocument document) =>
        throw new NotSupportedException($"{nameof(FulfilmentLevelSegmentationRepository)} has no full-entity read path to map a domain instance for.");
}
