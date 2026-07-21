using IIS.WMS.Consumer.Domain.Aggregates;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Port for fulfilment-level segmentation rule lookups (cosmos-db.instructions.md §5). Controllers and
/// other Application services never reference <c>CosmosClient</c>/<c>Container</c> directly - only this
/// interface, implemented by
/// <c>Infrastructure.Persistence.CosmosDb.Repository.FulfilmentLevelSegmentationRepository</c>.
/// </summary>
public interface IFulfilmentLevelSegmentationRepository
{
    /// <summary>
    /// Reads the store-leverage rule for one fulfilment/hallmark/item/country-of-origin combination -
    /// only <see cref="FulfilmentLevelSegmentationStoreLeveragePercentage.StoreLeveragePercentage"/> and
    /// <see cref="FulfilmentLevelSegmentationStoreLeveragePercentage.IsActive"/> are fetched, not the full
    /// segmentation rule, since callers of this lookup only ever need those two fields.
    /// </summary>
    /// <param name="fulfilment">Fulfilment code, part of the rule's partition key.</param>
    /// <param name="hallMarkType">Hallmark code, part of the rule's partition key.</param>
    /// <param name="cancellationToken">Token to cancel the read.</param>
    /// <returns>The matching rule's leverage percentage/active flag, or <see langword="null"/> if no rule matches.</returns>
    Task<FulfilmentLevelSegmentationStoreLeveragePercentage?> GetFulfilmentLevelFulfilmentyByCategory(
        string fulfilment, string hallMarkType, CancellationToken cancellationToken = default);
}
