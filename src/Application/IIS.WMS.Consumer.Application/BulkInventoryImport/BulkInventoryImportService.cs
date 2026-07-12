using IIS.WMS.Consumer.Application.BulkInventoryImport.Dtos;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Application.BulkInventoryImport;

/// <inheritdoc cref="IBulkInventoryImportService"/>
public sealed class BulkInventoryImportService(
    IBulkInventoryImportRepository repository, ILogger<BulkInventoryImportService> logger) : IBulkInventoryImportService
{
    /// <inheritdoc />
    public async Task ImportAsync(ImportBulkInventoryItemRequest request, CancellationToken cancellationToken = default)
    {
        var item = InventoryBulkImportItem.Create(
            request.WarehouseId, request.Sku, request.Quantity, request.SourceSystem, request.SourceLastUpdatedUtc);

        await repository.UpsertAsync(item, cancellationToken);

        logger.LogInformation(
            "Imported bulk on-hand quantity {Quantity} for {PartitionKey} from {SourceSystem}.",
            request.Quantity, item.PartitionKey, request.SourceSystem);
    }
}
