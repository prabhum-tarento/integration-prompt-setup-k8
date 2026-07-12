using IIS.WMS.Consumer.Application.BulkInventoryImport.Dtos;

namespace IIS.WMS.Consumer.Application.BulkInventoryImport;

/// <summary>
/// Use-case orchestration for bulk-import items: maps the relayed event to
/// <c>Domain.Aggregates.InventoryBulkImportItem</c> and upserts it via
/// <see cref="IBulkInventoryImportRepository"/>. This is the interface the bulk-import Service Bus
/// consumer depends on (integration-resiliency.instructions.md §2 pattern, applied to the new
/// non-session queue) - it never touches the repository or Cosmos types directly.
/// </summary>
public interface IBulkInventoryImportService
{
    /// <summary>
    /// Imports one warehouse/SKU's on-hand figure, overwriting whatever is currently stored for that
    /// key. Idempotent under redelivery - the same warehouse/SKU always maps to the same item id.
    /// </summary>
    /// <param name="request">Warehouse, SKU, quantity, and source metadata to persist.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task ImportAsync(ImportBulkInventoryItemRequest request, CancellationToken cancellationToken = default);
}
