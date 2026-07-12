namespace IIS.WMS.Consumer.Application.BulkInventoryImport.Dtos;

/// <summary>
/// One warehouse/SKU's on-hand figure from an upstream bulk-import feed, already validated at the
/// Kafka consumer's <c>ValidateAsync</c> hook (the bulk-import Avro event's own field-level rules) -
/// this request is not re-validated here. See <c>Domain.Aggregates.InventoryBulkImportItem</c> for
/// why this is an unconditional overwrite, not a reserve/allocate operation.
/// </summary>
/// <param name="WarehouseId">Warehouse this on-hand figure belongs to.</param>
/// <param name="Sku">SKU this on-hand figure belongs to.</param>
/// <param name="Quantity">On-hand quantity as reported by the upstream system.</param>
/// <param name="SourceSystem">Identifies the upstream system that produced this event.</param>
/// <param name="SourceLastUpdatedUtc">Timestamp the upstream system last updated this figure.</param>
public sealed record ImportBulkInventoryItemRequest(
    string WarehouseId, string Sku, int Quantity, string SourceSystem, DateTime SourceLastUpdatedUtc);
