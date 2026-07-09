using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

namespace IIS.WMS.Consumer.Application.InventoryEvents;

/// <summary>
/// Use-case orchestration for the inventory aggregate: loads/persists via
/// <see cref="IInventoryEventRepository"/>, invokes the aggregate's business methods, and
/// dispatches the domain events they raise. This is the interface
/// <c>InventoryEventsController</c> depends on (aspnet-rest-apis.instructions.md) - it never
/// touches the repository or Cosmos types directly.
/// </summary>
public interface IInventoryEventService
{
    /// <summary>Returns the current on-hand state for a warehouse/SKU, or <see langword="null"/> if it doesn't exist yet.</summary>
    /// <param name="warehouseId">Warehouse to look up.</param>
    /// <param name="sku">SKU to look up.</param>
    /// <param name="cancellationToken">Token to cancel the read.</param>
    Task<InventoryEventResponse?> GetAsync(string warehouseId, string sku, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates the inventory aggregate for a warehouse/SKU. Idempotent under redelivery: the
    /// aggregate id is deterministic (<c>WarehouseId:Sku</c>), so a duplicate call for the same
    /// pair returns the existing aggregate rather than throwing (cosmos-db.instructions.md §5).
    /// </summary>
    /// <param name="request">Warehouse, SKU, and initial on-hand quantity.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task<InventoryEventResponse> CreateAsync(CreateInventoryEventRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reserves quantity against on-hand for a warehouse/SKU, retrying against fresh state on an
    /// ETag conflict (integration-resiliency.instructions.md §2). Throws
    /// <see cref="Exceptions.NotFoundException"/> if the aggregate doesn't exist and
    /// <see cref="Domain.Exceptions.InsufficientStockException"/> if on-hand can't cover the request.
    /// </summary>
    /// <param name="warehouseId">Warehouse to reserve against.</param>
    /// <param name="sku">SKU to reserve against.</param>
    /// <param name="request">Reservation id and quantity.</param>
    /// <param name="cancellationToken">Token to cancel the write.</param>
    Task<InventoryEventResponse> ReserveStockAsync(
        string warehouseId, string sku, ReserveStockRequest request, CancellationToken cancellationToken = default);
}
