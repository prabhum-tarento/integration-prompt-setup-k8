namespace IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

/// <summary>Current on-hand state for a warehouse/SKU, returned by the Api and used by the Service Bus consumer's use-case calls.</summary>
/// <param name="Id">Deterministic aggregate id (<c>WarehouseId:Sku</c>).</param>
/// <param name="WarehouseId">Warehouse this balance belongs to.</param>
/// <param name="Sku">SKU this balance belongs to.</param>
/// <param name="OnHandQuantity">Current on-hand quantity, net of active reservations.</param>
/// <param name="CreatedUtc">UTC timestamp the aggregate was first created.</param>
/// <param name="ModifiedUtc">UTC timestamp of the most recent state change.</param>
public sealed record InventoryEventResponse(
    string Id,
    string WarehouseId,
    string Sku,
    int OnHandQuantity,
    DateTime CreatedUtc,
    DateTime ModifiedUtc);
