namespace IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

/// <summary>Request to create the inventory aggregate for a warehouse/SKU.</summary>
/// <param name="WarehouseId">Warehouse to create the balance for.</param>
/// <param name="Sku">SKU to create the balance for.</param>
/// <param name="InitialQuantity">Starting on-hand quantity.</param>
public sealed record CreateInventoryEventRequest(string WarehouseId, string Sku, int InitialQuantity);
