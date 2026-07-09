namespace IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

/// <summary>Request to reserve quantity against a warehouse/SKU's on-hand balance.</summary>
/// <param name="ReservationId">Deterministic id for this reservation - reserving the same id twice is a no-op, not a double-decrement.</param>
/// <param name="Quantity">Quantity to reserve.</param>
public sealed record ReserveStockRequest(string ReservationId, int Quantity);
