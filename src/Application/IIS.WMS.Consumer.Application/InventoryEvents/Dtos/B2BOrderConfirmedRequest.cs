using IIS.WMS.Consumer.Domain.Enums;

namespace IIS.WMS.Consumer.Application.InventoryEvents.Dtos;

/// <summary>
/// One shipment line's B2B confirmation input for <see cref="IConsolidatedOrderShippedService"/>
/// (docs/events/b2b.sales.ConsolidatedOrderShipped.md §3.1).
/// </summary>
/// <param name="FulfilmentCode">Warehouse the shipment left from.</param>
/// <param name="ItemCode">Item code being confirmed.</param>
/// <param name="CountryOfOrigin">Country of origin of the item line.</param>
/// <param name="Hallmark">Hallmarking value of the item line.</param>
/// <param name="ShippedQuantity">Quantity shipped for this line - must be greater than zero.</param>
/// <param name="ConfirmationType">Confirmation type reported by the shipment - drives the §4.1 arithmetic branch.</param>
/// <param name="AllocatedFromB2BBucketQuantity">Quantity previously allocated from the B2B bucket for this line - must be at least <paramref name="ShippedQuantity"/>.</param>
public sealed record B2BOrderConfirmedRequest(
    string FulfilmentCode,
    string ItemCode,
    string CountryOfOrigin,
    string Hallmark,
    int ShippedQuantity,
    ConfirmationType ConfirmationType,
    int AllocatedFromB2BBucketQuantity);
