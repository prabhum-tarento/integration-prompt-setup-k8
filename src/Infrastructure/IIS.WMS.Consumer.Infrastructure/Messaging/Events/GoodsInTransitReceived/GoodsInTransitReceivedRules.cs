using IIS.WMS.Consumer.Domain.Enums;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.GoodsInTransitReceived;

/// <summary>
/// Packing-slip normalization, sellability/state/status resolution, fulfilment-unit-id normalization,
/// destination-node resolution, and OMS-delta eligibility (docs/events/b2b.purchase.GoodsInTransitReceived.md
/// §3.1-§3.6) - shared between <see cref="GoodsInTransitReceivedConsumerHostedService"/>'s Kafka-side
/// <c>getServiceBusRouting</c> delegate and <see cref="Handlers.GoodsInTransitReceivedHandler"/>, so the two
/// layers can never disagree on how a given message resolves, mirroring
/// <see cref="OrderStatusChanged.OrderStatusChangedRules"/>'s split-responsibility rationale. Reuses
/// <see cref="FulfilmentLocationIds.Caecom"/>/<see cref="FulfilmentLocationIds.Adc"/>/<see cref="FulfilmentLocationIds.Tdc"/>
/// per the confirmed design decision to not introduce a second per-event constants class.
/// </summary>
internal static class GoodsInTransitReceivedRules
{
    /// <summary>
    /// The upstream Reflex facade's TDC-SAP-ID warehouse code (§3.4/§3.5) - kept as a local literal
    /// rather than adding it to <see cref="FulfilmentLocationIds"/>, mirroring how
    /// <see cref="OrderStatusChanged.OrderStatusChangedWarehouseIds.TdcSapId"/> already exists as its own
    /// event-scoped constant rather than a shared one.
    /// </summary>
    private const string TdcSapId = "TDC-SAP-ID";

    /// <summary>§3.1 - strips a case-insensitive two-character "PS" prefix, if present. Null/empty input returns an empty string.</summary>
    public static string NormalizePackingSlipId(string? packingSlipId) =>
        string.IsNullOrEmpty(packingSlipId)
            ? string.Empty
            : packingSlipId.StartsWith("PS", StringComparison.OrdinalIgnoreCase)
                ? packingSlipId[2..]
                : packingSlipId;

    /// <summary>§3.2 - only a CAECOM destination is sellable; ADC, null, or any other destination routes to the extended container.</summary>
    public static bool IsSellable(InventoryEventLocation? locationTo) =>
        locationTo?.Id == FulfilmentLocationIds.Caecom;

    /// <summary>§3.3 - a present <c>ReturnReasonCode</c> routes to inspection; otherwise the item is available but held pending the buffer.</summary>
    public static State ResolveState(string? returnReasonCode) =>
        string.IsNullOrEmpty(returnReasonCode) ? State.AVAILABLE : State.INSPECTION;

    /// <summary>§3.3 - every non-sellable receipt lands as <see cref="Status.HELD"/>, regardless of <see cref="ResolveState"/>'s branch.</summary>
    public static Status ResolveStatus() => Status.HELD;

    /// <summary>
    /// §3.4 fulfilment-unit-id resolution. The CAECOM/ADC branch (order lookup by <c>PackingSlipId</c>)
    /// has no backing repository in this codebase, so it always falls through to <c>"UNKNOWN"</c> with a
    /// logged warning - a documented non-fatal fallback (§8: "does not fail the message"), not a defect
    /// to fix silently.
    /// </summary>
    /// <remarks>
    /// TODO(ai): PackingSlipId-keyed order lookup not implemented - always resolves to "UNKNOWN" for a
    /// CAECOM/ADC destination per doc §3.4/§8/§10. No order-lookup repository exists in this codebase;
    /// implementing it is out of scope for this change.
    /// </remarks>
    public static string ResolveFulfilmentUnitId(InventoryEventLocation? locationTo, string warehouseCode, string vendorCode) =>
        locationTo?.Id is FulfilmentLocationIds.Caecom or FulfilmentLocationIds.Adc
            ? "UNKNOWN"
            : warehouseCode == TdcSapId
                ? FulfilmentLocationIds.Tdc
                : vendorCode;

    /// <summary>§3.5 - resolves the (CustomerId, DestinationNode) pair for the order-tracking request. Both values are always identical per the doc's pseudocode.</summary>
    public static string ResolveDestinationNode(InventoryEventLocation? locationTo, string warehouseCode) =>
        locationTo?.Id is FulfilmentLocationIds.Caecom or FulfilmentLocationIds.Adc
            ? locationTo.Id
            : warehouseCode == TdcSapId
                ? FulfilmentLocationIds.Tdc
                : warehouseCode;

    /// <summary>§3.6 - OMS delta only fires for a direct-from-supplier CAECOM receipt (no warehouse code).</summary>
    public static bool IsOmsDeltaEligible(string? warehouseCode, InventoryEventLocation? locationTo) =>
        string.IsNullOrWhiteSpace(warehouseCode) && locationTo?.Id == FulfilmentLocationIds.Caecom;
}
