using IIS.WMS.Consumer.Application.OrderTracking.Dtos;

namespace IIS.WMS.Consumer.Application.OrderTracking;

/// <summary>
/// §3.9 Order Tracking publisher port (docs/events/inventory.InventoryStateChanged.md) - ported from
/// the upstream Reflex facade's <c>OrderTrackingCommonOrchestratorRequest</c> builder, minus the
/// orchestrator (docs/events/shared/delta-towards-oms.md). Implemented in the Infrastructure layer
/// since it depends on Service Bus queue configuration (<c>InventoryPublishOptions</c>) - this port
/// only exposes an Application-shaped request.
/// </summary>
public interface IOrderTrackingPublisher
{
    /// <summary>
    /// Publishes one order-tracking request to the <c>order-tracking</c> queue. This is a best-effort
    /// side channel per §8 of the doc: a publish failure is logged and swallowed rather than
    /// propagated, so it never fails the message's overall inventory outcome.
    /// </summary>
    /// <param name="request">The order-tracking request built from the source <c>InventoryStateChangedEvent</c>.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task PublishAsync(OrderTrackingRelayRequest request, CancellationToken cancellationToken = default);
}
