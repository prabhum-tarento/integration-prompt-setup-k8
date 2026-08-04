using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.Logging;
using IIS.WMS.Common.Messaging;
using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryAdjusted.Handlers;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryAdjusted;

/// <summary>
/// Applies relayed <see cref="InventoryAdjustedEvent"/> messages via
/// <see cref="IInventoryAdjustedHandler"/>, exactly the way
/// <see cref="InventoryStateChanged.InventoryStateChangedServiceBusHostedService"/> does for its own
/// event. The sole consumer of the dedicated <c>inventory-adjusted</c> queue
/// (docs/events/inventory.InventoryAdjusted.md §9).
/// </summary>
[LogLevelCriteria(LogCriteria.High)]
[Module("Inventory")]
public sealed class InventoryAdjustedServiceBusHostedService : ServiceBusConsumerHostedService<InventoryAdjustedEvent>
{
    /// <param name="dependencies">Plumbing dependencies shared by every Service Bus consumer - client, scope factory, hot/cold file stores, blob storage options, and the health-state registry.</param>
    /// <param name="queueName">Queue this consumer reads from.</param>
    /// <param name="eventOptions">Queue-level session-processor overrides, already resolved (queue-level-first, ServiceBus-level-fallback) via <see cref="InventoryAdjustedServiceBusConsumerOptions.ApplyServiceBusLevelDefaults"/>.</param>
    /// <param name="logger">Logger for processing/error events.</param>
    public InventoryAdjustedServiceBusHostedService(
        ServiceBusConsumerDependencies dependencies,
        string queueName,
        IOptions<InventoryAdjustedServiceBusConsumerOptions> eventOptions,
        ILogger<InventoryAdjustedServiceBusHostedService> logger)
        : base(dependencies, queueName, eventOptions.Value, logger)
    {
    }

    /// <inheritdoc/>
    protected override async Task ProcessMessageAsync(
        InventoryAdjustedEvent message, ICorrelationContext correlationContext, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        var handler = serviceProvider.GetRequiredService<IInventoryAdjustedHandler>();

        await handler.HandleAsync(message, correlationContext.CorrelationId, cancellationToken);
    }
}
