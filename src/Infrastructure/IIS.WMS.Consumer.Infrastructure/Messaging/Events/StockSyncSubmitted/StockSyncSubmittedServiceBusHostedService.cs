using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.Logging;
using IIS.WMS.Common.Messaging;
using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockSyncSubmitted.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockSyncSubmitted;

/// <summary>
/// Applies relayed <see cref="StockSyncSubmittedEvent"/> messages via
/// <see cref="IStockSyncSubmittedHandler"/>, exactly the way
/// <see cref="InventoryAdjusted.InventoryAdjustedServiceBusHostedService"/> does for its own event.
/// The sole consumer of the dedicated <c>stock-sync-submitted</c> queue
/// (docs/events/inventory.StockSyncSubmitted.md §9).
/// </summary>
[LogLevelCriteria(LogCriteria.High)]
[Module("Inventory")]
public sealed class StockSyncSubmittedServiceBusHostedService : ServiceBusConsumerHostedService<StockSyncSubmittedEvent>
{
    /// <param name="dependencies">Plumbing dependencies shared by every Service Bus consumer - client, scope factory, hot/cold file stores, blob storage options, and the health-state registry.</param>
    /// <param name="queueName">Queue this consumer reads from.</param>
    /// <param name="eventOptions">Queue-level session-processor overrides, already resolved (queue-level-first, ServiceBus-level-fallback) via <see cref="StockSyncSubmittedServiceBusConsumerOptions.ApplyServiceBusLevelDefaults"/>.</param>
    /// <param name="logger">Logger for processing/error events.</param>
    public StockSyncSubmittedServiceBusHostedService(
        ServiceBusConsumerDependencies dependencies,
        string queueName,
        IOptions<StockSyncSubmittedServiceBusConsumerOptions> eventOptions,
        ILogger<StockSyncSubmittedServiceBusHostedService> logger)
        : base(dependencies, queueName, eventOptions.Value, logger)
    {
    }

    /// <inheritdoc/>
    protected override async Task ProcessMessageAsync(
        StockSyncSubmittedEvent message, ICorrelationContext correlationContext, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        var handler = serviceProvider.GetRequiredService<IStockSyncSubmittedHandler>();

        await handler.HandleAsync(message, correlationContext.CorrelationId, cancellationToken);
    }
}
