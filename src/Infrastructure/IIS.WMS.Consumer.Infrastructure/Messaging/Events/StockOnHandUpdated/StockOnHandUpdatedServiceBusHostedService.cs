using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.Logging;
using IIS.WMS.Common.Messaging;
using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockOnHandUpdated.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.StockOnHandUpdated;

/// <summary>
/// Applies relayed <see cref="StockOnHandUpdatedEvent"/> messages via
/// <see cref="IStockOnHandUpdatedHandler"/>, exactly the way
/// <see cref="StockSyncSubmitted.StockSyncSubmittedServiceBusHostedService"/> does for its own event.
/// The sole consumer of the dedicated <c>stock-on-hand-updated</c> queue
/// (docs/events/inventory.StockOnHandUpdated.md §9).
/// </summary>
[LogLevelCriteria(LogCriteria.High)]
[Module("Inventory")]
public sealed class StockOnHandUpdatedServiceBusHostedService : ServiceBusConsumerHostedService<StockOnHandUpdatedEvent>
{
    /// <param name="dependencies">Plumbing dependencies shared by every Service Bus consumer - client, scope factory, hot/cold file stores, blob storage options, and the health-state registry.</param>
    /// <param name="queueName">Queue this consumer reads from.</param>
    /// <param name="eventOptions">Queue-level session-processor overrides, already resolved (queue-level-first, ServiceBus-level-fallback) via <see cref="StockOnHandUpdatedServiceBusConsumerOptions.ApplyServiceBusLevelDefaults"/>.</param>
    /// <param name="logger">Logger for processing/error events.</param>
    public StockOnHandUpdatedServiceBusHostedService(
        ServiceBusConsumerDependencies dependencies,
        string queueName,
        IOptions<StockOnHandUpdatedServiceBusConsumerOptions> eventOptions,
        ILogger<StockOnHandUpdatedServiceBusHostedService> logger)
        : base(dependencies, queueName, eventOptions.Value, logger)
    {
    }

    /// <inheritdoc/>
    protected override async Task ProcessMessageAsync(
        StockOnHandUpdatedEvent message, ICorrelationContext correlationContext, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        var handler = serviceProvider.GetRequiredService<IStockOnHandUpdatedHandler>();

        await handler.HandleAsync(message, correlationContext.CorrelationId, cancellationToken);
    }
}
