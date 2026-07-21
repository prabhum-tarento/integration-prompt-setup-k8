using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.Logging;
using IIS.WMS.Common.Messaging;
using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InventoryStateChanged;

/// <summary>
/// Applies relayed inventory events via <see cref="IInventoryStateChangedHandler"/>, exactly the way
/// an Api controller does (integration-resiliency.instructions.md §2). The sole consumer of the
/// "inventory-state-changed" queue - replaces the old sealed <c>ServiceBusConsumerHostedService</c> now
/// that the base class is generic and reusable.
/// </summary>
[LogLevelCriteria(LogCriteria.High)]
[Module("Inventory")]
public sealed class InventoryStateChangedServiceBusHostedService : ServiceBusConsumerHostedService<InventoryStateChangedEvent>
{
    private readonly IServiceScopeFactory scopeFactory;

    /// <param name="dependencies">Plumbing dependencies shared by every Service Bus consumer - client, scope factory, hot/cold file stores, blob storage options, and the health-state registry.</param>
    /// <param name="queueName">Queue this consumer reads from.</param>
    /// <param name="eventOptions">Queue-level session-processor overrides, already resolved (queue-level-first, ServiceBus-level-fallback) via <see cref="InventoryStateChangedServiceBusConsumerOptions.ApplyServiceBusLevelDefaults"/>.</param>
    /// <param name="logger">Logger for processing/error events.</param>
    public InventoryStateChangedServiceBusHostedService(
        ServiceBusConsumerDependencies dependencies,
        string queueName,
        IOptions<InventoryStateChangedServiceBusConsumerOptions> eventOptions,
        ILogger<InventoryStateChangedServiceBusHostedService> logger)
        : base(dependencies, queueName, eventOptions.Value, logger)
    {
        scopeFactory = dependencies.ScopeFactory;
    }

    /// <inheritdoc/>
    protected override async Task ProcessMessageAsync(
        InventoryStateChangedEvent message, ICorrelationContext correlationContext, CancellationToken cancellationToken)
    {
        using var scope = scopeFactory.CreateScope();
        var handler = scope.ServiceProvider.GetRequiredService<IInventoryStateChangedHandler>();

        await handler.HandleAsync(message, correlationContext.CorrelationId, cancellationToken);
    }
}
