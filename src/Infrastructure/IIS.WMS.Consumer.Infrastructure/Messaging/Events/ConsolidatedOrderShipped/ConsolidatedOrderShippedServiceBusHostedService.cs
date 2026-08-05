using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.Logging;
using IIS.WMS.Common.Messaging;
using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.ConsolidatedOrderShipped;

/// <summary>
/// Applies relayed consolidated-order-shipped events via <see cref="IConsolidatedOrderShippedHandler"/>,
/// exactly the way an Api controller does (integration-resiliency.instructions.md §2). The sole consumer
/// of the "consolidated-order-shipped" queue.
/// </summary>
[LogLevelCriteria(LogCriteria.High)]
[Module("Inventory")]
public sealed class ConsolidatedOrderShippedServiceBusHostedService : ServiceBusConsumerHostedService<ConsolidatedOrderShippedEvent>
{
    /// <param name="dependencies">Plumbing dependencies shared by every Service Bus consumer - client, scope factory, hot/cold file stores, blob storage options, and the health-state registry.</param>
    /// <param name="queueName">Queue this consumer reads from.</param>
    /// <param name="eventOptions">Queue-level session-processor overrides, already resolved (queue-level-first, ServiceBus-level-fallback) via <see cref="ConsolidatedOrderShippedServiceBusConsumerOptions.ApplyServiceBusLevelDefaults"/>.</param>
    /// <param name="logger">Logger for processing/error events.</param>
    public ConsolidatedOrderShippedServiceBusHostedService(
        ServiceBusConsumerDependencies dependencies,
        string queueName,
        IOptions<ConsolidatedOrderShippedServiceBusConsumerOptions> eventOptions,
        ILogger<ConsolidatedOrderShippedServiceBusHostedService> logger)
        : base(dependencies, queueName, eventOptions.Value, logger)
    {
    }

    /// <inheritdoc/>
    protected override async Task ProcessMessageAsync(
        ConsolidatedOrderShippedEvent message, ICorrelationContext correlationContext, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        var handler = serviceProvider.GetRequiredService<IConsolidatedOrderShippedHandler>();

        await handler.HandleAsync(message, correlationContext.CorrelationId, cancellationToken);
    }
}
