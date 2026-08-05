using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.Logging;
using IIS.WMS.Common.Messaging;
using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderToInventoryAllocated.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.OrderToInventoryAllocated;

/// <summary>
/// Applies relayed order-to-inventory-allocation events via <see cref="IOrderToInventoryAllocatedHandler"/>,
/// exactly the way an API controller does (integration-resiliency.instructions.md §2). The sole consumer
/// of the "order-to-inventory-allocated" queue.
/// </summary>
[LogLevelCriteria(LogCriteria.High)]
[Module("Inventory")]
public sealed class OrderToInventoryAllocatedServiceBusHostedService : ServiceBusConsumerHostedService<OrderToInventoryAllocatedEvent>
{
    /// <param name="dependencies">Plumbing dependencies shared by every Service Bus consumer.</param>
    /// <param name="queueName">Queue this consumer reads from.</param>
    /// <param name="eventOptions">Queue-level session-processor overrides, already resolved via <see cref="OrderToInventoryAllocatedServiceBusConsumerOptions.ApplyServiceBusLevelDefaults"/>.</param>
    /// <param name="logger">Logger for processing/error events.</param>
    public OrderToInventoryAllocatedServiceBusHostedService(
        ServiceBusConsumerDependencies dependencies,
        string queueName,
        IOptions<OrderToInventoryAllocatedServiceBusConsumerOptions> eventOptions,
        ILogger<OrderToInventoryAllocatedServiceBusHostedService> logger)
        : base(dependencies, queueName, eventOptions.Value, logger)
    {
    }

    /// <summary>
    /// Resolves the handler from the per-message DI scope and delegates to its business logic.
    /// </summary>
    protected override async Task ProcessMessageAsync(
        OrderToInventoryAllocatedEvent message,
        ICorrelationContext correlationContext,
        IServiceProvider serviceProvider,
        CancellationToken cancellationToken)
    {
        var handler = serviceProvider.GetRequiredService<IOrderToInventoryAllocatedHandler>();
        await handler.HandleAsync(message, correlationContext.CorrelationId, cancellationToken);
    }
}
