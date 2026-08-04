using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.Logging;
using IIS.WMS.Common.Messaging;
using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.InternalHallmarkingStatusChanged.Handlers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.InternalHallmarkingStatusChanged;

/// <summary>
/// Applies relayed internal-hallmarking-status-changed events via
/// <see cref="IInternalHallmarkingStatusChangedHandler"/>, exactly the way an Api controller does
/// (integration-resiliency.instructions.md §2). The sole consumer of the
/// "internal-hallmarking-status-changed" queue.
/// </summary>
[LogLevelCriteria(LogCriteria.High)]
[Module("Inventory")]
public sealed class InternalHallmarkingStatusChangedServiceBusHostedService : ServiceBusConsumerHostedService<InternalHallmarkingStatusChangedEvent>
{
    /// <param name="dependencies">Plumbing dependencies shared by every Service Bus consumer - client, scope factory, hot/cold file stores, blob storage options, and the health-state registry.</param>
    /// <param name="queueName">Queue this consumer reads from.</param>
    /// <param name="eventOptions">Queue-level session-processor overrides, already resolved (queue-level-first, ServiceBus-level-fallback) via <see cref="InternalHallmarkingStatusChangedServiceBusConsumerOptions.ApplyServiceBusLevelDefaults"/>.</param>
    /// <param name="logger">Logger for processing/error events.</param>
    public InternalHallmarkingStatusChangedServiceBusHostedService(
        ServiceBusConsumerDependencies dependencies,
        string queueName,
        IOptions<InternalHallmarkingStatusChangedServiceBusConsumerOptions> eventOptions,
        ILogger<InternalHallmarkingStatusChangedServiceBusHostedService> logger)
        : base(dependencies, queueName, eventOptions.Value, logger)
    {
    }

    /// <inheritdoc/>
    protected override async Task ProcessMessageAsync(
        InternalHallmarkingStatusChangedEvent message, ICorrelationContext correlationContext, IServiceProvider serviceProvider, CancellationToken cancellationToken)
    {
        var handler = serviceProvider.GetRequiredService<IInternalHallmarkingStatusChangedHandler>();

        await handler.HandleAsync(message, correlationContext.CorrelationId, cancellationToken);
    }
}
