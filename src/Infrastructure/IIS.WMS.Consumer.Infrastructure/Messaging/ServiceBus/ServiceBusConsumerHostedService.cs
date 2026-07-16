using System.Text.Json;
using Azure.Messaging.ServiceBus;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Exceptions;
using IIS.WMS.Consumer.Infrastructure.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;

/// <summary>
/// Applies relayed inventory events to Cosmos DB via the Application layer, exactly the way an
/// Api controller does (integration-resiliency.instructions.md §2). Uses
/// <see cref="ServiceBusSessionProcessor"/> - sessions guarantee in-order, single-active-consumer
/// processing per <c>WarehouseId:Sku</c>, which is what makes the ETag retry loop inside
/// <see cref="IInventoryEventService"/> a defensive backstop rather than the primary correctness
/// mechanism. Idempotency relies on the aggregate's naturally-idempotent operations (deterministic
/// create id, reservation-id-keyed reserve) rather than a separate dedupe record store - the
/// alternative the doc allows when the target write is already idempotent.
/// </summary>
public sealed class ServiceBusConsumerHostedService : BackgroundService, IAsyncDisposable
{
    private readonly ServiceBusClient client;
    private readonly ServiceBusConsumerOptions options;
    private readonly IServiceScopeFactory scopeFactory;
    private readonly ServiceBusHealthState healthState;
    private readonly ILogger<ServiceBusConsumerHostedService> logger;

    // Built lazily in ExecuteAsync, not the constructor - see ExecuteAsync's remarks for why.
    private ServiceBusSessionProcessor? processor;

    /// <param name="client">Service Bus client the session processor is built from, on first start (see <see cref="ExecuteAsync"/>).</param>
    /// <param name="options">Queue name and other Service Bus settings.</param>
    /// <param name="scopeFactory">Creates a DI scope per message, since the hosted service itself is a singleton but the Application services it calls are scoped.</param>
    /// <param name="healthState">This consumer's own keyed <see cref="ServiceBusHealthState"/> instance - keyed since the bulk-import consumer has its own, separate instance too.</param>
    /// <param name="logger">Logger for processing/error events.</param>
    public ServiceBusConsumerHostedService(
        ServiceBusClient client,
        IOptions<ServiceBusConsumerOptions> options,
        IServiceScopeFactory scopeFactory,
        [FromKeyedServices(MessagingServiceCollectionExtensions.InventoryEventsServiceBusKey)] ServiceBusHealthState healthState,
        ILogger<ServiceBusConsumerHostedService> logger)
    {
        this.client = client;
        this.options = options.Value;
        this.scopeFactory = scopeFactory;
        this.healthState = healthState;
        this.logger = logger;
    }

    /// <summary>
    /// Builds the session processor, wires its message/error event handlers, starts it, and keeps the
    /// service alive until cancellation, then stops it cleanly. Building the processor here rather than
    /// in the constructor is deliberate, not just style: subscribing to
    /// <see cref="ServiceBusSessionProcessor.ProcessMessageAsync"/> on a processor built from a
    /// <see cref="ServiceBusClient"/> with no real connection throws on the subscription itself (a hard
    /// SDK limitation - that event's add accessor is not virtual and touches internal state only a
    /// genuine connection populates), so an integration test swapping in such a client
    /// (integration-resiliency.instructions.md §9) can safely construct this hosted service and call
    /// <see cref="HandleMessageAsync"/> directly as long as it never calls <see cref="ExecuteAsync"/>.
    /// This is not a behavior change for production - the class is a DI singleton either way, so the
    /// processor is still built exactly once, just on first start instead of at construction.
    /// </summary>
    /// <param name="stoppingToken">Signaled when the host is shutting down.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        processor = client.CreateSessionProcessor(options.QueueName, new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 8,
            MaxConcurrentCallsPerSession = 1,
            AutoCompleteMessages = false,
        });

        processor.ProcessMessageAsync += OnProcessMessageAsync;
        processor.ProcessErrorAsync += ProcessErrorAsync;

        await processor.StartProcessingAsync(stoppingToken);

        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Expected on shutdown.
        }
        finally
        {
            await processor.StopProcessingAsync(CancellationToken.None);
        }
    }

    /// <summary>Thin adapter wired to the real SDK event - runs <see cref="HandleMessageAsync"/> then settles the message (complete/abandon/dead-letter) per its returned outcome.</summary>
    /// <param name="args">Event args carrying the message and the settlement methods (complete/abandon/dead-letter).</param>
    private async Task OnProcessMessageAsync(ProcessSessionMessageEventArgs args)
    {
        var outcome = await HandleMessageAsync(args.Message, args.CancellationToken);

        switch (outcome.Kind)
        {
            case ServiceBusMessageOutcomeKind.Completed:
                await args.CompleteMessageAsync(args.Message, args.CancellationToken);
                break;

            case ServiceBusMessageOutcomeKind.Abandoned:
                await args.AbandonMessageAsync(args.Message, cancellationToken: args.CancellationToken);
                break;

            case ServiceBusMessageOutcomeKind.DeadLettered:
                await args.DeadLetterMessageAsync(args.Message, outcome.Reason, outcome.Description, args.CancellationToken);
                break;
        }
    }

    /// <summary>
    /// Deserializes one received message and dispatches it to the matching use case by <c>EventType</c>,
    /// returning the outcome instead of settling the message directly - see
    /// <see cref="ServiceBusMessageOutcome"/>'s remarks for why this is split out of
    /// <see cref="OnProcessMessageAsync"/>. <see langword="internal"/> so an integration test in the same
    /// solution (<c>IIS.WMS.Consumer.IntegrationTests</c>, via <c>InternalsVisibleTo</c>) can call this
    /// directly with a message built via <c>ServiceBusModelFactory</c>, without a working
    /// <see cref="ServiceBusSessionProcessor"/>.
    /// </summary>
    internal async Task<ServiceBusMessageOutcome> HandleMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken)
    {
        healthState.LastSuccessfulReceiveUtc = DateTimeOffset.UtcNow;

        ServiceBusRelayEnvelope envelope;
        InboundInventoryEventMessage inbound;

        try
        {
            envelope = JsonSerializer.Deserialize<ServiceBusRelayEnvelope>(message.Body.ToString())
                ?? throw new JsonException("Deserialized envelope was null.");
            inbound = envelope.Payload.Deserialize<InboundInventoryEventMessage>()
                ?? throw new JsonException("Deserialized payload was null.");
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException)
        {
            logger.LogError(ex, "Poison Service Bus message {MessageId} - dead-lettering.", message.MessageId);

            return ServiceBusMessageOutcome.DeadLettered("PoisonMessage", ex.Message);
        }

        // ApplicationProperties is the transport-hop property (integration-resiliency.instructions.md
        // §4) - preferred when present, with the envelope's own CorrelationId as a fallback for a
        // message relayed without it, and a fresh id only if neither is set.
        var correlationId = message.ApplicationProperties.TryGetValue("CorrelationId", out var value)
                && value?.ToString() is { Length: > 0 } propertyCorrelationId
            ? propertyCorrelationId
            : envelope.CorrelationId is { Length: > 0 } envelopeCorrelationId
                ? envelopeCorrelationId
                : Guid.NewGuid().ToString();

        using var scope = scopeFactory.CreateScope();
        var correlationContext = scope.ServiceProvider.GetRequiredService<ICorrelationContext>();
        correlationContext.Set(correlationId, envelope.AppId, envelope.Types);

        var inventoryEventService = scope.ServiceProvider.GetRequiredService<IInventoryEventService>();

        try
        {
            switch (inbound.EventType)
            {
                case "Create":
                    await inventoryEventService.CreateAsync(
                        new CreateInventoryEventRequest(inbound.WarehouseId, inbound.Sku, inbound.Quantity),
                        cancellationToken);
                    break;

                case "Reserve":
                    await inventoryEventService.ReserveStockAsync(
                        inbound.WarehouseId, inbound.Sku,
                        new ReserveStockRequest(inbound.EventId, inbound.Quantity),
                        cancellationToken);
                    break;

                default:
                    logger.LogWarning(
                        "Unknown inventory event type '{EventType}' for message {MessageId} - dead-lettering.",
                        inbound.EventType, message.MessageId);

                    return ServiceBusMessageOutcome.DeadLettered("UnknownEventType");
            }

            return ServiceBusMessageOutcome.Completed;
        }
        catch (ConcurrencyException ex)
        {
            // The re-read-and-reapply loop inside InventoryEventService exhausted its attempts -
            // treat as a processing failure for this message, not success
            // (integration-resiliency.instructions.md §2). Not retried by Polly - that pipeline
            // only covers transient infrastructure faults, not application-level conflicts.
            logger.LogWarning(
                ex, "Concurrency retries exhausted for message {MessageId} - abandoning for redelivery.",
                message.MessageId);

            return ServiceBusMessageOutcome.Abandoned;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(ex, "Processing failed for message {MessageId} - abandoning for redelivery.", message.MessageId);

            return ServiceBusMessageOutcome.Abandoned;
        }
    }

    /// <summary>Logs an error raised by the processor's own infrastructure (e.g. a lock-renewal or connection failure) - not tied to any single message.</summary>
    /// <param name="args">Event args carrying the exception and where in the processor it occurred.</param>
    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        logger.LogError(args.Exception, "Service Bus session processor error in {ErrorSource}.", args.ErrorSource);

        return Task.CompletedTask;
    }

    /// <summary>Disposes the session processor asynchronously, if one was ever built (see <see cref="ExecuteAsync"/>) - required since <see cref="ServiceBusSessionProcessor"/> has no synchronous dispose.</summary>
    public async ValueTask DisposeAsync()
    {
        if (processor is not null)
        {
            await processor.DisposeAsync();
        }
    }
}
