using System.Text.Json;
using Azure.Messaging.ServiceBus;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Exceptions;
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
    private readonly ServiceBusSessionProcessor processor;
    private readonly IServiceScopeFactory scopeFactory;
    private readonly ServiceBusHealthState healthState;
    private readonly ILogger<ServiceBusConsumerHostedService> logger;

    /// <summary>Builds the session processor and wires its message/error event handlers.</summary>
    /// <param name="client">Service Bus client used to create the session processor.</param>
    /// <param name="options">Queue name and other Service Bus settings.</param>
    /// <param name="scopeFactory">Creates a DI scope per message, since the hosted service itself is a singleton but the Application services it calls are scoped.</param>
    /// <param name="healthState">Shared state updated on every message received.</param>
    /// <param name="logger">Logger for processing/error events.</param>
    public ServiceBusConsumerHostedService(
        ServiceBusClient client,
        IOptions<ServiceBusConsumerOptions> options,
        IServiceScopeFactory scopeFactory,
        ServiceBusHealthState healthState,
        ILogger<ServiceBusConsumerHostedService> logger)
    {
        this.scopeFactory = scopeFactory;
        this.healthState = healthState;
        this.logger = logger;

        processor = client.CreateSessionProcessor(options.Value.QueueName, new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 8,
            MaxConcurrentCallsPerSession = 1,
            AutoCompleteMessages = false,
        });

        processor.ProcessMessageAsync += ProcessMessageAsync;
        processor.ProcessErrorAsync += ProcessErrorAsync;
    }

    /// <summary>Starts the session processor and keeps the service alive until cancellation, then stops it cleanly.</summary>
    /// <param name="stoppingToken">Signaled when the host is shutting down.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
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

    /// <summary>Deserializes one received message, dispatches it to the matching use case by <c>EventType</c>, and settles it (complete/abandon/dead-letter) based on the outcome.</summary>
    /// <param name="args">Event args carrying the message and the settlement methods (complete/abandon/dead-letter).</param>
    private async Task ProcessMessageAsync(ProcessSessionMessageEventArgs args)
    {
        healthState.LastSuccessfulReceiveUtc = DateTimeOffset.UtcNow;

        InboundInventoryEventMessage inbound;

        try
        {
            inbound = JsonSerializer.Deserialize<InboundInventoryEventMessage>(args.Message.Body.ToString())
                ?? throw new JsonException("Deserialized payload was null.");
        }
        catch (JsonException ex)
        {
            logger.LogError(ex, "Poison Service Bus message {MessageId} - dead-lettering.", args.Message.MessageId);
            await args.DeadLetterMessageAsync(args.Message, "PoisonMessage", ex.Message, args.CancellationToken);

            return;
        }

        var correlationId = args.Message.ApplicationProperties.TryGetValue("CorrelationId", out var value)
            ? value?.ToString() ?? Guid.NewGuid().ToString()
            : Guid.NewGuid().ToString();

        using var scope = scopeFactory.CreateScope();
        var correlationContext = scope.ServiceProvider.GetRequiredService<ICorrelationContext>();
        correlationContext.Set(correlationId);

        var inventoryEventService = scope.ServiceProvider.GetRequiredService<IInventoryEventService>();

        try
        {
            switch (inbound.EventType)
            {
                case "Create":
                    await inventoryEventService.CreateAsync(
                        new CreateInventoryEventRequest(inbound.WarehouseId, inbound.Sku, inbound.Quantity),
                        args.CancellationToken);
                    break;

                case "Reserve":
                    await inventoryEventService.ReserveStockAsync(
                        inbound.WarehouseId, inbound.Sku,
                        new ReserveStockRequest(inbound.EventId, inbound.Quantity),
                        args.CancellationToken);
                    break;

                default:
                    logger.LogWarning(
                        "Unknown inventory event type '{EventType}' for message {MessageId} - dead-lettering.",
                        inbound.EventType, args.Message.MessageId);
                    await args.DeadLetterMessageAsync(args.Message, "UnknownEventType", cancellationToken: args.CancellationToken);

                    return;
            }

            await args.CompleteMessageAsync(args.Message, args.CancellationToken);
        }
        catch (ConcurrencyException ex)
        {
            // The re-read-and-reapply loop inside InventoryEventService exhausted its attempts -
            // treat as a processing failure for this message, not success
            // (integration-resiliency.instructions.md §2). Not retried by Polly - that pipeline
            // only covers transient infrastructure faults, not application-level conflicts.
            logger.LogWarning(
                ex, "Concurrency retries exhausted for message {MessageId} - abandoning for redelivery.",
                args.Message.MessageId);
            await args.AbandonMessageAsync(args.Message, cancellationToken: args.CancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(ex, "Processing failed for message {MessageId} - abandoning for redelivery.", args.Message.MessageId);
            await args.AbandonMessageAsync(args.Message, cancellationToken: args.CancellationToken);
        }
    }

    /// <summary>Logs an error raised by the processor's own infrastructure (e.g. a lock-renewal or connection failure) - not tied to any single message.</summary>
    /// <param name="args">Event args carrying the exception and where in the processor it occurred.</param>
    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        logger.LogError(args.Exception, "Service Bus session processor error in {ErrorSource}.", args.ErrorSource);

        return Task.CompletedTask;
    }

    /// <summary>Disposes the session processor asynchronously - required since <see cref="ServiceBusSessionProcessor"/> has no synchronous dispose.</summary>
    public async ValueTask DisposeAsync()
    {
        await processor.DisposeAsync();
    }
}
