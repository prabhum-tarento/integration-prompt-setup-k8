using System.Text.Json;
using Azure.Messaging.ServiceBus;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.DynamicValidation;
using IIS.WMS.Common.Logging;
using IIS.WMS.Common.Messaging;
using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Domain.Exceptions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Serilog.Context;

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
/// <remarks>
/// TODO(ai): <see cref="ServiceBusRelayEnvelope.BlobPath"/> (set by <c>ConsumerHostedService</c>'s
/// claim-check offload when a schema payload exceeds <c>ConsumerOptions.MaxServiceBusMessageSizeBytes</c>
/// - see <see cref="BlobStorage.BlobStorageOptions.LargePayloadContainerName"/>) is not read here yet.
/// <see cref="HandleMessageAsync"/> only ever deserializes <see cref="ServiceBusRelayEnvelope.ReflexSchema"/>,
/// which is empty for an offloaded message - once a producer can exceed the threshold, this needs to
/// download the blob at <c>BlobPath</c> (hot-tier <see cref="IFileStore"/>) and
/// deserialize <see cref="InboundInventoryEventMessage"/> from that instead when <c>BlobPath</c> is set.
/// </remarks>
[LogLevelCriteria(LogCriteria.High)]
[Module("Inventory")]
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

        var deserialized = TryDeserializeEnvelope(message, out var deadLetterOutcome);

        if (deserialized is null)
        {
            return deadLetterOutcome!;
        }

        var (envelope, inbound) = deserialized.Value;
        var correlationId = ResolveCorrelationId(message, envelope);

        var (logLevel, module) = LogMetadataResolver.Resolve(GetType());
        string[] types = envelope.Type is { Length: > 0 } envelopeType ? [envelopeType] : [];

        using var scope = scopeFactory.CreateScope();
        var correlationContext = scope.ServiceProvider.GetRequiredService<ICorrelationContext>();
        correlationContext.Set(correlationId, envelope.AppId ?? string.Empty, types, logLevel, module);

        using var logLevelLogContext = LogContext.PushProperty("LogLevel", logLevel);
        using var moduleLogContext = LogContext.PushProperty("Module", module);

        var dynamicEventValidator = scope.ServiceProvider.GetRequiredService<IDynamicEventValidator>();
        var validationOutcome = await RunDynamicValidationAsync(dynamicEventValidator, scope.ServiceProvider, inbound, message, cancellationToken);

        if (validationOutcome is not null)
        {
            return validationOutcome;
        }

        var inventoryEventService = scope.ServiceProvider.GetRequiredService<IInventoryEventService>();

        return await DispatchEventAsync(inventoryEventService, inbound, message, cancellationToken);
    }

    /// <summary>
    /// Deserializes the envelope and inner inbound event payload. On a poison message (malformed
    /// JSON, or either deserialization producing <see langword="null"/>) returns <see langword="null"/>
    /// and sets <paramref name="deadLetterOutcome"/> to the outcome the caller should return immediately.
    /// </summary>
    /// <param name="message">The received Service Bus message.</param>
    /// <param name="deadLetterOutcome">Set when this message is poison and must be dead-lettered by the caller.</param>
    private (ServiceBusRelayEnvelope Envelope, InboundInventoryEventMessage Inbound)? TryDeserializeEnvelope(
        ServiceBusReceivedMessage message, out ServiceBusMessageOutcome? deadLetterOutcome)
    {
        try
        {
            var envelope = JsonSerializer.Deserialize<ServiceBusRelayEnvelope>(message.Body.ToString())
                ?? throw new JsonException("Deserialized envelope was null.");
            var inbound = envelope.ReflexSchema.Deserialize<InboundInventoryEventMessage>()
                ?? throw new JsonException("Deserialized payload was null.");

            deadLetterOutcome = null;
            return (envelope, inbound);
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException)
        {
            logger.LogError(ex, "Poison Service Bus message {MessageId} - dead-lettering.", message.MessageId);

            deadLetterOutcome = ServiceBusMessageOutcome.DeadLettered("PoisonMessage", ex.Message);
            return null;
        }
    }

    /// <summary>
    /// Resolves this message's correlation id: <see cref="ServiceBusReceivedMessage.ApplicationProperties"/>
    /// (the transport-hop property, integration-resiliency.instructions.md §4) first, falling back to
    /// the envelope's own <see cref="ServiceBusRelayEnvelope.CorrelationId"/> for a message relayed
    /// without it, and a fresh id only if neither is set.
    /// </summary>
    private static string ResolveCorrelationId(ServiceBusReceivedMessage message, ServiceBusRelayEnvelope envelope) =>
        message.ApplicationProperties.TryGetValue("CorrelationId", out var value)
                && value?.ToString() is { Length: > 0 } propertyCorrelationId
            ? propertyCorrelationId
            : envelope.CorrelationId is { Length: > 0 } envelopeCorrelationId
                ? envelopeCorrelationId
                : Guid.NewGuid().ToString();

    /// <summary>
    /// Runs the blob-stored dynamic template (if any) against <paramref name="inbound"/>, returning
    /// <see langword="null"/> when the message should continue to dispatch. A non-null result is the
    /// outcome the caller should return immediately - either <see cref="ServiceBusMessageOutcome.Completed"/>
    /// (the template deliberately skipped this message) or <see cref="ServiceBusMessageOutcome.DeadLettered(string, string?)"/>
    /// (the template itself threw).
    /// </summary>
    private async Task<ServiceBusMessageOutcome?> RunDynamicValidationAsync(
        IDynamicEventValidator dynamicEventValidator, IServiceProvider serviceProvider, InboundInventoryEventMessage inbound,
        ServiceBusReceivedMessage message, CancellationToken cancellationToken)
    {
        try
        {
            var isValid = await dynamicEventValidator.ValidateAsync(
                DynamicValidationTransports.ServiceBus, options.QueueName, inbound, ToHeaderLookup(message.ApplicationProperties), logger, serviceProvider, cancellationToken);

            if (!isValid)
            {
                logger.LogInformation("Message {MessageId} skipped by dynamic validation - completing without dispatch.", message.MessageId);
                return ServiceBusMessageOutcome.Completed;
            }

            return null;
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Dynamic validation failed for message {MessageId} - dead-lettering.", message.MessageId);

            return ServiceBusMessageOutcome.DeadLettered("DynamicValidationFailed", ex.Message);
        }
    }

    /// <summary>Dispatches <paramref name="inbound"/> to the matching <see cref="IInventoryEventService"/> use case by <c>EventType</c>.</summary>
    private async Task<ServiceBusMessageOutcome> DispatchEventAsync(
        IInventoryEventService inventoryEventService, InboundInventoryEventMessage inbound,
        ServiceBusReceivedMessage message, CancellationToken cancellationToken)
    {
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

    /// <summary>
    /// Adapts this message's <see cref="ServiceBusReceivedMessage.ApplicationProperties"/> into the
    /// transport-neutral <see cref="HeaderLookup"/> the dynamic-validation script contract reads
    /// through - see <see cref="HeaderLookup"/>'s own remarks for why templates don't read
    /// <see cref="ServiceBusReceivedMessage.ApplicationProperties"/> directly.
    /// </summary>
    /// <param name="applicationProperties">Application properties on the received message.</param>
    private static HeaderLookup ToHeaderLookup(IReadOnlyDictionary<string, object> applicationProperties)
    {
        var values = new Dictionary<string, string>();

        foreach (var (key, value) in applicationProperties)
        {
            if (value?.ToString() is { } stringValue)
            {
                values[key] = stringValue;
            }
        }

        return new HeaderLookup(values);
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
