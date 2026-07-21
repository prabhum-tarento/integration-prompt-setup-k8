using System.Text.Json;
using Azure.Messaging.ServiceBus;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.Logging;
using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Application.BulkInventoryImport;
using IIS.WMS.Consumer.Application.BulkInventoryImport.Dtos;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.BulkInventoryImport.AvroContracts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Serilog.Context;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Events.BulkInventoryImport;

/// <summary>
/// Applies relayed bulk-import events to Cosmos DB via the Application layer
/// (integration-resiliency.instructions.md §1/§2). Uses a plain <see cref="ServiceBusProcessor"/>,
/// not <see cref="ServiceBusSessionProcessor"/> - this queue has <c>RequiresSession = false</c>,
/// since bulk-import data is an unordered, idempotent snapshot reload with no per-aggregate ordering
/// need (unlike <see cref="ServiceBusConsumerHostedService"/>'s session-scoped queue). Concurrency is
/// bounded by <see cref="ServiceBusProcessorOptions.MaxConcurrentCalls"/> alone - no in-process
/// <c>Channel</c> on top of it, and the Cosmos write itself unconditionally upserts
/// (<see cref="IBulkInventoryImportRepository.UpsertAsync"/>) rather than the ETag-guarded
/// read-modify-write <see cref="IInventoryEventService"/> uses, so there is no concurrency-conflict
/// retry loop here either.
/// </summary>
[LogLevelCriteria(LogCriteria.Low)]
[Module("BulkImport")]
public sealed class BulkImportServiceBusConsumerHostedService : BackgroundService, IAsyncDisposable
{
    private readonly ServiceBusClient client;
    private readonly BulkImportServiceBusConsumerOptions options;
    private readonly IServiceScopeFactory scopeFactory;
    private readonly ServiceBusHealthState healthState;
    private readonly ILogger<BulkImportServiceBusConsumerHostedService> logger;

    // Built lazily in ExecuteAsync, not the constructor - see ExecuteAsync's remarks for why.
    private ServiceBusProcessor? processor;

    /// <param name="client">Service Bus client the processor is built from, on first start (see <see cref="ExecuteAsync"/>) - the same client <see cref="ServiceBusConsumerHostedService"/> uses, just a different queue.</param>
    /// <param name="options">Queue name and concurrency settings.</param>
    /// <param name="scopeFactory">Creates a DI scope per message, since the hosted service itself is a singleton but <see cref="IBulkInventoryImportService"/> is scoped.</param>
    /// <param name="healthStateRegistry">Process-wide registry this consumer resolves its own <see cref="ServiceBusHealthState"/> from, keyed by <see cref="BulkImportServiceBusConsumerOptions.QueueName"/>.</param>
    /// <param name="logger">Logger for processing/error events.</param>
    public BulkImportServiceBusConsumerHostedService(
        ServiceBusClient client,
        IOptions<BulkImportServiceBusConsumerOptions> options,
        IServiceScopeFactory scopeFactory,
        ServiceBusHealthStateRegistry healthStateRegistry,
        ILogger<BulkImportServiceBusConsumerHostedService> logger)
    {
        this.client = client;
        this.options = options.Value;
        this.scopeFactory = scopeFactory;
        healthState = healthStateRegistry.GetOrAdd(this.options.QueueName);
        this.logger = logger;
    }

    /// <summary>
    /// Builds the processor, wires its message/error event handlers, starts it, and keeps the service
    /// alive until cancellation, then stops it cleanly. Building the processor here rather than in the
    /// constructor is deliberate - see <see cref="ServiceBusConsumerHostedService.ExecuteAsync"/>'s
    /// remarks for why (the same reasoning applies to the non-session <see cref="ServiceBusProcessor"/>
    /// this class uses).
    /// </summary>
    /// <param name="stoppingToken">Signaled when the host is shutting down.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        processor = client.CreateProcessor(options.QueueName, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = options.MaxConcurrentCalls,
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
    private async Task OnProcessMessageAsync(ProcessMessageEventArgs args)
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
    /// Deserializes one received message and upserts it via <see cref="IBulkInventoryImportService"/>,
    /// returning the outcome instead of settling the message directly - see
    /// <see cref="ServiceBusMessageOutcome"/>'s remarks for why this is split out of
    /// <see cref="OnProcessMessageAsync"/>. <see langword="internal"/> so an integration test in the same
    /// solution (<c>IIS.WMS.Consumer.IntegrationTests</c>, via <c>InternalsVisibleTo</c>) can call this
    /// directly with a message built via <c>ServiceBusModelFactory</c>, without a working
    /// <see cref="ServiceBusProcessor"/>.
    /// </summary>
    internal async Task<ServiceBusMessageOutcome> HandleMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken)
    {
        healthState.LastSuccessfulReceiveUtc = DateTimeOffset.UtcNow;

        var inbound = TryDeserializeEvent(message, out var deadLetterOutcome);

        if (inbound is null)
        {
            return deadLetterOutcome!;
        }

        var correlationId = message.ApplicationProperties.TryGetValue("CorrelationId", out var value)
            ? value?.ToString() ?? Guid.NewGuid().ToString()
            : Guid.NewGuid().ToString();

        var (logLevel, module) = LogMetadataResolver.Resolve(GetType());

        using var scope = scopeFactory.CreateScope();
        var correlationContext = scope.ServiceProvider.GetRequiredService<ICorrelationContext>();
        correlationContext.Set(correlationId, string.Empty, [], logLevel, module);

        using var logLevelLogContext = LogContext.PushProperty("LogLevel", logLevel);
        using var moduleLogContext = LogContext.PushProperty("Module", module);

        var bulkInventoryImportService = scope.ServiceProvider.GetRequiredService<IBulkInventoryImportService>();

        return await ImportEventAsync(bulkInventoryImportService, inbound, message, cancellationToken);
    }

    /// <summary>
    /// Deserializes the bulk-import event payload. On a poison message (malformed JSON, or a
    /// <see langword="null"/> result) returns <see langword="null"/> and sets
    /// <paramref name="deadLetterOutcome"/> to the outcome the caller should return immediately.
    /// </summary>
    /// <param name="message">The received Service Bus message.</param>
    /// <param name="deadLetterOutcome">Set when this message is poison and must be dead-lettered by the caller.</param>
    private BulkInventoryImportEvent? TryDeserializeEvent(ServiceBusReceivedMessage message, out ServiceBusMessageOutcome? deadLetterOutcome)
    {
        try
        {
            var inbound = JsonSerializer.Deserialize<BulkInventoryImportEvent>(message.Body.ToString())
                ?? throw new JsonException("Deserialized payload was null.");

            deadLetterOutcome = null;
            return inbound;
        }
        catch (JsonException ex)
        {
            logger.LogError(ex, "Poison Service Bus message {MessageId} - dead-lettering.", message.MessageId);

            deadLetterOutcome = ServiceBusMessageOutcome.DeadLettered("PoisonMessage", ex.Message);
            return null;
        }
    }

    /// <summary>Upserts <paramref name="inbound"/> via <see cref="IBulkInventoryImportService"/>.</summary>
    private async Task<ServiceBusMessageOutcome> ImportEventAsync(
        IBulkInventoryImportService bulkInventoryImportService, BulkInventoryImportEvent inbound,
        ServiceBusReceivedMessage message, CancellationToken cancellationToken)
    {
        try
        {
            var request = new ImportBulkInventoryItemRequest(
                inbound.WarehouseId, inbound.Sku, inbound.Quantity, inbound.SourceSystem,
                DateTimeOffset.FromUnixTimeMilliseconds(inbound.LastUpdatedUtcMillis).UtcDateTime);

            await bulkInventoryImportService.ImportAsync(request, cancellationToken);

            return ServiceBusMessageOutcome.Completed;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // No concurrency-conflict case to special-case here, unlike ServiceBusConsumerHostedService -
            // the Cosmos write is an unconditional upsert (see class-level remarks), so the only failure
            // mode past deserialization is a transient infrastructure fault. Abandon for redelivery.
            logger.LogError(ex, "Processing failed for message {MessageId} - abandoning for redelivery.", message.MessageId);

            return ServiceBusMessageOutcome.Abandoned;
        }
    }

    /// <summary>Logs an error raised by the processor's own infrastructure (e.g. a lock-renewal or connection failure) - not tied to any single message.</summary>
    /// <param name="args">Event args carrying the exception and where in the processor it occurred.</param>
    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        logger.LogError(args.Exception, "Bulk-import Service Bus processor error in {ErrorSource}.", args.ErrorSource);

        return Task.CompletedTask;
    }

    /// <summary>Disposes the processor asynchronously, if one was ever built (see <see cref="ExecuteAsync"/>) - required since <see cref="ServiceBusProcessor"/> has no synchronous dispose.</summary>
    public async ValueTask DisposeAsync()
    {
        if (processor is not null)
        {
            await processor.DisposeAsync();
        }
    }
}
