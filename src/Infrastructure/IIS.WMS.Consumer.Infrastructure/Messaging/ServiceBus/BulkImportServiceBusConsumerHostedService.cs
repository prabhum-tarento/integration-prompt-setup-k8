using System.Text.Json;
using Azure.Messaging.ServiceBus;
using IIS.WMS.Consumer.Application.BulkInventoryImport;
using IIS.WMS.Consumer.Application.BulkInventoryImport.Dtos;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka.AvroContracts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;

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
public sealed class BulkImportServiceBusConsumerHostedService : BackgroundService, IAsyncDisposable
{
    private readonly ServiceBusProcessor processor;
    private readonly IServiceScopeFactory scopeFactory;
    private readonly ServiceBusHealthState healthState;
    private readonly ILogger<BulkImportServiceBusConsumerHostedService> logger;

    /// <summary>Builds the processor and wires its message/error event handlers.</summary>
    /// <param name="client">Service Bus client used to create the processor - the same client <see cref="ServiceBusConsumerHostedService"/> uses, just a different queue.</param>
    /// <param name="options">Queue name and concurrency settings.</param>
    /// <param name="scopeFactory">Creates a DI scope per message, since the hosted service itself is a singleton but <see cref="IBulkInventoryImportService"/> is scoped.</param>
    /// <param name="healthState">This consumer's own keyed <see cref="ServiceBusHealthState"/> instance.</param>
    /// <param name="logger">Logger for processing/error events.</param>
    public BulkImportServiceBusConsumerHostedService(
        ServiceBusClient client,
        IOptions<BulkImportServiceBusConsumerOptions> options,
        IServiceScopeFactory scopeFactory,
        [FromKeyedServices(MessagingServiceCollectionExtensions.BulkInventoryImportServiceBusKey)] ServiceBusHealthState healthState,
        ILogger<BulkImportServiceBusConsumerHostedService> logger)
    {
        this.scopeFactory = scopeFactory;
        this.healthState = healthState;
        this.logger = logger;

        processor = client.CreateProcessor(options.Value.QueueName, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = options.Value.MaxConcurrentCalls,
            AutoCompleteMessages = false,
        });

        processor.ProcessMessageAsync += ProcessMessageAsync;
        processor.ProcessErrorAsync += ProcessErrorAsync;
    }

    /// <summary>Starts the processor and keeps the service alive until cancellation, then stops it cleanly.</summary>
    /// <param name="stoppingToken">Signaled when the host is shutting down.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await processor.StartProcessingAsync(stoppingToken);
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

    /// <summary>Deserializes one received message, upserts it via <see cref="IBulkInventoryImportService"/>, and settles it (complete/abandon/dead-letter) based on the outcome.</summary>
    /// <param name="args">Event args carrying the message and the settlement methods (complete/abandon/dead-letter).</param>
    private async Task ProcessMessageAsync(ProcessMessageEventArgs args)
    {
        healthState.LastSuccessfulReceiveUtc = DateTimeOffset.UtcNow;

        BulkInventoryImportEvent inbound;

        try
        {
            inbound = JsonSerializer.Deserialize<BulkInventoryImportEvent>(args.Message.Body.ToString())
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

        var bulkInventoryImportService = scope.ServiceProvider.GetRequiredService<IBulkInventoryImportService>();

        try
        {
            var request = new ImportBulkInventoryItemRequest(
                inbound.WarehouseId, inbound.Sku, inbound.Quantity, inbound.SourceSystem,
                DateTimeOffset.FromUnixTimeMilliseconds(inbound.LastUpdatedUtcMillis).UtcDateTime);

            await bulkInventoryImportService.ImportAsync(request, args.CancellationToken);
            await args.CompleteMessageAsync(args.Message, args.CancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // No concurrency-conflict case to special-case here, unlike ServiceBusConsumerHostedService -
            // the Cosmos write is an unconditional upsert (see class-level remarks), so the only failure
            // mode past deserialization is a transient infrastructure fault. Abandon for redelivery.
            logger.LogError(ex, "Processing failed for message {MessageId} - abandoning for redelivery.", args.Message.MessageId);
            await args.AbandonMessageAsync(args.Message, cancellationToken: args.CancellationToken);
        }
    }

    /// <summary>Logs an error raised by the processor's own infrastructure (e.g. a lock-renewal or connection failure) - not tied to any single message.</summary>
    /// <param name="args">Event args carrying the exception and where in the processor it occurred.</param>
    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        logger.LogError(args.Exception, "Bulk-import Service Bus processor error in {ErrorSource}.", args.ErrorSource);

        return Task.CompletedTask;
    }

    /// <summary>Disposes the processor asynchronously - required since <see cref="ServiceBusProcessor"/> has no synchronous dispose.</summary>
    public async ValueTask DisposeAsync()
    {
        await processor.DisposeAsync();
    }
}
