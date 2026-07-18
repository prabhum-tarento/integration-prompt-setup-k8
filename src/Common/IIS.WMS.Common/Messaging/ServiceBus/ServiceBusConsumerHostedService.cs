using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.DynamicValidation;
using IIS.WMS.Common.Exceptions;
using IIS.WMS.Common.Logging;
using IIS.WMS.Common.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog.Context;

namespace IIS.WMS.Common.Messaging.ServiceBus;

/// <summary>
/// Generic Service Bus → Cosmos DB consumer (integration-resiliency.instructions.md §2), the
/// session-enabled counterpart to <c>Kafka.KafkaConsumerHostedServiceBase</c>. Unlike the Kafka side, one
/// queue here carries exactly one message shape, deserialized via a concrete, private
/// <see cref="DeserializePayload"/> helper using this class's own <typeparamref name="TMessage"/>
/// generic parameter - fixed at compile time by the derived class's declaration, so there is nothing
/// for a derived class to override. A derived class supplies only
/// <see cref="ProcessMessageAsync(TMessage, ServiceBusRelayEnvelope, string, CancellationToken)"/>
/// instead of registering a schema-keyed handler map. Uses <see cref="ServiceBusSessionProcessor"/> -
/// sessions guarantee in-order, single-active-consumer processing per aggregate key, which is what
/// makes a derived handler's own concurrency-conflict retry loop (if any) a defensive backstop rather
/// than the primary correctness mechanism.
/// </summary>
/// <remarks>
/// <see cref="ServiceBusRelayEnvelope.BlobPath"/> (set by <c>KafkaConsumerHostedServiceBase</c>'s claim-check
/// offload when a schema payload exceeds <c>ConsumerOptions.MaxServiceBusMessageSizeBytes</c> - see
/// <see cref="BlobStorageOptions.LargePayloadContainerName"/>) is rehydrated in its own pipeline step,
/// before the request-audit blob write: when set, the hot-tier <see cref="IFileStore"/> downloads the
/// blob and its content replaces the (empty) inline <see cref="ServiceBusRelayEnvelope.ReflexSchema"/>
/// on the envelope, so both the audit write and the later payload-deserialize step see the real
/// payload from a single download.
/// </remarks>
public abstract class ServiceBusConsumerHostedService<TMessage> : BackgroundService, IAsyncDisposable
{
    private readonly ServiceBusClient client;
    private readonly IFileStore hotFileStore;
    private readonly IFileStore coldFileStore;
    private readonly BlobStorageOptions blobStorageOptions;

    // Built lazily in ExecuteAsync, not the constructor - see ExecuteAsync's remarks for why.
    private ServiceBusSessionProcessor? processor;

    /// <summary>Queue name this consumer reads from - also this consumer's <see cref="ServiceBusHealthState"/> registry key.</summary>
    protected string QueueName { get; }

    /// <summary>Creates a DI scope per message, since this hosted service is a singleton but the Application services a derived class calls are scoped.</summary>
    protected IServiceScopeFactory ScopeFactory { get; }

    /// <summary>This consumer's own <see cref="ServiceBusHealthState"/> instance, resolved from <see cref="ServiceBusHealthStateRegistry"/> by <see cref="QueueName"/>.</summary>
    protected ServiceBusHealthState HealthState { get; }

    /// <summary>Logger for processing/error events.</summary>
    protected ILogger Logger { get; }

    /// <summary>Resolved (queue-level-first, ServiceBus-level-fallback) value for <see cref="ServiceBusSessionProcessorOptions.MaxConcurrentSessions"/>.</summary>
    protected int MaxConcurrentSessions { get; }

    /// <summary>Resolved (queue-level-first, ServiceBus-level-fallback) value for <see cref="ServiceBusSessionProcessorOptions.MaxConcurrentCallsPerSession"/>.</summary>
    protected int MaxConcurrentCallsPerSession { get; }

    /// <param name="dependencies">Plumbing dependencies shared by every Service Bus consumer - client, scope factory, hot/cold file stores, blob storage options, and the health-state registry.</param>
    /// <param name="queueName">Queue this consumer reads from - also the key used to resolve this consumer's <see cref="ServiceBusHealthState"/> from <paramref name="dependencies"/>'s registry.</param>
    /// <param name="maxConcurrentSessions">Resolved value for <see cref="ServiceBusSessionProcessorOptions.MaxConcurrentSessions"/>.</param>
    /// <param name="maxConcurrentCallsPerSession">Resolved value for <see cref="ServiceBusSessionProcessorOptions.MaxConcurrentCallsPerSession"/>.</param>
    /// <param name="logger">Logger for processing/error events.</param>
    protected ServiceBusConsumerHostedService(
        ServiceBusConsumerDependencies dependencies,
        string queueName,
        int maxConcurrentSessions,
        int maxConcurrentCallsPerSession,
        ILogger logger)
    {
        client = dependencies.Client;
        QueueName = queueName;
        ScopeFactory = dependencies.ScopeFactory;
        hotFileStore = dependencies.HotFileStore;
        coldFileStore = dependencies.ColdFileStore;
        blobStorageOptions = dependencies.BlobStorageOptions.Value;
        HealthState = dependencies.HealthStateRegistry.GetOrAdd(queueName);
        MaxConcurrentSessions = maxConcurrentSessions;
        MaxConcurrentCallsPerSession = maxConcurrentCallsPerSession;
        Logger = logger;
    }

    private static TMessage DeserializePayload(JsonElement reflexSchema) =>
        reflexSchema.Deserialize<TMessage>() ?? throw new JsonException("Deserialized payload was null.");

    protected abstract Task ProcessMessageAsync(TMessage message, ServiceBusRelayEnvelope envelope, string correlationId, CancellationToken cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        processor = client.CreateSessionProcessor(QueueName, new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = MaxConcurrentSessions,
            MaxConcurrentCallsPerSession = MaxConcurrentCallsPerSession,
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

    internal async Task<ServiceBusMessageOutcome> HandleMessageAsync(ServiceBusReceivedMessage message, CancellationToken cancellationToken)
    {
        var totalStopwatch = Stopwatch.StartNew();
        HealthState.LastSuccessfulReceiveUtc = DateTimeOffset.UtcNow;

        var envelopeDeserializeStopwatch = Stopwatch.StartNew();
        var envelope = TryDeserializeEnvelope(message, out var envelopeDeadLetterOutcome);
        var envelopeDeserializeDuration = envelopeDeserializeStopwatch.Elapsed;

        if (envelope is null)
        {
            await WriteDeadLetterBlobAsync("PoisonEnvelope", "bin", new MemoryStream(message.Body.ToArray()), "unknown", cancellationToken);
            return envelopeDeadLetterOutcome!;
        }

        var correlationId = ResolveCorrelationId(message, envelope);

        var (logLevel, module) = LogMetadataResolver.Resolve(GetType());
        string[] types = envelope.Type is { Length: > 0 } envelopeType ? [envelopeType] : [];

        using var scope = ScopeFactory.CreateScope();
        var correlationContext = scope.ServiceProvider.GetRequiredService<ICorrelationContext>();
        correlationContext.Set(correlationId, envelope.AppId ?? string.Empty, types, logLevel, module);

        using var logLevelLogContext = LogContext.PushProperty("LogLevel", logLevel);
        using var moduleLogContext = LogContext.PushProperty("Module", module);

        var blobRehydrateStopwatch = Stopwatch.StartNew();
        var blobRehydrateError = await TryRehydrateReflexSchemaFromBlobAsync(envelope, correlationId, cancellationToken);
        var blobRehydrateDuration = blobRehydrateStopwatch.Elapsed;

        var requestAuditBlobWriteDuration = await WriteRequestAuditBlobAsync(correlationId, message, envelope, cancellationToken);

        var payloadDeserializeStopwatch = Stopwatch.StartNew();
        var (payload, payloadDeadLetterOutcome) = blobRehydrateError is null
            ? TryDeserializePayload(envelope)
            : (default, ServiceBusMessageOutcome.DeadLettered("PoisonMessage", blobRehydrateError.Message));
        var payloadDeserializeDuration = blobRehydrateDuration + payloadDeserializeStopwatch.Elapsed;

        if (payload is null)
        {
            await WriteDeadLetterBlobAsync("PoisonPayload", envelope, correlationId, cancellationToken);
            return payloadDeadLetterOutcome!;
        }

        var dynamicValidationStopwatch = Stopwatch.StartNew();
        var dynamicEventValidator = scope.ServiceProvider.GetRequiredService<IDynamicEventValidator>();
        var validationOutcome = await RunDynamicValidationAsync(dynamicEventValidator, scope.ServiceProvider, payload, message, correlationId, cancellationToken);
        var dynamicValidationDuration = dynamicValidationStopwatch.Elapsed;

        if (validationOutcome is not null)
        {
            return validationOutcome;
        }

        var processingStopwatch = Stopwatch.StartNew();
        var outcome = await RunProcessMessageAsync(payload, envelope, message, correlationId, cancellationToken);
        var processingDuration = processingStopwatch.Elapsed;

        var durations = new ProcessingDurations(
            envelopeDeserializeDuration, requestAuditBlobWriteDuration, payloadDeserializeDuration, dynamicValidationDuration, processingDuration);

        Logger.LogInformation(
            "{QueueName}: handled message {MessageId} with outcome {Outcome}. CorrelationId: {CorrelationId}, " +
            "EnvelopeDeserializeDurationMs: {EnvelopeDeserializeDurationMs}, RequestAuditBlobWriteDurationMs: {RequestAuditBlobWriteDurationMs}, " +
            "PayloadDeserializeDurationMs: {PayloadDeserializeDurationMs}, DynamicValidationDurationMs: {DynamicValidationDurationMs}, " +
            "ProcessingDurationMs: {ProcessingDurationMs}, TotalDurationMs: {TotalDurationMs}",
            QueueName, message.MessageId, outcome.Kind, correlationId,
            durations.EnvelopeDeserialize.TotalMilliseconds, durations.RequestAuditBlobWrite.TotalMilliseconds,
            durations.PayloadDeserialize.TotalMilliseconds, durations.DynamicValidation.TotalMilliseconds,
            durations.Processing.TotalMilliseconds, totalStopwatch.Elapsed.TotalMilliseconds);

        return outcome;
    }

    private ServiceBusRelayEnvelope? TryDeserializeEnvelope(ServiceBusReceivedMessage message, out ServiceBusMessageOutcome? deadLetterOutcome)
    {
        try
        {
            var envelope = JsonSerializer.Deserialize<ServiceBusRelayEnvelope>(message.Body.ToString())
                ?? throw new JsonException("Deserialized envelope was null.");

            deadLetterOutcome = null;
            return envelope;
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException)
        {
            Logger.LogError(ex, "Poison Service Bus message {MessageId} - dead-lettering.", message.MessageId);

            deadLetterOutcome = ServiceBusMessageOutcome.DeadLettered("PoisonMessage", ex.Message);
            return null;
        }
    }

    /// <summary>
    /// When <see cref="ServiceBusRelayEnvelope.BlobPath"/> is set, downloads and parses that blob and
    /// assigns its content onto <paramref name="envelope"/>'s <see cref="ServiceBusRelayEnvelope.ReflexSchema"/>
    /// - a no-op when <c>BlobPath</c> is empty. Deliberately non-fatal: a download/parse failure is
    /// returned rather than thrown, so the caller can still run the (unconditional, best-effort)
    /// request-audit blob write before surfacing the failure as a poison payload.
    /// </summary>
    private async Task<Exception?> TryRehydrateReflexSchemaFromBlobAsync(
        ServiceBusRelayEnvelope envelope, string correlationId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(envelope.BlobPath))
        {
            return null;
        }

        try
        {
            await using var blobContent = await hotFileStore.DownloadAsync(
                blobStorageOptions.LargePayloadContainerName, envelope.BlobPath, cancellationToken);
            using var blobDocument = await JsonDocument.ParseAsync(blobContent, cancellationToken: cancellationToken);

            envelope.ReflexSchema = blobDocument.RootElement.Clone();
            return null;
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException)
        {
            Logger.LogError(ex,
                "Failed to rehydrate BlobPath-offloaded payload for message with CorrelationId {CorrelationId} - continuing with poison payload handling.",
                correlationId);

            return ex;
        }
    }

    private (TMessage? Payload, ServiceBusMessageOutcome? DeadLetterOutcome) TryDeserializePayload(ServiceBusRelayEnvelope envelope)
    {
        try
        {
            var payload = DeserializePayload(envelope.ReflexSchema)
                ?? throw new JsonException("Deserialized payload was null.");

            return (payload, null);
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException)
        {
            Logger.LogError(ex, "Poison payload for message with CorrelationId {CorrelationId} - dead-lettering.", envelope.CorrelationId);

            return (default, ServiceBusMessageOutcome.DeadLettered("PoisonMessage", ex.Message));
        }
    }

    private static string ResolveCorrelationId(ServiceBusReceivedMessage message, ServiceBusRelayEnvelope envelope) =>
        message.ApplicationProperties.TryGetValue("CorrelationId", out var value)
                && value?.ToString() is { Length: > 0 } propertyCorrelationId
            ? propertyCorrelationId
            : envelope.CorrelationId is { Length: > 0 } envelopeCorrelationId
                ? envelopeCorrelationId
                : Guid.NewGuid().ToString();

    private async Task<ServiceBusMessageOutcome?> RunDynamicValidationAsync(
        IDynamicEventValidator dynamicEventValidator, IServiceProvider serviceProvider, TMessage payload,
        ServiceBusReceivedMessage message, string correlationId, CancellationToken cancellationToken)
    {
        try
        {
            var isValid = await dynamicEventValidator.ValidateAsync(
                DynamicValidationTransports.ServiceBus, QueueName, payload!, ToHeaderLookup(message.ApplicationProperties), Logger, serviceProvider, cancellationToken);

            if (!isValid)
            {
                Logger.LogInformation("Message {MessageId} skipped by dynamic validation - completing without dispatch.", message.MessageId);
                return ServiceBusMessageOutcome.Completed;
            }

            return null;
        }
        catch (Exception ex)
        {
            Logger.LogCritical(ex, "Dynamic validation failed for message {MessageId} - dead-lettering.", message.MessageId);

            await WriteDeadLetterBlobAsync("DynamicValidationFailed", payload, correlationId, cancellationToken);
            return ServiceBusMessageOutcome.DeadLettered("DynamicValidationFailed", ex.Message);
        }
    }

    private async Task<ServiceBusMessageOutcome> RunProcessMessageAsync(
        TMessage payload, ServiceBusRelayEnvelope envelope, ServiceBusReceivedMessage message, string correlationId, CancellationToken cancellationToken)
    {
        try
        {
            await ProcessMessageAsync(payload, envelope, correlationId, cancellationToken);
            return ServiceBusMessageOutcome.Completed;
        }
        catch (ConcurrencyException ex)
        {
            Logger.LogWarning(
                ex, "Concurrency retries exhausted for message {MessageId} - abandoning for redelivery.",
                message.MessageId);

            return ServiceBusMessageOutcome.Abandoned;
        }
        catch (OperationCanceledException ex)
        {
            Logger.LogWarning(ex, "Processing canceled for message {MessageId} - abandoning for redelivery.", message.MessageId);

            return ServiceBusMessageOutcome.Abandoned;
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Processing failed for message {MessageId} - dead-lettering.", message.MessageId);

            await WriteDeadLetterBlobAsync(ex.GetType().Name, payload, correlationId, cancellationToken);
            return ServiceBusMessageOutcome.DeadLettered(ex.GetType().Name, ex.ToString());
        }
    }

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

    private Task ProcessErrorAsync(ProcessErrorEventArgs args)
    {
        Logger.LogError(args.Exception, "Service Bus session processor error in {ErrorSource}.", args.ErrorSource);

        return Task.CompletedTask;
    }

    private readonly record struct ProcessingDurations(
        TimeSpan EnvelopeDeserialize,
        TimeSpan RequestAuditBlobWrite,
        TimeSpan PayloadDeserialize,
        TimeSpan DynamicValidation,
        TimeSpan Processing);

    private async Task<TimeSpan> WriteRequestAuditBlobAsync(
        string correlationId, ServiceBusReceivedMessage message, ServiceBusRelayEnvelope envelope, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        var blobName = $"{correlationId}/ServiceBus/{QueueName}/{DateTime.UtcNow:yyyyMMddHHmmssfff}_{Guid.NewGuid():N}.json";

        await using (var content = BuildRequestAuditContent(message, envelope))
        {
            try
            {
                await coldFileStore.UploadAsync(blobStorageOptions.RequestAuditContainerName, blobName, content, cancellationToken);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex,
                    "Failed to write request-audit blob {ContainerName}/{BlobName} - continuing without it. CorrelationId: {CorrelationId}",
                    blobStorageOptions.RequestAuditContainerName, blobName, correlationId);
            }
        }

        return stopwatch.Elapsed;
    }

    /// <summary>
    /// Raw wire bytes for an inline message, or - when <see cref="ServiceBusRelayEnvelope.BlobPath"/> was
    /// set and successfully rehydrated onto <paramref name="envelope"/> - the envelope re-serialized with
    /// its real <see cref="ServiceBusRelayEnvelope.ReflexSchema"/> populated, so the audit trail is
    /// self-contained instead of just a pointer into <see cref="BlobStorageOptions.LargePayloadContainerName"/>.
    /// Falls back to the raw wire bytes if rehydration failed (an undefined <c>ReflexSchema</c>).
    /// </summary>
    private static MemoryStream BuildRequestAuditContent(ServiceBusReceivedMessage message, ServiceBusRelayEnvelope envelope) =>
        !string.IsNullOrEmpty(envelope.BlobPath) && envelope.ReflexSchema.ValueKind != JsonValueKind.Undefined
            ? new MemoryStream(JsonSerializer.SerializeToUtf8Bytes(envelope))
            : new MemoryStream(message.Body.ToArray());

    private async Task WriteDeadLetterBlobAsync(string reason, string extension, Stream content, string correlationId, CancellationToken cancellationToken)
    {
        var blobName = $"{correlationId}/{QueueName}/{DateTime.UtcNow:yyyyMMddHHmmssfff}_{Guid.NewGuid():N}.{extension}";

        await using (content)
        {
            try
            {
                await hotFileStore.UploadAsync(blobStorageOptions.ConsumerDeadLetterContainerName, blobName, content, cancellationToken);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex,
                    "Failed to write {Reason} dead-letter blob {ContainerName}/{BlobName} - continuing without it. CorrelationId: {CorrelationId}",
                    reason, blobStorageOptions.ConsumerDeadLetterContainerName, blobName, correlationId);
            }
        }
    }

    private async Task WriteDeadLetterBlobAsync<T>(string reason, T value, string correlationId, CancellationToken cancellationToken)
    {
        var blobName = $"{correlationId}/{QueueName}/{DateTime.UtcNow:yyyyMMddHHmmssfff}_{Guid.NewGuid():N}.json";

        try
        {
            await using var content = new MemoryStream(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value)));
            await hotFileStore.UploadAsync(blobStorageOptions.ConsumerDeadLetterContainerName, blobName, content, cancellationToken);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex,
                "Failed to write {Reason} dead-letter blob {ContainerName}/{BlobName} - continuing without it. CorrelationId: {CorrelationId}",
                reason, blobStorageOptions.ConsumerDeadLetterContainerName, blobName, correlationId);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (processor is not null)
        {
            await processor.DisposeAsync();
        }
    }
}
