using System.Text;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.DynamicValidation;
using IIS.WMS.Common.Exceptions;
using IIS.WMS.Common.Logging;
using IIS.WMS.Common.Messaging;
using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;
using IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus.Handlers;
using IIS.WMS.Consumer.UnitTests.Infrastructure.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Tests for <see cref="ServiceBusConsumerHostedService{TMessage}.HandleMessageAsync"/> - exercised
/// through its sole derived class, <see cref="InventoryStateChangedServiceBusHostedService"/> - the
/// session-enabled Service Bus consumer's message-handling core, tested directly (per
/// integration-resiliency.instructions.md §9) rather than through a real
/// <see cref="ServiceBusSessionProcessor"/>, which cannot be subscribed to without a genuinely
/// connected <see cref="ServiceBusClient"/>.
/// </summary>
public class ServiceBusConsumerHostedServiceTests
{
    private static bool IsGuid(string? value) =>
        !string.IsNullOrEmpty(value) && Guid.TryParse(value.TrimStart('-'), out _);

    [Fact(DisplayName = "A Create event dispatches to CreateAsync and the outcome is Completed")]
    public async Task HandleMessageAsync_CreateEvent_DispatchesCreateAndReturnsCompleted()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var healthStateRegistry = new ServiceBusHealthStateRegistry();
        var sut = CreateSut(inventoryEventService, out _, out _, out _, healthStateRegistry: healthStateRegistry);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create")));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.Completed, outcome.Kind);
        await inventoryEventService.Received(1).CreateAsync(
            Arg.Is<CreateInventoryEventRequest>(r => r.WarehouseId == "WH1" && r.Sku == "SKU1" && r.InitialQuantity == 10),
            Arg.Any<CancellationToken>());
        Assert.True(healthStateRegistry.GetOrAdd("inventory-events").LastSuccessfulReceiveUtc > DateTimeOffset.UnixEpoch);
    }

    [Fact(DisplayName = "A Reserve event dispatches to ReserveStockAsync and the outcome is Completed")]
    public async Task HandleMessageAsync_ReserveEvent_DispatchesReserveAndReturnsCompleted()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _, out _, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Reserve", eventId: "res-1", quantity: 3)));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.Completed, outcome.Kind);
        await inventoryEventService.Received(1).ReserveStockAsync(
            "WH1", "SKU1",
            Arg.Is<ReserveStockRequest>(r => r.ReservationId == "res-1" && r.Quantity == 3),
            Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "Dynamic validation returning false completes the message without dispatching")]
    public async Task HandleMessageAsync_DynamicValidationReturnsFalse_ReturnsCompletedWithoutDispatch()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var dynamicEventValidator = Substitute.For<IDynamicEventValidator>();
        dynamicEventValidator.ValidateAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<object>(), Arg.Any<HeaderLookup?>(),
                Arg.Any<ILogger>(), Arg.Any<IServiceProvider>(), Arg.Any<CancellationToken>())
            .Returns(false);
        var sut = CreateSut(inventoryEventService, out _, out _, out _, dynamicEventValidator: dynamicEventValidator);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create")));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.Completed, outcome.Kind);
        await inventoryEventService.DidNotReceiveWithAnyArgs().CreateAsync(default!, default);
    }

    [Fact(DisplayName = "Dynamic validation throwing dead-letters the message as DynamicValidationFailed and writes a dead-letter blob")]
    public async Task HandleMessageAsync_DynamicValidationThrows_ReturnsDeadLetteredDynamicValidationFailed()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var dynamicEventValidator = Substitute.For<IDynamicEventValidator>();
        dynamicEventValidator.ValidateAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<object>(), Arg.Any<HeaderLookup?>(),
                Arg.Any<ILogger>(), Arg.Any<IServiceProvider>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<bool>(new InvalidOperationException("Template blew up.")));
        var sut = CreateSut(inventoryEventService, out _, out var hotFileStore, out _, dynamicEventValidator: dynamicEventValidator);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create"), correlationId: "corr-dv"));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.DeadLettered, outcome.Kind);
        Assert.Equal("DynamicValidationFailed", outcome.Reason);
        Assert.Equal("Template blew up.", outcome.Description);
        await inventoryEventService.DidNotReceiveWithAnyArgs().CreateAsync(default!, default);
        await hotFileStore.Received(1).UploadAsync(
            "consumer-dead-letter", Arg.Is<string>(name => name.StartsWith("corr-property/inventory-events/", StringComparison.Ordinal) && name.EndsWith(".json", StringComparison.Ordinal)),
            Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "Dynamic validation returning true dispatches normally, unaffected by wiring")]
    public async Task HandleMessageAsync_DynamicValidationReturnsTrue_DispatchesNormally()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _, out _, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create")));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.Completed, outcome.Kind);
        await inventoryEventService.Received(1).CreateAsync(Arg.Any<CreateInventoryEventRequest>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "An unrecognized event type is dead-lettered")]
    public async Task HandleMessageAsync_UnknownEventType_ReturnsDeadLettered()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _, out _, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "SomethingElse")));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.DeadLettered, outcome.Kind);
        Assert.Equal(nameof(InvalidOperationException), outcome.Reason);
        await inventoryEventService.DidNotReceiveWithAnyArgs().CreateAsync(default!, default);
        await inventoryEventService.DidNotReceiveWithAnyArgs().ReserveStockAsync(default!, default!, default!, default);
    }

    [Fact(DisplayName = "Malformed envelope JSON is dead-lettered as a poison message and the raw bytes are written to the dead-letter blob store")]
    public async Task HandleMessageAsync_MalformedEnvelope_ReturnsDeadLetteredPoisonMessage()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _, out var hotFileStore, out _);

        var message = BuildReceivedMessage("{ not valid json");

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.DeadLettered, outcome.Kind);
        Assert.Equal("PoisonMessage", outcome.Reason);
        await hotFileStore.Received(1).UploadAsync(
            "consumer-dead-letter", Arg.Is<string>(name => name.StartsWith("unknown/inventory-events/", StringComparison.Ordinal) && name.EndsWith(".bin", StringComparison.Ordinal)),
            Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "A JSON literal null ReflexSchema is dead-lettered as a poison message and the envelope JSON is written to the dead-letter blob store")]
    public async Task HandleMessageAsync_NullReflexSchema_ReturnsDeadLetteredPoisonMessage()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _, out var hotFileStore, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(payload: null, correlationId: "corr-null"));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.DeadLettered, outcome.Kind);
        Assert.Equal("PoisonMessage", outcome.Reason);
        Assert.Equal("Deserialized payload was null.", outcome.Description);
        await hotFileStore.Received(1).UploadAsync(
            "consumer-dead-letter", Arg.Is<string>(name => name.StartsWith("corr-property/inventory-events/", StringComparison.Ordinal) && name.EndsWith(".json", StringComparison.Ordinal)),
            Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "A set BlobPath downloads and deserializes the blob content instead of ReflexSchema")]
    public async Task HandleMessageAsync_BlobPathSet_DeserializesBlobContentInsteadOfReflexSchema()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _, out var hotFileStore, out _);

        hotFileStore.DownloadAsync("large-payload", "corr-blob/inventory-events/blob.json", Arg.Any<CancellationToken>())
            .Returns(new MemoryStream(Encoding.UTF8.GetBytes(BuildInboundJson(eventType: "Create"))));

        var envelopeJson = JsonSerializer.Serialize(new
        {
            CorrelationId = "corr-blob",
            AppId = "app-1",
            Type = "InventoryStateChanged",
            BlobPath = "corr-blob/inventory-events/blob.json",
        });
        var message = BuildReceivedMessage(envelopeJson);

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.Completed, outcome.Kind);
        await inventoryEventService.Received(1).CreateAsync(
            Arg.Is<CreateInventoryEventRequest>(r => r.WarehouseId == "WH1" && r.Sku == "SKU1" && r.InitialQuantity == 10),
            Arg.Any<CancellationToken>());
        await hotFileStore.Received(1).DownloadAsync("large-payload", "corr-blob/inventory-events/blob.json", Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "A set BlobPath rehydrates ReflexSchema before the request-audit blob write, so the audit blob carries the real payload instead of the raw wire bytes")]
    public async Task HandleMessageAsync_BlobPathSet_WritesRehydratedContentToRequestAuditBlob()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _, out var hotFileStore, out var coldFileStore);

        hotFileStore.DownloadAsync("large-payload", "corr-blob-audit/inventory-events/blob.json", Arg.Any<CancellationToken>())
            .Returns(new MemoryStream(Encoding.UTF8.GetBytes(BuildInboundJson(eventType: "Create"))));

        byte[]? uploadedBytes = null;
        coldFileStore.UploadAsync("request-audit", Arg.Any<string>(), Arg.Do<Stream>(s => uploadedBytes = ((MemoryStream)s).ToArray()), Arg.Any<CancellationToken>())
            .Returns("https://blob/request-audit.json");

        var envelopeJson = JsonSerializer.Serialize(new
        {
            CorrelationId = "corr-blob-audit",
            AppId = "app-1",
            Type = "InventoryStateChanged",
            BlobPath = "corr-blob-audit/inventory-events/blob.json",
        });
        var message = BuildReceivedMessage(envelopeJson);

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.Completed, outcome.Kind);
        Assert.NotNull(uploadedBytes);

        using var uploadedDocument = JsonDocument.Parse(uploadedBytes!);
        Assert.Equal("corr-blob-audit/inventory-events/blob.json", uploadedDocument.RootElement.GetProperty("BlobPath").GetString());
        var uploadedReflexSchema = uploadedDocument.RootElement.GetProperty("ReflexSchema");
        Assert.Equal("WH1", uploadedReflexSchema.GetProperty("WarehouseId").GetString());
        Assert.Equal("SKU1", uploadedReflexSchema.GetProperty("Sku").GetString());
    }

    [Fact(DisplayName = "A BlobPath download failure is dead-lettered as a poison payload and the envelope JSON is written to the dead-letter blob store")]
    public async Task HandleMessageAsync_BlobPathDownloadThrows_ReturnsDeadLetteredPoisonMessage()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _, out var hotFileStore, out _);

        hotFileStore.DownloadAsync("large-payload", "corr-blob-fail/inventory-events/blob.json", Arg.Any<CancellationToken>())
            .Returns(Task.FromException<Stream>(new InvalidOperationException("Blob not found.")));

        var envelopeJson = JsonSerializer.Serialize(new
        {
            CorrelationId = "corr-blob-fail",
            AppId = "app-1",
            Type = "InventoryStateChanged",
            BlobPath = "corr-blob-fail/inventory-events/blob.json",
        });
        var message = BuildReceivedMessage(envelopeJson);

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.DeadLettered, outcome.Kind);
        Assert.Equal("PoisonMessage", outcome.Reason);
        await inventoryEventService.DidNotReceiveWithAnyArgs().CreateAsync(default!, default);
    }

    [Fact(DisplayName = "A missing ReflexSchema property (undefined JsonElement) is dead-lettered as a poison message")]
    public async Task HandleMessageAsync_MissingReflexSchema_ReturnsDeadLetteredPoisonMessage()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _, out _, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(payload: null, omitReflexSchema: true));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.DeadLettered, outcome.Kind);
        Assert.Equal("PoisonMessage", outcome.Reason);
    }

    [Fact(DisplayName = "An exhausted concurrency retry loop (ConcurrencyException) is abandoned for redelivery")]
    public async Task HandleMessageAsync_ConcurrencyExceptionFromReserve_ReturnsAbandoned()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        inventoryEventService.ReserveStockAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<ReserveStockRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<InventoryEventResponse>(new ConcurrencyException("WH1:SKU1", "etag-1")));
        var sut = CreateSut(inventoryEventService, out _, out _, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Reserve")));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.Abandoned, outcome.Kind);
    }

    [Fact(DisplayName = "A general processing failure from CreateAsync is dead-lettered with the full exception details, and the payload is written to the dead-letter blob store")]
    public async Task HandleMessageAsync_GeneralExceptionFromCreate_ReturnsDeadLettered()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var exception = new InvalidOperationException("Cosmos is unavailable.");
        inventoryEventService.CreateAsync(Arg.Any<CreateInventoryEventRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<InventoryEventResponse>(exception));
        var sut = CreateSut(inventoryEventService, out _, out var hotFileStore, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create"), correlationId: "corr-fail"));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.DeadLettered, outcome.Kind);
        Assert.Equal(nameof(InvalidOperationException), outcome.Reason);
        Assert.Equal(exception.ToString(), outcome.Description);
        await hotFileStore.Received(1).UploadAsync(
            "consumer-dead-letter", Arg.Is<string>(name => name.StartsWith("corr-property/inventory-events/", StringComparison.Ordinal) && name.EndsWith(".json", StringComparison.Ordinal)),
            Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "An OperationCanceledException from CreateAsync is abandoned for redelivery, not propagated")]
    public async Task HandleMessageAsync_OperationCanceledFromCreate_ReturnsAbandoned()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        inventoryEventService.CreateAsync(Arg.Any<CreateInventoryEventRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<InventoryEventResponse>(new OperationCanceledException()));
        var sut = CreateSut(inventoryEventService, out _, out _, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create")));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.Abandoned, outcome.Kind);
    }

    [Fact(DisplayName = "Every handled message writes a request-audit blob at the CorrelationId/ServiceBus/QueueName path")]
    public async Task HandleMessageAsync_AnyMessage_WritesRequestAuditBlob()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _, out _, out var coldFileStore);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create")), correlationIdProperty: "corr-audit");

        await sut.HandleMessageAsync(message, CancellationToken.None);

        await coldFileStore.Received(1).UploadAsync(
            "request-audit",
            Arg.Is<string>(name => name.StartsWith("corr-audit/ServiceBus/inventory-events/", StringComparison.Ordinal) && name.EndsWith(".json", StringComparison.Ordinal)),
            Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "The CorrelationId application property, when present, wins over the envelope's own correlation id")]
    public async Task HandleMessageAsync_CorrelationIdPropertyPresent_UsesPropertyValue()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out var correlationContext, out _, out _);

        var message = BuildReceivedMessage(
            BuildEnvelopeJson(BuildInboundJson(eventType: "Create"), correlationId: "corr-envelope"),
            correlationIdProperty: "corr-property");

        await sut.HandleMessageAsync(message, CancellationToken.None);

        correlationContext.Received(1).Set(
            "corr-property", Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<LogCriteria>(), Arg.Any<string>(), Arg.Any<int>());
    }

    [Fact(DisplayName = "Without a CorrelationId application property, the envelope's own correlation id is used")]
    public async Task HandleMessageAsync_CorrelationIdPropertyAbsent_FallsBackToEnvelopeCorrelationId()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out var correlationContext, out _, out _);

        var message = BuildReceivedMessage(
            BuildEnvelopeJson(BuildInboundJson(eventType: "Create"), correlationId: "corr-envelope"),
            correlationIdProperty: null);

        await sut.HandleMessageAsync(message, CancellationToken.None);

        correlationContext.Received(1).Set(
            "corr-envelope", Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<LogCriteria>(), Arg.Any<string>(), Arg.Any<int>());
    }

    [Fact(DisplayName = "With neither a CorrelationId property nor an envelope correlation id, a fresh id is generated")]
    public async Task HandleMessageAsync_NoCorrelationIdAnywhere_GeneratesNewId()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out var correlationContext, out _, out _);

        var message = BuildReceivedMessage(
            BuildEnvelopeJson(BuildInboundJson(eventType: "Create"), correlationId: null),
            correlationIdProperty: null);

        await sut.HandleMessageAsync(message, CancellationToken.None);

        correlationContext.Received(1).Set(
            Arg.Is<string>(id => IsGuid(id)),
            Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<LogCriteria>(), Arg.Any<string>(), Arg.Any<int>());
    }

    [Fact(DisplayName = "The envelope's Type is carried into the correlation context's Types list when present")]
    public async Task HandleMessageAsync_EnvelopeTypePresent_PopulatesTypesList()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out var correlationContext, out _, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create"), type: "InventoryStateChanged"));

        await sut.HandleMessageAsync(message, CancellationToken.None);

        correlationContext.Received(1).Set(
            Arg.Any<string>(), Arg.Any<string>(),
            Arg.Is<IReadOnlyList<string>>(types => types.Count == 3 && types[0] == "InventoryStateChanged"),
            Arg.Any<LogCriteria>(), Arg.Any<string>(), Arg.Any<int>());
    }

    [Fact(DisplayName = "A missing envelope Type results in an empty Types list, and a missing AppId falls back to empty string")]
    public async Task HandleMessageAsync_EnvelopeTypeAndAppIdAbsent_PopulatesEmptyDefaults()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out var correlationContext, out _, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create"), appId: null, type: null));

        await sut.HandleMessageAsync(message, CancellationToken.None);

        correlationContext.Received(1).Set(
            Arg.Any<string>(), string.Empty,
            Arg.Is<IReadOnlyList<string>>(types => types.Count == 2),
            Arg.Any<LogCriteria>(), Arg.Any<string>(), Arg.Any<int>());
    }

    [Fact(DisplayName = "The message's DeliveryCount is carried into the correlation context")]
    public async Task HandleMessageAsync_AnyMessage_PassesDeliveryCountToCorrelationContext()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out var correlationContext, out _, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create")), deliveryCount: 4);

        await sut.HandleMessageAsync(message, CancellationToken.None);

        correlationContext.Received(1).Set(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>(),
            Arg.Any<LogCriteria>(), Arg.Any<string>(), 4);
    }

    private static string BuildInboundJson(
        string eventId = "evt-1", string warehouseId = "WH1", string sku = "SKU1", int quantity = 10, string eventType = "Create") =>
        JsonSerializer.Serialize(new
        {
            EventId = eventId,
            WarehouseId = warehouseId,
            Sku = sku,
            Quantity = quantity,
            EventType = eventType,
        });

    private static string BuildEnvelopeJson(
        string? payload, string? correlationId = "corr-1", string? appId = "app-1", string? type = "InventoryStateChanged",
        bool omitReflexSchema = false)
    {
        if (omitReflexSchema)
        {
            return JsonSerializer.Serialize(new { CorrelationId = correlationId, AppId = appId, Type = type, BlobPath = "" });
        }

        using var payloadDocument = payload is null ? null : JsonDocument.Parse(payload);
        return JsonSerializer.Serialize(new
        {
            CorrelationId = correlationId,
            AppId = appId,
            Type = type,
            ReflexSchema = payloadDocument?.RootElement,
            BlobPath = "",
        });
    }

    private static ServiceBusReceivedMessage BuildReceivedMessage(
        string envelopeJson, string? correlationIdProperty = "corr-property", int deliveryCount = 1) =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString(envelopeJson),
            messageId: "msg-1",
            sessionId: "WH1:SKU1",
            deliveryCount: deliveryCount,
            properties: correlationIdProperty is null
                ? new Dictionary<string, object>()
                : new Dictionary<string, object> { ["CorrelationId"] = correlationIdProperty });

    private static InventoryStateChangedServiceBusHostedService CreateSut(
        IInventoryEventService inventoryEventService,
        out ICorrelationContext correlationContext,
        out IFileStore hotFileStore,
        out IFileStore coldFileStore,
        ServiceBusHealthStateRegistry? healthStateRegistry = null,
        IDynamicEventValidator? dynamicEventValidator = null)
    {
        var scopedCorrelationContext = Substitute.For<ICorrelationContext>();
        correlationContext = scopedCorrelationContext;

        var validator = dynamicEventValidator ?? CreatePassthroughValidator();

        var hotStore = Substitute.For<IFileStore>();
        var coldStore = Substitute.For<IFileStore>();
        hotFileStore = hotStore;
        coldFileStore = coldStore;

        var handler = new InventoryStateChangedHandler(inventoryEventService, Substitute.For<ILogger<InventoryStateChangedHandler>>());

        var services = new ServiceCollection();
        services.AddSingleton(scopedCorrelationContext);
        services.AddSingleton(validator);
        services.AddSingleton<IInventoryStateChangedHandler>(handler);
        var scopeFactory = services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();

        var dependencies = new ServiceBusConsumerDependencies(
            new FakeServiceBusClient(),
            scopeFactory,
            hotStore,
            coldStore,
            Options.Create(new BlobStorageOptions()),
            healthStateRegistry ?? new ServiceBusHealthStateRegistry());

        return new InventoryStateChangedServiceBusHostedService(
            dependencies,
            "inventory-events",
            Options.Create(new InventoryStateChangedServiceBusConsumerOptions()),
            Substitute.For<ILogger<InventoryStateChangedServiceBusHostedService>>());
    }

    private static IDynamicEventValidator CreatePassthroughValidator()
    {
        var validator = Substitute.For<IDynamicEventValidator>();
        validator.ValidateAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<object>(), Arg.Any<HeaderLookup?>(),
                Arg.Any<ILogger>(), Arg.Any<IServiceProvider>(), Arg.Any<CancellationToken>())
            .Returns(true);

        return validator;
    }
}
