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
using IIS.WMS.Consumer.Infrastructure.Messaging.ServiceBus;
using IIS.WMS.Consumer.UnitTests.Infrastructure.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Tests for <see cref="ServiceBusConsumerHostedService.HandleMessageAsync"/> - the session-enabled
/// Service Bus consumer's message-handling core, tested directly (per
/// integration-resiliency.instructions.md §9) rather than through a real
/// <see cref="ServiceBusSessionProcessor"/>, which cannot be subscribed to without a genuinely
/// connected <see cref="ServiceBusClient"/>.
/// </summary>
public class ServiceBusConsumerHostedServiceTests
{
    private static bool IsGuid(string? value) => !string.IsNullOrEmpty(value) && Guid.TryParse(value, out _);

    [Fact(DisplayName = "A Create event dispatches to CreateAsync and the outcome is Completed")]
    public async Task HandleMessageAsync_CreateEvent_DispatchesCreateAndReturnsCompleted()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var healthState = new ServiceBusHealthState { LastSuccessfulReceiveUtc = DateTimeOffset.UnixEpoch };
        var sut = CreateSut(inventoryEventService, out _, healthState: healthState);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create")));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.Completed, outcome.Kind);
        await inventoryEventService.Received(1).CreateAsync(
            Arg.Is<CreateInventoryEventRequest>(r => r.WarehouseId == "WH1" && r.Sku == "SKU1" && r.InitialQuantity == 10),
            Arg.Any<CancellationToken>());
        Assert.True(healthState.LastSuccessfulReceiveUtc > DateTimeOffset.UnixEpoch);
    }

    [Fact(DisplayName = "A Reserve event dispatches to ReserveStockAsync and the outcome is Completed")]
    public async Task HandleMessageAsync_ReserveEvent_DispatchesReserveAndReturnsCompleted()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _);

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
        var sut = CreateSut(inventoryEventService, out _, dynamicEventValidator: dynamicEventValidator);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create")));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.Completed, outcome.Kind);
        await inventoryEventService.DidNotReceiveWithAnyArgs().CreateAsync(default!, default);
    }

    [Fact(DisplayName = "Dynamic validation throwing dead-letters the message as DynamicValidationFailed")]
    public async Task HandleMessageAsync_DynamicValidationThrows_ReturnsDeadLetteredDynamicValidationFailed()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var dynamicEventValidator = Substitute.For<IDynamicEventValidator>();
        dynamicEventValidator.ValidateAsync(
                Arg.Any<string>(), Arg.Any<string>(), Arg.Any<object>(), Arg.Any<HeaderLookup?>(),
                Arg.Any<ILogger>(), Arg.Any<IServiceProvider>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<bool>(new InvalidOperationException("Template blew up.")));
        var sut = CreateSut(inventoryEventService, out _, dynamicEventValidator: dynamicEventValidator);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create")));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.DeadLettered, outcome.Kind);
        Assert.Equal("DynamicValidationFailed", outcome.Reason);
        Assert.Equal("Template blew up.", outcome.Description);
        await inventoryEventService.DidNotReceiveWithAnyArgs().CreateAsync(default!, default);
    }

    [Fact(DisplayName = "Dynamic validation returning true dispatches normally, unaffected by wiring")]
    public async Task HandleMessageAsync_DynamicValidationReturnsTrue_DispatchesNormally()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create")));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.Completed, outcome.Kind);
        await inventoryEventService.Received(1).CreateAsync(Arg.Any<CreateInventoryEventRequest>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "An unrecognized event type is dead-lettered as UnknownEventType")]
    public async Task HandleMessageAsync_UnknownEventType_ReturnsDeadLetteredUnknownEventType()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "SomethingElse")));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.DeadLettered, outcome.Kind);
        Assert.Equal("UnknownEventType", outcome.Reason);
        await inventoryEventService.DidNotReceiveWithAnyArgs().CreateAsync(default!, default);
        await inventoryEventService.DidNotReceiveWithAnyArgs().ReserveStockAsync(default!, default!, default!, default);
    }

    [Fact(DisplayName = "Malformed envelope JSON is dead-lettered as a poison message rather than throwing")]
    public async Task HandleMessageAsync_MalformedEnvelope_ReturnsDeadLetteredPoisonMessage()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _);

        var message = BuildReceivedMessage("{ not valid json");

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.DeadLettered, outcome.Kind);
        Assert.Equal("PoisonMessage", outcome.Reason);
    }

    [Fact(DisplayName = "A JSON literal null ReflexSchema is dead-lettered as a poison message")]
    public async Task HandleMessageAsync_NullReflexSchema_ReturnsDeadLetteredPoisonMessage()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(payload: null));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.DeadLettered, outcome.Kind);
        Assert.Equal("PoisonMessage", outcome.Reason);
        Assert.Equal("Deserialized payload was null.", outcome.Description);
    }

    [Fact(DisplayName = "A missing ReflexSchema property (undefined JsonElement) is dead-lettered as a poison message")]
    public async Task HandleMessageAsync_MissingReflexSchema_ReturnsDeadLetteredPoisonMessage()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out _);

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
        var sut = CreateSut(inventoryEventService, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Reserve")));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.Abandoned, outcome.Kind);
    }

    [Fact(DisplayName = "A general processing failure from CreateAsync is abandoned for redelivery")]
    public async Task HandleMessageAsync_GeneralExceptionFromCreate_ReturnsAbandoned()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        inventoryEventService.CreateAsync(Arg.Any<CreateInventoryEventRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<InventoryEventResponse>(new InvalidOperationException("Cosmos is unavailable.")));
        var sut = CreateSut(inventoryEventService, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create")));

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.Abandoned, outcome.Kind);
    }

    [Fact(DisplayName = "An OperationCanceledException from CreateAsync is not swallowed as Abandoned")]
    public async Task HandleMessageAsync_OperationCanceledFromCreate_Propagates()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        inventoryEventService.CreateAsync(Arg.Any<CreateInventoryEventRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<InventoryEventResponse>(new OperationCanceledException()));
        var sut = CreateSut(inventoryEventService, out _);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create")));

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => sut.HandleMessageAsync(message, CancellationToken.None));
    }

    [Fact(DisplayName = "The CorrelationId application property, when present, wins over the envelope's own correlation id")]
    public async Task HandleMessageAsync_CorrelationIdPropertyPresent_UsesPropertyValue()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out var correlationContext);

        var message = BuildReceivedMessage(
            BuildEnvelopeJson(BuildInboundJson(eventType: "Create"), correlationId: "corr-envelope"),
            correlationIdProperty: "corr-property");

        await sut.HandleMessageAsync(message, CancellationToken.None);

        correlationContext.Received(1).Set(
            "corr-property", Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<LogCriteria>(), Arg.Any<string>());
    }

    [Fact(DisplayName = "Without a CorrelationId application property, the envelope's own correlation id is used")]
    public async Task HandleMessageAsync_CorrelationIdPropertyAbsent_FallsBackToEnvelopeCorrelationId()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out var correlationContext);

        var message = BuildReceivedMessage(
            BuildEnvelopeJson(BuildInboundJson(eventType: "Create"), correlationId: "corr-envelope"),
            correlationIdProperty: null);

        await sut.HandleMessageAsync(message, CancellationToken.None);

        correlationContext.Received(1).Set(
            "corr-envelope", Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<LogCriteria>(), Arg.Any<string>());
    }

    [Fact(DisplayName = "With neither a CorrelationId property nor an envelope correlation id, a fresh id is generated")]
    public async Task HandleMessageAsync_NoCorrelationIdAnywhere_GeneratesNewId()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out var correlationContext);

        var message = BuildReceivedMessage(
            BuildEnvelopeJson(BuildInboundJson(eventType: "Create"), correlationId: null),
            correlationIdProperty: null);

        await sut.HandleMessageAsync(message, CancellationToken.None);

        correlationContext.Received(1).Set(
            Arg.Is<string>(id => IsGuid(id)),
            Arg.Any<string>(), Arg.Any<IReadOnlyList<string>>(), Arg.Any<LogCriteria>(), Arg.Any<string>());
    }

    [Fact(DisplayName = "The envelope's Type is carried into the correlation context's Types list when present")]
    public async Task HandleMessageAsync_EnvelopeTypePresent_PopulatesTypesList()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out var correlationContext);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create"), type: "InventoryStateChanged"));

        await sut.HandleMessageAsync(message, CancellationToken.None);

        correlationContext.Received(1).Set(
            Arg.Any<string>(), Arg.Any<string>(),
            Arg.Is<IReadOnlyList<string>>(types => types.Count == 1 && types[0] == "InventoryStateChanged"),
            Arg.Any<LogCriteria>(), Arg.Any<string>());
    }

    [Fact(DisplayName = "A missing envelope Type results in an empty Types list, and a missing AppId falls back to empty string")]
    public async Task HandleMessageAsync_EnvelopeTypeAndAppIdAbsent_PopulatesEmptyDefaults()
    {
        var inventoryEventService = Substitute.For<IInventoryEventService>();
        var sut = CreateSut(inventoryEventService, out var correlationContext);

        var message = BuildReceivedMessage(BuildEnvelopeJson(BuildInboundJson(eventType: "Create"), appId: null, type: null));

        await sut.HandleMessageAsync(message, CancellationToken.None);

        correlationContext.Received(1).Set(
            Arg.Any<string>(), string.Empty,
            Arg.Is<IReadOnlyList<string>>(types => types.Count == 0),
            Arg.Any<LogCriteria>(), Arg.Any<string>());
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

    private static ServiceBusReceivedMessage BuildReceivedMessage(string envelopeJson, string? correlationIdProperty = "corr-property") =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString(envelopeJson),
            messageId: "msg-1",
            sessionId: "WH1:SKU1",
            properties: correlationIdProperty is null
                ? new Dictionary<string, object>()
                : new Dictionary<string, object> { ["CorrelationId"] = correlationIdProperty });

    private static ServiceBusConsumerHostedService CreateSut(
        IInventoryEventService inventoryEventService,
        out ICorrelationContext correlationContext,
        ServiceBusHealthState? healthState = null,
        IDynamicEventValidator? dynamicEventValidator = null)
    {
        var scopedCorrelationContext = Substitute.For<ICorrelationContext>();
        correlationContext = scopedCorrelationContext;

        var validator = dynamicEventValidator ?? CreatePassthroughValidator();

        var services = new ServiceCollection();
        services.AddSingleton(scopedCorrelationContext);
        services.AddSingleton(inventoryEventService);
        services.AddSingleton(validator);
        var scopeFactory = services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();

        return new ServiceBusConsumerHostedService(
            new FakeServiceBusClient(),
            Options.Create(new ServiceBusConsumerOptions()),
            scopeFactory,
            healthState ?? new ServiceBusHealthState(),
            Substitute.For<ILogger<ServiceBusConsumerHostedService>>());
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
