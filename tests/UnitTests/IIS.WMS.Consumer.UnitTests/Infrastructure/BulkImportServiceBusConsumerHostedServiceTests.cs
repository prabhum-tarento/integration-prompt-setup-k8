using System.Text.Json;
using Azure.Messaging.ServiceBus;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.Logging;
using IIS.WMS.Common.Messaging.ServiceBus;
using IIS.WMS.Consumer.Application.BulkInventoryImport;
using IIS.WMS.Consumer.Application.BulkInventoryImport.Dtos;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.BulkInventoryImport;
using IIS.WMS.Consumer.UnitTests.Infrastructure.TestDoubles;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Tests for <see cref="BulkImportServiceBusConsumerHostedService.HandleMessageAsync"/> - the
/// non-session Service Bus consumer's message-handling core, tested directly (per
/// integration-resiliency.instructions.md §9) rather than through a real
/// <see cref="ServiceBusProcessor"/>, which cannot be subscribed to without a genuinely connected
/// <see cref="ServiceBusClient"/>.
/// </summary>
public class BulkImportServiceBusConsumerHostedServiceTests
{
    private static bool IsGuid(string? value) => !string.IsNullOrEmpty(value) && Guid.TryParse(value, out _);

    [Fact(DisplayName = "A valid message is imported via IBulkInventoryImportService and the outcome is Completed")]
    public async Task HandleMessageAsync_ValidMessage_ImportsAndReturnsCompleted()
    {
        var bulkImportService = Substitute.For<IBulkInventoryImportService>();
        var healthStateRegistry = new ServiceBusHealthStateRegistry();
        var sut = CreateSut(bulkImportService, out _, healthStateRegistry: healthStateRegistry);

        var message = BuildReceivedMessage(BuildEventJson());

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.Completed, outcome.Kind);
        await bulkImportService.Received(1).ImportAsync(
            Arg.Is<ImportBulkInventoryItemRequest>(r =>
                r.WarehouseId == "WH1" && r.Sku == "SKU1" && r.Quantity == 42 && r.SourceSystem == "Nexus"),
            Arg.Any<CancellationToken>());
        Assert.True(healthStateRegistry.GetOrAdd("inventory-bulk-import").LastSuccessfulReceiveUtc > DateTimeOffset.UnixEpoch);
    }

    [Fact(DisplayName = "Malformed JSON is dead-lettered as a poison message rather than throwing")]
    public async Task HandleMessageAsync_MalformedJson_ReturnsDeadLetteredPoisonMessage()
    {
        var bulkImportService = Substitute.For<IBulkInventoryImportService>();
        var sut = CreateSut(bulkImportService, out _);

        var message = BuildReceivedMessage("{ not valid json");

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.DeadLettered, outcome.Kind);
        Assert.Equal("PoisonMessage", outcome.Reason);
        await bulkImportService.DidNotReceiveWithAnyArgs().ImportAsync(default!, default);
    }

    [Fact(DisplayName = "A JSON literal null body is dead-lettered as a poison message")]
    public async Task HandleMessageAsync_NullPayload_ReturnsDeadLetteredPoisonMessage()
    {
        var bulkImportService = Substitute.For<IBulkInventoryImportService>();
        var sut = CreateSut(bulkImportService, out _);

        var message = BuildReceivedMessage("null");

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.DeadLettered, outcome.Kind);
        Assert.Equal("PoisonMessage", outcome.Reason);
        Assert.Equal("Deserialized payload was null.", outcome.Description);
    }

    [Fact(DisplayName = "A transient failure from IBulkInventoryImportService is abandoned for redelivery")]
    public async Task HandleMessageAsync_ImportThrows_ReturnsAbandoned()
    {
        var bulkImportService = Substitute.For<IBulkInventoryImportService>();
        bulkImportService.ImportAsync(Arg.Any<ImportBulkInventoryItemRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("Cosmos is unavailable.")));
        var sut = CreateSut(bulkImportService, out _);

        var message = BuildReceivedMessage(BuildEventJson());

        var outcome = await sut.HandleMessageAsync(message, CancellationToken.None);

        Assert.Equal(ServiceBusMessageOutcomeKind.Abandoned, outcome.Kind);
    }

    [Fact(DisplayName = "An OperationCanceledException from IBulkInventoryImportService is not swallowed as Abandoned")]
    public async Task HandleMessageAsync_ImportThrowsOperationCanceled_Propagates()
    {
        var bulkImportService = Substitute.For<IBulkInventoryImportService>();
        bulkImportService.ImportAsync(Arg.Any<ImportBulkInventoryItemRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new OperationCanceledException()));
        var sut = CreateSut(bulkImportService, out _);

        var message = BuildReceivedMessage(BuildEventJson());

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => sut.HandleMessageAsync(message, CancellationToken.None));
    }

    [Fact(DisplayName = "The CorrelationId application property, when present, is used as the correlation id")]
    public async Task HandleMessageAsync_CorrelationIdHeaderPresent_UsesHeaderValue()
    {
        var bulkImportService = Substitute.For<IBulkInventoryImportService>();
        var sut = CreateSut(bulkImportService, out var correlationContext);

        var message = BuildReceivedMessage(BuildEventJson(), correlationId: "corr-from-header");

        await sut.HandleMessageAsync(message, CancellationToken.None);

        correlationContext.Received(1).Set(
            "corr-from-header", string.Empty, Arg.Any<IReadOnlyList<string>>(), Arg.Any<LogCriteria>(), Arg.Any<string>());
    }

    [Fact(DisplayName = "A missing CorrelationId application property falls back to a freshly generated id")]
    public async Task HandleMessageAsync_CorrelationIdHeaderAbsent_GeneratesNewId()
    {
        var bulkImportService = Substitute.For<IBulkInventoryImportService>();
        var sut = CreateSut(bulkImportService, out var correlationContext);

        var message = BuildReceivedMessage(BuildEventJson(), correlationId: null);

        await sut.HandleMessageAsync(message, CancellationToken.None);

        correlationContext.Received(1).Set(
            Arg.Is<string>(id => IsGuid(id)),
            string.Empty, Arg.Any<IReadOnlyList<string>>(), Arg.Any<LogCriteria>(), Arg.Any<string>());
    }

    [Fact(DisplayName = "A CorrelationId application property present with a null value falls back to a freshly generated id")]
    public async Task HandleMessageAsync_CorrelationIdHeaderPresentButNull_GeneratesNewId()
    {
        var bulkImportService = Substitute.For<IBulkInventoryImportService>();
        var sut = CreateSut(bulkImportService, out var correlationContext);

        var message = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString(BuildEventJson()),
            messageId: "msg-1",
            properties: new Dictionary<string, object> { ["CorrelationId"] = null! });

        await sut.HandleMessageAsync(message, CancellationToken.None);

        correlationContext.Received(1).Set(
            Arg.Is<string>(id => IsGuid(id)),
            string.Empty, Arg.Any<IReadOnlyList<string>>(), Arg.Any<LogCriteria>(), Arg.Any<string>());
    }

    private static string BuildEventJson(
        string eventId = "evt-1", string warehouseId = "WH1", string sku = "SKU1",
        int quantity = 42, string sourceSystem = "Nexus", long lastUpdatedUtcMillis = 1_700_000_000_000) =>
        JsonSerializer.Serialize(new
        {
            EventId = eventId,
            WarehouseId = warehouseId,
            Sku = sku,
            Quantity = quantity,
            SourceSystem = sourceSystem,
            LastUpdatedUtcMillis = lastUpdatedUtcMillis,
        });

    private static ServiceBusReceivedMessage BuildReceivedMessage(string bodyJson, string? correlationId = "corr-1") =>
        ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString(bodyJson),
            messageId: "msg-1",
            properties: correlationId is null
                ? new Dictionary<string, object>()
                : new Dictionary<string, object> { ["CorrelationId"] = correlationId });

    private static BulkImportServiceBusConsumerHostedService CreateSut(
        IBulkInventoryImportService bulkImportService,
        out ICorrelationContext correlationContext,
        ServiceBusHealthStateRegistry? healthStateRegistry = null)
    {
        var scopedCorrelationContext = Substitute.For<ICorrelationContext>();
        correlationContext = scopedCorrelationContext;

        var services = new ServiceCollection();
        services.AddSingleton(scopedCorrelationContext);
        services.AddSingleton(bulkImportService);
        var scopeFactory = services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();

        return new BulkImportServiceBusConsumerHostedService(
            new FakeServiceBusClient(),
            Options.Create(new BulkImportServiceBusConsumerOptions()),
            scopeFactory,
            healthStateRegistry ?? new ServiceBusHealthStateRegistry(),
            Substitute.For<ILogger<BulkImportServiceBusConsumerHostedService>>());
    }
}
