using System.Text.Json;
using Azure.Messaging.ServiceBus;
using IIS.WMS.Consumer.Application.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly.Registry;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Relays JSON-contract inventory events from Kafka onto the durable Azure Service Bus queue
/// (integration-resiliency.instructions.md §1), built on the shared <see cref="ConsumerHostedService{TValue}"/>
/// - the JSON counterpart to the Avro <see cref="InventoryStateChangedConsumerHostedService"/>.
/// </summary>
public sealed class KafkaConsumerHostedService : ConsumerHostedService<InboundInventoryEventMessage>
{
    /// <summary>Builds the long-lived Kafka consumer (with a JSON value deserializer) and the Service Bus sender it relays onto.</summary>
    /// <param name="options">Kafka connection/topic settings.</param>
    /// <param name="serviceBusClient">Client used to create the sender for the relay queue.</param>
    /// <param name="pipelineProvider">Resolves the named Polly pipeline used for the Service Bus publish step.</param>
    /// <param name="fileStore">Writes the cold-tier and hot-tier audit blobs.</param>
    /// <param name="deduplicationService">Checks each message against the Nexus deduplication service.</param>
    /// <param name="healthState">Shared state updated on every poll, read by this consumer's <see cref="ConsumerHealthCheck"/>.</param>
    /// <param name="logger">Logger for consume/relay/poison-message events.</param>
    public KafkaConsumerHostedService(
        IOptions<KafkaConsumerOptions> options,
        ServiceBusClient serviceBusClient,
        ResiliencePipelineProvider<string> pipelineProvider,
        IFileStore fileStore,
        IDeduplicationService deduplicationService,
        [FromKeyedServices(MessagingServiceCollectionExtensions.InventoryEventsConsumerKey)] ConsumerHealthState healthState,
        ILogger<KafkaConsumerHostedService> logger)
        : base(
            options.Value,
            "InventoryEvents Kafka consumer",
            new JsonDeserializer<InboundInventoryEventMessage>(),
            serviceBusClient,
            pipelineProvider,
            fileStore,
            deduplicationService,
            healthState,
            logger)
    {
    }

    /// <inheritdoc />
    protected override (string SessionId, string MessageId, string Body) MapToServiceBusMessage(InboundInventoryEventMessage value)
    {
        var sessionId = $"{value.WarehouseId}:{value.Sku}";

        return (sessionId, value.EventId, JsonSerializer.Serialize(value));
    }
}
