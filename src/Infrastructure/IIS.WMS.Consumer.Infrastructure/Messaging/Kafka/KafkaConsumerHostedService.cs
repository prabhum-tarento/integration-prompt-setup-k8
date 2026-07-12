using System.Text.Json;
using IIS.WMS.Consumer.Application.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Relays JSON-contract inventory events from Kafka onto the durable Azure Service Bus queue
/// (integration-resiliency.instructions.md §1), built on the shared <see cref="ConsumerHostedService"/>
/// - the JSON counterpart to the Avro <see cref="InventoryStateChangedConsumerHostedService"/>. Handles
/// exactly one schema regardless of the Kafka <c>Type</c> header's value (registered under
/// <see cref="DefaultEventType"/>), same as before this class supported registering more than one.
/// </summary>
public sealed class KafkaConsumerHostedService : ConsumerHostedService
{
    /// <summary>Builds the long-lived Kafka consumer (with a JSON value deserializer) and the Service Bus sender it relays onto.</summary>
    /// <param name="options">Kafka connection/topic settings.</param>
    /// <param name="infrastructure">The Service Bus client, Polly pipeline provider, hot/cold file stores, Blob Storage options, and dedup service every consumer shares - see <see cref="ConsumerRelayInfrastructure"/>.</param>
    /// <param name="healthState">Shared state updated on every poll, read by this consumer's <see cref="ConsumerHealthCheck"/>.</param>
    /// <param name="logger">Logger for consume/relay/poison-message events.</param>
    public KafkaConsumerHostedService(
        IOptions<KafkaConsumerOptions> options,
        ConsumerRelayInfrastructure infrastructure,
        [FromKeyedServices(MessagingServiceCollectionExtensions.InventoryEventsConsumerKey)] ConsumerHealthState healthState,
        ILogger<KafkaConsumerHostedService> logger)
        : base(
            options.Value,
            "InventoryEvents Kafka consumer",
            new Dictionary<string, ISchemaHandler>
            {
                [DefaultEventType] = CreateSchemaHandler(
                    new JsonDeserializer<InboundInventoryEventMessage>(),
                    value => JsonSerializer.Serialize(value),
                    value => ($"{value.WarehouseId}:{value.Sku}", value.EventId)),
            },
            infrastructure,
            healthState,
            logger)
    {
    }
}
