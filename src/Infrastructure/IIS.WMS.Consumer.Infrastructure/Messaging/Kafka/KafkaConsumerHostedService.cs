using System.Text.Json;
using IIS.WMS.Common.Logging;
using IIS.WMS.Common.Messaging;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Relays JSON-contract inventory events from Kafka onto the durable Azure Service Bus queue
/// (integration-resiliency.instructions.md §1), built on the shared <see cref="KafkaConsumerHostedServiceBase"/>
/// - the JSON counterpart to the Avro <see cref="InventoryStateChangedConsumerHostedService"/>. Handles
/// exactly one schema regardless of the Kafka <c>Type</c> header's value (registered under
/// <see cref="DefaultEventType"/>), same as before this class supported registering more than one.
/// </summary>
[LogLevelCriteria(LogCriteria.High)]
[Module("Inventory")]
public sealed class KafkaConsumerHostedService : KafkaConsumerHostedServiceBase
{
    /// <summary>Builds the long-lived Kafka consumer (with a JSON value deserializer) and the Service Bus sender it relays onto.</summary>
    /// <param name="options">Kafka connection/topic settings.</param>
    /// <param name="infrastructure">The Service Bus client, Polly pipeline provider, hot/cold file stores, Blob Storage options, and dedup service every consumer shares - see <see cref="ConsumerRelayInfrastructure"/>.</param>
    /// <param name="logger">Logger for consume/relay/poison-message events.</param>
    public KafkaConsumerHostedService(
        IOptions<KafkaConsumerOptions> options,
        ConsumerRelayInfrastructure infrastructure,
        ILogger<KafkaConsumerHostedService> logger)
        : base(options.Value, infrastructure, logger)
    {
        RegisterSchemaHandlers(new Dictionary<string, ISchemaHandler>
        {
            [DefaultEventType] = CreateSchemaHandler(
                new JsonDeserializer<InboundInventoryEventMessage>(),
                value => JsonSerializer.Serialize(value),
                (value, _) => ($"{value.WarehouseId}:{value.Sku}", value.EventId)),
        });
    }
}
