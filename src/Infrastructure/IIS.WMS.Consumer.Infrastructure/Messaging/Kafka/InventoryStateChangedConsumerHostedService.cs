using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using net.pandora.nexus.@event.inventory;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Relays <c>net.pandora.nexus.event.inventory.InventoryStateChanged</c> Avro events from Kafka
/// onto the durable Azure Service Bus queue (integration-resiliency.instructions.md §1), built on
/// the shared <see cref="ConsumerHostedService"/> - the Avro counterpart to the JSON
/// <see cref="KafkaConsumerHostedService"/>. Handles exactly one schema regardless of the Kafka
/// <c>Type</c> header's value (registered under <see cref="DefaultEventType"/>), same as before this
/// class supported registering more than one. Unlike that JSON contract, this event carries one
/// <c>location</c> but an array of <c>itemLines</c> (potentially several products) - the
/// <c>{WarehouseId}:{Sku}</c> SessionId convention that doc describes does not apply 1:1, so this
/// relay uses <c>location.id</c> alone and forwards the event (including all its item lines) as a
/// single Service Bus message, per team decision - a future per-item-line fan-out would need a
/// different SessionId derivation.
/// </summary>
public sealed class InventoryStateChangedConsumerHostedService : ConsumerHostedService
{
    /// <summary>
    /// <c>IgnoreReadOnlyProperties</c> skips the Avro-generated SpecificRecord's get-only
    /// <c>Schema</c> property, which System.Text.Json would otherwise try (and fail) to serialize.
    /// </summary>
    private static readonly JsonSerializerOptions RelayJsonOptions = new() { IgnoreReadOnlyProperties = true };

    /// <summary>Builds the schema-registry-backed Avro consumer and the Service Bus sender it relays onto.</summary>
    /// <param name="options">Topic, consumer group, Schema Registry URL, and Service Bus queue settings for this consumer.</param>
    /// <param name="specificRecordDeserializerFactory">Builds the Avro deserializer and its backing Schema Registry client.</param>
    /// <param name="infrastructure">The Service Bus client, Polly pipeline provider, hot/cold file stores, Blob Storage options, and dedup service every consumer shares - see <see cref="ConsumerRelayInfrastructure"/>.</param>
    /// <param name="healthState">Shared state updated on every poll, read by this consumer's <see cref="ConsumerHealthCheck"/>.</param>
    /// <param name="logger">Logger for consume/relay/poison-message events.</param>
    public InventoryStateChangedConsumerHostedService(
        IOptions<InventoryStateChangedConsumerOptions> options,
        ISpecificRecordDeserializerFactory specificRecordDeserializerFactory,
        ConsumerRelayInfrastructure infrastructure,
        [FromKeyedServices(MessagingServiceCollectionExtensions.InventoryStateChangedConsumerKey)] ConsumerHealthState healthState,
        ILogger<InventoryStateChangedConsumerHostedService> logger)
        : base(
            options.Value,
            "InventoryStateChanged Kafka consumer",
            new Dictionary<string, ISchemaHandler>
            {
                [DefaultEventType] = CreateSchemaHandler(
                    specificRecordDeserializerFactory.Create<InventoryStateChanged>(
                        options.Value.SchemaRegistryUrl
                            ?? throw new InvalidOperationException(
                                $"Missing SchemaRegistryUrl - configure '{InventoryStateChangedConsumerOptions.SectionName}:SchemaRegistryUrl' " +
                                $"or the Kafka-level '{KafkaConsumerOptions.SectionName}:SchemaRegistryUrl' fallback."),
                        options.Value.SchemaRegistryApiKey,
                        options.Value.SchemaRegistryApiSecret,
                        out var schemaRegistryClient),
                    value => JsonSerializer.Serialize(value, RelayJsonOptions),
                    // location.id, not {WarehouseId}:{Sku} - see the class-level remarks on why this
                    // event's shape (one location, an array of itemLines) doesn't fit that convention.
                    // id is the schema's own declared dedup key (see the .avsc's metadata.key).
                    value => (value.location.id, value.id)),
            },
            infrastructure,
            healthState,
            logger,
            additionalDisposable: schemaRegistryClient)
    {
    }
}
