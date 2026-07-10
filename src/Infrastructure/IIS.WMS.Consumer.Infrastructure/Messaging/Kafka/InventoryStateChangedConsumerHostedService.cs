using System.Text.Json;
using Azure.Messaging.ServiceBus;
using IIS.WMS.Consumer.Application.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly.Registry;
using net.pandora.nexus.@event.inventory;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Relays <c>net.pandora.nexus.event.inventory.InventoryStateChanged</c> Avro events from Kafka
/// onto the durable Azure Service Bus queue (integration-resiliency.instructions.md §1), built on
/// the shared <see cref="ConsumerHostedService{TValue}"/> - the Avro counterpart to the JSON
/// <see cref="KafkaConsumerHostedService"/>. Unlike that JSON contract, this event carries one
/// <c>location</c> but an array of <c>itemLines</c> (potentially several products) - the
/// <c>{WarehouseId}:{Sku}</c> SessionId convention that doc describes does not apply 1:1, so this
/// relay uses <c>location.id</c> alone and forwards the event (including all its item lines) as a
/// single Service Bus message, per team decision - a future per-item-line fan-out would need a
/// different SessionId derivation.
/// </summary>
public sealed class InventoryStateChangedConsumerHostedService : ConsumerHostedService<InventoryStateChanged>
{
    private static readonly JsonSerializerOptions RelayJsonOptions = new() { IgnoreReadOnlyProperties = true };

    /// <summary>Builds the schema-registry-backed Avro consumer and the Service Bus sender it relays onto.</summary>
    /// <param name="options">Topic, consumer group, Schema Registry URL, and Service Bus queue settings for this consumer.</param>
    /// <param name="specificRecordDeserializerFactory">Builds the Avro deserializer and its backing Schema Registry client.</param>
    /// <param name="serviceBusClient">Client used to create the sender for the relay queue.</param>
    /// <param name="pipelineProvider">Resolves the named Polly pipeline used for the Service Bus publish step.</param>
    /// <param name="fileStore">Writes the cold-tier and hot-tier audit blobs.</param>
    /// <param name="deduplicationService">Checks each message against the Nexus deduplication service.</param>
    /// <param name="healthState">Shared state updated on every poll, read by this consumer's <see cref="ConsumerHealthCheck"/>.</param>
    /// <param name="logger">Logger for consume/relay/poison-message events.</param>
    public InventoryStateChangedConsumerHostedService(
        IOptions<InventoryStateChangedConsumerOptions> options,
        ISpecificRecordDeserializerFactory specificRecordDeserializerFactory,
        ServiceBusClient serviceBusClient,
        ResiliencePipelineProvider<string> pipelineProvider,
        IFileStore fileStore,
        IDeduplicationService deduplicationService,
        [FromKeyedServices(MessagingServiceCollectionExtensions.InventoryStateChangedConsumerKey)] ConsumerHealthState healthState,
        ILogger<InventoryStateChangedConsumerHostedService> logger)
        : base(
            options.Value,
            "InventoryStateChanged Kafka consumer",
            specificRecordDeserializerFactory.Create<InventoryStateChanged>(
                options.Value.SchemaRegistryUrl
                    ?? throw new InvalidOperationException(
                        $"Missing SchemaRegistryUrl - configure '{InventoryStateChangedConsumerOptions.SectionName}:SchemaRegistryUrl' " +
                        $"or the Kafka-level '{KafkaConsumerOptions.SectionName}:SchemaRegistryUrl' fallback."),
                out var schemaRegistryClient),
            serviceBusClient,
            pipelineProvider,
            fileStore,
            deduplicationService,
            healthState,
            logger,
            additionalDisposable: schemaRegistryClient)
    {
    }

    /// <inheritdoc />
    protected override (string SessionId, string MessageId, string Body) MapToServiceBusMessage(InventoryStateChanged value)
    {
        // location.id, not {WarehouseId}:{Sku} - see the type-level remarks on why this event's
        // shape (one location, an array of itemLines) doesn't fit that convention.
        var sessionId = value.location.id;

        // id is the schema's own declared dedup key (see the .avsc's metadata.key).
        // IgnoreReadOnlyProperties skips the Avro-generated SpecificRecord's get-only Schema
        // property, which System.Text.Json would otherwise try (and fail) to serialize.
        return (sessionId, value.id, JsonSerializer.Serialize(value, RelayJsonOptions));
    }
}
