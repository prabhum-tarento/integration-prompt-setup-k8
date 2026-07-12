using System.Text.Json;
using FluentValidation;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka.AvroContracts;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Relays the high-volume, unordered bulk-import Avro event from Kafka onto a
/// <b>non-session</b> Service Bus queue (integration-resiliency.instructions.md §1) - built on the
/// shared <see cref="ConsumerHostedService"/> like the other two consumers, but this one's
/// target queue has <c>RequiresSession = false</c>: the upstream data is an idempotent snapshot
/// reload with no per-aggregate ordering requirement, so there is no reason to pay the
/// <c>MaxConcurrentCallsPerSession = 1</c> serialization cost sessions would otherwise impose on any
/// warehouse/SKU repeated across the burst. The registered handler's <c>SessionId</c> is
/// therefore unused by the downstream consumer (<c>BulkImportServiceBusConsumerHostedService</c>) -
/// harmless to still set (Service Bus stores it as ordinary metadata on a non-session queue), kept
/// only because <see cref="ConsumerHostedService"/>'s shared per-message flow always sets it. Handles
/// exactly one schema regardless of the Kafka <c>Type</c> header's value (registered under
/// <see cref="DefaultEventType"/>), same as before this class supported registering more than one.
/// </summary>
public sealed class BulkInventoryImportConsumerHostedService : ConsumerHostedService
{
    private static readonly JsonSerializerOptions RelayJsonOptions = new() { IgnoreReadOnlyProperties = true };

    /// <summary>Builds the schema-registry-backed Avro consumer and the Service Bus sender it relays onto.</summary>
    /// <param name="options">Topic, consumer group, Schema Registry URL, and Service Bus queue settings for this consumer.</param>
    /// <param name="specificRecordDeserializerFactory">Builds the Avro deserializer and its backing Schema Registry client.</param>
    /// <param name="infrastructure">The Service Bus client, Polly pipeline provider, hot/cold file stores, Blob Storage options, and dedup service every consumer shares - see <see cref="ConsumerRelayInfrastructure"/>.</param>
    /// <param name="validator">Field-level validation for one deserialized event - the single validation point for bulk-import data.</param>
    /// <param name="healthState">Shared state updated on every poll, read by this consumer's <see cref="ConsumerHealthCheck"/>.</param>
    /// <param name="logger">Logger for consume/relay/poison-message events.</param>
    public BulkInventoryImportConsumerHostedService(
        IOptions<BulkInventoryImportConsumerOptions> options,
        ISpecificRecordDeserializerFactory specificRecordDeserializerFactory,
        ConsumerRelayInfrastructure infrastructure,
        IValidator<BulkInventoryImportEvent> validator,
        [FromKeyedServices(MessagingServiceCollectionExtensions.BulkInventoryImportConsumerKey)] ConsumerHealthState healthState,
        ILogger<BulkInventoryImportConsumerHostedService> logger)
        : base(
            options.Value,
            "BulkInventoryImport Kafka consumer",
            new Dictionary<string, ISchemaHandler>
            {
                [DefaultEventType] = CreateSchemaHandler(
                    specificRecordDeserializerFactory.Create<BulkInventoryImportEvent>(
                        options.Value.SchemaRegistryUrl
                            ?? throw new InvalidOperationException(
                                $"Missing SchemaRegistryUrl - configure '{BulkInventoryImportConsumerOptions.SectionName}:SchemaRegistryUrl' " +
                                $"or the Kafka-level '{KafkaConsumerOptions.SectionName}:SchemaRegistryUrl' fallback."),
                        options.Value.SchemaRegistryApiKey,
                        options.Value.SchemaRegistryApiSecret,
                        out var schemaRegistryClient),
                    value => JsonSerializer.Serialize(value, RelayJsonOptions),
                    // SessionId is unused downstream (non-session queue) - see the class-level
                    // remarks. MessageId is EventId, the upstream system's own deterministic key,
                    // which is what makes the Service Bus consumer's dedupe check on redelivery work.
                    value => (value.EventId, value.EventId),
                    async (value, ct) =>
                    {
                        // The single validation point for bulk-import data. Field-level rule
                        // violations throw (via ValidateAndThrowAsync) and are handled like any other
                        // non-fatal failure by the base class - this consumer has no "valid but
                        // intentionally unforwarded" case today, so it never returns false.
                        await validator.ValidateAndThrowAsync(value, ct);

                        return true;
                    }),
            },
            infrastructure,
            healthState,
            logger,
            additionalDisposable: schemaRegistryClient)
    {
    }
}
