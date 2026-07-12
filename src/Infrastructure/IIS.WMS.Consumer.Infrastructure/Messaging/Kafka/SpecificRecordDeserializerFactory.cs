using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Builds a Confluent Schema Registry client and the Avro <see cref="IDeserializer{T}"/> wired to it
/// for a given Avro-generated <c>ISpecificRecord</c> type - the reusable, testable counterpart to
/// what <see cref="InventoryStateChangedConsumerHostedService"/> used to build inline in a private
/// static method. Kept separate from <see cref="ConsumerHostedService"/> so a second
/// Avro-schema consumer (integration-resiliency.instructions.md §1: "adding a third schema consumer
/// means adding its options/hosted-service/health-check trio") can reuse this instead of duplicating
/// the Schema Registry wiring.
/// </summary>
public interface ISpecificRecordDeserializerFactory
{
    /// <summary>Builds an Avro deserializer for <typeparamref name="TAvro"/> against the given Schema Registry.</summary>
    /// <typeparam name="TAvro">The Avro-generated <c>ISpecificRecord</c> type to deserialize into.</typeparam>
    /// <param name="schemaRegistryUrl">Confluent Schema Registry URL used to resolve the writer schema.</param>
    /// <param name="schemaRegistryApiKey">
    /// Schema Registry API key - <see langword="null"/>/empty for the unauthenticated local emulator.
    /// A real Confluent Cloud registry rejects every request with 401 without this (and
    /// <paramref name="schemaRegistryApiSecret"/>) set, even if the caller already authenticated
    /// against the Kafka cluster itself - the two use separate credentials.
    /// </param>
    /// <param name="schemaRegistryApiSecret">Schema Registry API secret, paired with <paramref name="schemaRegistryApiKey"/>.</param>
    /// <param name="schemaRegistryClient">
    /// The Schema Registry client created for this deserializer - the caller owns disposing it
    /// alongside the Kafka consumer it's wired into (see <see cref="ConsumerHostedService"/>'s
    /// <c>additionalDisposable</c>).
    /// </param>
    /// <returns>A deserializer ready to pass to <see cref="ConsumerBuilder{TKey,TValue}.SetValueDeserializer"/>.</returns>
    IDeserializer<TAvro> Create<TAvro>(
        string schemaRegistryUrl, string? schemaRegistryApiKey, string? schemaRegistryApiSecret, out ISchemaRegistryClient schemaRegistryClient)
        where TAvro : Avro.Specific.ISpecificRecord;
}

/// <inheritdoc cref="ISpecificRecordDeserializerFactory" />
public sealed class SpecificRecordDeserializerFactory(ILogger<SpecificRecordDeserializerFactory> logger)
    : ISpecificRecordDeserializerFactory
{
    /// <inheritdoc />
    public IDeserializer<TAvro> Create<TAvro>(
        string schemaRegistryUrl, string? schemaRegistryApiKey, string? schemaRegistryApiSecret, out ISchemaRegistryClient schemaRegistryClient)
        where TAvro : Avro.Specific.ISpecificRecord
    {
        logger.LogDebug("Building Avro Schema Registry deserializer for {AvroType}.", typeof(TAvro).Name);

        var config = new SchemaRegistryConfig { Url = schemaRegistryUrl };

        // Left unset (no Authorization header at all) when either half is missing - correct for the
        // unauthenticated local emulator, never for a real Confluent Cloud registry, which 401s every
        // request without both. See the interface doc comment for why this can't just reuse the
        // Kafka SASL Username/Password - they're different credentials for different services.
        if (!string.IsNullOrEmpty(schemaRegistryApiKey) && !string.IsNullOrEmpty(schemaRegistryApiSecret))
        {
            config.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo;
            config.BasicAuthUserInfo = $"{schemaRegistryApiKey}:{schemaRegistryApiSecret}";
        }

        schemaRegistryClient = new CachedSchemaRegistryClient(config);

        // AvroDeserializer<T> only exposes an async API; ConsumerBuilder<TKey,TValue> needs a
        // synchronous IDeserializer<T> (Confluent.Kafka's Consume() call itself is synchronous),
        // hence AsSyncOverAsync() - the same wrapper the inline version this replaces already used.
        return new AvroDeserializer<TAvro>(schemaRegistryClient).AsSyncOverAsync();
    }
}
