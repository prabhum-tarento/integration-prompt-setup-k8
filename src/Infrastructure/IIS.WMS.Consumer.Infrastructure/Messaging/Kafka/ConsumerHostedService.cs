using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Domain.Aggregates;
using IIS.WMS.Consumer.Infrastructure.BlobStorage;
using IIS.WMS.Consumer.Infrastructure.Resilience;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly.Registry;
using Serilog.Context;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Generic Kafka → Service Bus relay (integration-resiliency.instructions.md §1) shared by every
/// consumer regardless of wire format. A single topic/consumer group can carry more than one
/// schema/event type - a derived class registers one <see cref="ISchemaHandler"/> per schema it
/// understands (built via <see cref="CreateSchemaHandler{TValue}"/>), keyed by the Kafka
/// <see cref="KafkaHeaderNames.Type"/> header value that schema corresponds to. A consumer that
/// handles exactly one schema regardless of what's in that header registers its one handler under
/// <see cref="DefaultEventType"/> instead of an exact value - see <see cref="ProcessMessageAsync"/>
/// for the exact lookup order. Each schema publishes to its own <see cref="ISchemaHandler.ServiceBusQueueName"/>
/// (falling back to <see cref="ConsumerOptions.ServiceBusQueueName"/> if unset), so multiple schemas on
/// one consumer can land on different queues - <see cref="GetServiceBusSender"/> opens one
/// <see cref="ServiceBusSender"/> per distinct queue actually used, not one per schema or one per
/// consumer. Deserialization itself happens manually, after the dedup check below, not inside
/// <see cref="IConsumer{TKey,TValue}.Consume(TimeSpan)"/> - see the class-level flow remarks.
/// Everything past schema selection (poll loop, bounded-channel worker pool, cold/hot-tier audit
/// logging, deduplication, non-fatal failure handling, ordered offset commit, resilience, correlation
/// id propagation, the <see cref="ConsumerOptions.Enabled"/> toggle) lives here once, regardless of
/// how many schemas a consumer registers.
/// </summary>
/// <remarks>
/// <para>
/// Per-message flow: consume raw bytes → read Kafka headers (correlation id, dedup id, event
/// type, app id) → log them → resolve the schema handler for this message's event type (falling back
/// to <see cref="DefaultEventType"/> if no exact match is registered; hot-tier raw-bytes dead-letter
/// if neither matches) → deserialize → write the deserialized value as JSON to the cold-tier
/// audit container (<see cref="BlobStorageOptions.RequestAuditContainerName"/>, unconditionally -
/// not gated by <see cref="BlobStorageOptions.RequestAuditEnabled"/>, which covers a separate,
/// still-optional audit use) → check <see cref="IDeduplicationService"/> → the resolved handler's
/// <c>ValidateAsync</c> → map and publish to Service Bus → commit.
/// </para>
/// <para>
/// <b>No single event's failure stops the consumer or blocks any other message</b> - deliberate,
/// not just a poison-message backstop: multiple upstream systems produce onto the same topics on
/// their own release cadence, so a producer running a newer version of a shared Avro schema than
/// this consumer's currently-deployed reader is an expected, recurring event. Every failure mode
/// below is handled the same way - log at <c>LogLevel.Critical</c> tagged with which stage
/// failed, write to the hot-tier <see cref="BlobStorageOptions.ConsumerDeadLetterContainerName"/>
/// container for a separate, out-of-band watcher process to reprocess, and commit the offset forward
/// (never redelivered) so the rest of the partition keeps flowing:
/// <list type="bullet">
/// <item>An event type with no registered schema handler (exact or <see cref="DefaultEventType"/>)
/// writes the <b>raw message bytes</b> to hot storage - there is no deserializer to even attempt.</item>
/// <item>A deserialization failure writes the <b>raw message bytes</b> to hot storage - there is no
/// successfully deserialized value to serialize as JSON at that point.</item>
/// <item>A validation failure (the handler's <c>ValidateAsync</c> throws) writes the deserialized
/// value as <b>JSON</b> to hot storage - unlike deserialization, a valid value already exists. This is
/// distinct from <c>ValidateAsync</c> returning <see langword="false"/>, which is not a failure at
/// all - see <see cref="CreateSchemaHandler{TValue}"/>.</item>
/// <item>A Service Bus publish failure - even after <see cref="ResiliencePipelines.ServiceBusPublish"/>'s
/// retries are exhausted - also writes the deserialized value as JSON and commits forward, rather
/// than faulting the worker/stopping the consumer as earlier versions of this class did. Accepted
/// trade-off: unlike a schema mismatch, a struggling Service Bus dependency is usually recoverable on
/// its own, and Kafka redelivery after a consumer restart would normally recover it automatically, in
/// order, for the same <c>{WarehouseId}:{Sku}</c> session (§2). Treating every publish failure as
/// non-fatal here instead means a sustained Service Bus outage dead-letters every in-flight message
/// for its duration rather than surfacing as an outage, and a message that fails to publish while a
/// later message for the *same* session succeeds can leave that Service Bus session's ordering
/// broken until the watcher replays it - both accepted in exchange for "one bad event never blocks
/// the partition" applying uniformly across every stage, not just deserialize/validate.</item>
/// </list>
/// </para>
/// <para>
/// The single-threaded poll loop reads from Kafka and writes each result into a bounded
/// <see cref="Channel{T}"/>; <see cref="ConsumerOptions.WorkerCount"/> concurrent workers drain it
/// and run the flow above (integration-resiliency.instructions.md §6). Because workers can finish out
/// of order, offset commits go through <see cref="PartitionOffsetCommitTracker"/>, which only
/// advances (and commits) a partition's low-water mark once every offset below it has completed -
/// never a plain <c>consumer.Commit(result)</c> per message. This does not handle a partition being
/// revoked and reassigned mid-flight (no
/// <c>SetPartitionsRevokedHandler</c>/<c>SetPartitionsAssignedHandler</c> yet) - a rebalance while
/// messages are in flight can produce a duplicate delivery to whichever consumer picks the partition
/// up next, which the dedup check above now also covers, in addition to the existing downstream
/// Service Bus consumer dedupe (§2).
/// </para>
/// </remarks>
public abstract class ConsumerHostedService : BackgroundService, IAsyncDisposable, IServiceBusSenderCacheSource
{
    /// <summary>
    /// The key a schema handler is registered under when a consumer doesn't distinguish by the Kafka
    /// <see cref="KafkaHeaderNames.Type"/> header - every consumer that (like each one today) handles
    /// exactly one schema regardless of that header's value registers its one handler under this key.
    /// A message's own <c>Type</c> header value is looked up first; if nothing is registered under
    /// that exact value, this key is tried next - see <see cref="ProcessMessageAsync"/>. <c>internal</c>
    /// (not just <c>protected</c>) so <c>MessagingServiceCollectionExtensions</c> can register this
    /// same key's <see cref="ConsumerHealthCheck"/> without duplicating it as a separate literal.
    /// </summary>
    protected internal const string DefaultEventType = "";

    /// <summary>
    /// Shared <c>JsonSerializer.Serialize</c> options for a schema's <c>serializeToJson</c> delegate
    /// (<see cref="CreateSchemaHandler{TValue}"/>) - every Avro-contract consumer needs the same
    /// setting, so it lives here once rather than as an identical private field on each one.
    /// <c>IgnoreReadOnlyProperties</c> skips the Avro-generated SpecificRecord's get-only
    /// <c>Schema</c> property, which System.Text.Json would otherwise try (and fail) to serialize.
    /// </summary>
    protected static readonly JsonSerializerOptions RelayJsonOptions = new() { IgnoreReadOnlyProperties = true };

    /// <summary>
    /// Type-erased per-schema entry - build one via <see cref="CreateSchemaHandler{TValue}"/>, never
    /// implement this directly. Lets <see cref="ConsumerHostedService"/> store handlers for
    /// structurally unrelated schemas (different Avro records, or JSON shapes) in one dictionary
    /// without itself being generic over any single one of them.
    /// </summary>
    protected interface ISchemaHandler
    {
        /// <summary>This schema's type name - the path segment used in the cold/hot-tier blob naming convention.</summary>
        string SchemaName { get; }

        /// <summary>
        /// Service Bus queue this schema's events relay onto, or <see langword="null"/> to fall back
        /// to <see cref="ConsumerOptions.ServiceBusQueueName"/> - the consumer-wide default every
        /// schema used before this override existed, and still what most schemas want (one consumer,
        /// one queue). Set this per schema only when different event types sharing one Kafka
        /// topic/consumer group need to land on different Service Bus queues.
        /// </summary>
        string? ServiceBusQueueName { get; }

        /// <summary>Deserializes raw Kafka bytes into this schema's value, boxed as <see cref="object"/>.</summary>
        object Deserialize(byte[]? data, SerializationContext context);

        /// <summary>Serializes a previously-deserialized value (of this schema) back to JSON.</summary>
        string SerializeToJson(object value);

        /// <summary>
        /// Routes a previously-deserialized value (of this schema) onto Service Bus.
        /// <paramref name="key"/> is the Kafka record key (<c>ConsumeResult.Message.Key</c>) this
        /// value was read under - the routing delegate decides whether/how to use it (e.g. as both
        /// the SessionId and MessageId, the way <see cref="InventoryStateChangedConsumerHostedService"/>
        /// does) instead of deriving them from fields inside <paramref name="value"/> itself.
        /// </summary>
        (string SessionId, string MessageId) GetServiceBusRouting(object value, string? key);

        /// <summary>Business-rule validation for a previously-deserialized value (of this schema).</summary>
        Task<bool> ValidateAsync(object value, CancellationToken cancellationToken);

        /// <summary>
        /// Computes this schema's <c>OrderArchive</c> category key for a previously-deserialized
        /// value, or <see langword="null"/>/empty if this schema doesn't archive to Cosmos before
        /// publishing - see <see cref="ConsumerHostedService.CreateSchemaHandler{TAvro,TValue}"/>'s
        /// <c>getOrderArchiveKey</c> parameter.
        /// </summary>
        string? GetOrderArchiveKey(object value);
    }

    /// <summary>Type-erasing <see cref="ISchemaHandler"/> implementation - see <see cref="CreateSchemaHandler{TValue}"/>, the only place this is constructed.</summary>
    private sealed class SchemaHandler<TValue>(
        IDeserializer<TValue> deserializer,
        Func<TValue, string> serializeToJson,
        Func<TValue, string?, (string SessionId, string MessageId)> getServiceBusRouting,
        Func<TValue, CancellationToken, Task<bool>>? validateAsync,
        string? serviceBusQueueName,
        Func<TValue, string?>? getOrderArchiveKey = null)
        : ISchemaHandler
    {
        private readonly Func<TValue, CancellationToken, Task<bool>> validateAsync = validateAsync ?? ((_, _) => Task.FromResult(true));
        private readonly Func<TValue, string?> getOrderArchiveKey = getOrderArchiveKey ?? (_ => null);

        public string SchemaName { get; } = typeof(TValue).Name;

        public string? ServiceBusQueueName { get; } = serviceBusQueueName;

        public object Deserialize(byte[]? data, SerializationContext context) => deserializer.Deserialize(data, data is null, context)!;

        public string SerializeToJson(object value) => serializeToJson((TValue)value);

        public (string SessionId, string MessageId) GetServiceBusRouting(object value, string? key) => getServiceBusRouting((TValue)value, key);

        public Task<bool> ValidateAsync(object value, CancellationToken cancellationToken) => validateAsync((TValue)value, cancellationToken);

        public string? GetOrderArchiveKey(object value) => getOrderArchiveKey((TValue)value);
    }

    /// <summary>
    /// Type-erasing <see cref="ISchemaHandler"/> implementation for a schema that also maps into an
    /// internal <typeparamref name="TValue"/> DTO - see <see cref="CreateSchemaHandler{TAvro,TValue}"/>,
    /// the only place this is constructed. Everything past <see cref="Deserialize"/> operates on the
    /// mapped <typeparamref name="TValue"/>, never the raw <typeparamref name="TAvro"/>.
    /// </summary>
    private sealed class MappedSchemaHandler<TAvro, TValue>(
        IDeserializer<TAvro> deserializer,
        Func<TAvro, TValue> map,
        Func<TValue, string> serializeToJson,
        Func<TValue, string?, (string SessionId, string MessageId)> getServiceBusRouting,
        Func<TValue, CancellationToken, Task<bool>>? validateAsync,
        string? serviceBusQueueName,
        Func<TValue, string?>? getOrderArchiveKey = null)
        : ISchemaHandler
    {
        private readonly Func<TValue, CancellationToken, Task<bool>> validateAsync = validateAsync ?? ((_, _) => Task.FromResult(true));
        private readonly Func<TValue, string?> getOrderArchiveKey = getOrderArchiveKey ?? (_ => null);

        // TAvro, not TValue - SchemaName is this schema's identity on the wire (and the cold/hot-tier
        // blob path segment), independent of which internal DTO it happens to be mapped into.
        public string SchemaName { get; } = typeof(TAvro).Name;

        public string? ServiceBusQueueName { get; } = serviceBusQueueName;

        public object Deserialize(byte[]? data, SerializationContext context) =>
            map(deserializer.Deserialize(data, data is null, context))!;

        public string SerializeToJson(object value) => serializeToJson((TValue)value);

        public (string SessionId, string MessageId) GetServiceBusRouting(object value, string? key) => getServiceBusRouting((TValue)value, key);

        public Task<bool> ValidateAsync(object value, CancellationToken cancellationToken) => validateAsync((TValue)value, cancellationToken);

        public string? GetOrderArchiveKey(object value) => getOrderArchiveKey((TValue)value);
    }

    /// <summary>
    /// Builds a type-erased schema entry for one event type - the only place the concrete
    /// <typeparamref name="TValue"/> needs naming when registering a schema. A derived consumer calls
    /// this once per schema it supports (in its own constructor) and passes the resulting dictionary,
    /// keyed by <see cref="KafkaHeaderNames.Type"/> header value (or <see cref="DefaultEventType"/> for
    /// "any/no specific type"), to the base constructor.
    /// </summary>
    /// <typeparam name="TValue">The deserialized shape for this one schema/event type.</typeparam>
    /// <param name="deserializer">Deserializer for this schema (JSON or Avro).</param>
    /// <param name="serializeToJson">Serializes one deserialized value of this schema to JSON - used for the cold-tier audit log and, on a validation/publish failure, the hot-tier dead-letter blob.</param>
    /// <param name="getServiceBusRouting">Routes one deserialized value of this schema onto Service Bus - returns the session id (groups this event with others for the same aggregate) and a deterministic message id (drives the downstream dedupe check).</param>
    /// <param name="validateAsync">
    /// Business-rule validation for one deserialized value of this schema, run after the dedup check
    /// and before mapping/publishing to Service Bus. Omit for "always valid." Two distinct ways to
    /// stop a message short of Service Bus, not to be confused with each other:
    /// <list type="bullet">
    /// <item><b>Throw</b> for a hard failure (malformed/invalid data) - logged at <c>LogLevel.Critical</c>,
    /// written to hot storage as JSON, offset committed forward. An out-of-band watcher can reprocess
    /// it once the underlying issue is fixed.</item>
    /// <item><b>Return <see langword="false"/></b> for a message that is valid but should deliberately
    /// not be relayed (e.g. an event type/version this consumer intentionally doesn't forward) -
    /// logged at <c>LogLevel.Information</c>, offset committed forward, but <b>not</b> written to hot
    /// storage, since nothing failed.</item>
    /// </list>
    /// </param>
    /// <param name="serviceBusQueueName">
    /// Overrides <see cref="ConsumerOptions.ServiceBusQueueName"/> for this schema only - omit (or pass
    /// <see langword="null"/>) for the common case of every schema on this consumer relaying onto the
    /// same queue. Set this when a consumer registers more than one schema and they need to land on
    /// different Service Bus queues; the base class opens one <see cref="Azure.Messaging.ServiceBus.ServiceBusSender"/>
    /// per distinct queue name actually used, not one per schema.
    /// </param>
    /// <param name="getOrderArchiveKey">
    /// Computes the <c>OrderArchive</c> category key for one deserialized value of this schema, or
    /// omit (default <see langword="null"/>) if this schema never archives to Cosmos. When the
    /// computed key is non-null/non-empty for a given message, <see cref="ProcessMessageAsync"/>
    /// upserts an <c>OrderArchive</c> record before publishing that message to Service Bus.
    /// </param>
    protected static ISchemaHandler CreateSchemaHandler<TValue>(
        IDeserializer<TValue> deserializer,
        Func<TValue, string> serializeToJson,
        Func<TValue, string?, (string SessionId, string MessageId)> getServiceBusRouting,
        Func<TValue, CancellationToken, Task<bool>>? validateAsync = null,
        string? serviceBusQueueName = null,
        Func<TValue, string?>? getOrderArchiveKey = null) =>
        new SchemaHandler<TValue>(deserializer, serializeToJson, getServiceBusRouting, validateAsync, serviceBusQueueName, getOrderArchiveKey);

    /// <summary>
    /// Builds a type-erased schema entry for one Avro event type that gets mapped into an internal
    /// <typeparamref name="TValue"/> DTO before anything downstream (JSON audit, Service Bus routing,
    /// validation) touches it - decouples this consumer's own wire contract from the Avro-generated
    /// <typeparamref name="TAvro"/> SpecificRecord (see e.g. <c>InventoryStateChangedEvent</c>'s own
    /// remarks for why). An <b>instance</b> method, unlike <see cref="CreateSchemaHandler{TValue}"/> -
    /// it needs this consumer's own <see cref="Options"/> and <see cref="ISpecificRecordDeserializerFactory"/>,
    /// neither of which exists yet inside a <c>base(...)</c> argument list, so call it from the derived
    /// class's constructor <i>body</i> (after the <c>base(...)</c> call), then hand the resulting
    /// dictionary to <see cref="RegisterSchemaHandlers"/> - not from the dictionary literal passed to
    /// <c>base(...)</c> itself. The Schema Registry client behind it is built once per consumer instance,
    /// on the first call, and reused for every subsequent schema on the same consumer (and disposed
    /// automatically by this base class - see <see cref="Dispose()"/>), so a consumer with two Avro
    /// schemas does not need to manage sharing it itself.
    /// </summary>
    /// <typeparam name="TAvro">The Avro-generated <c>ISpecificRecord</c> type this schema deserializes to.</typeparam>
    /// <typeparam name="TValue">The internal DTO <typeparamref name="TAvro"/> is mapped into.</typeparam>
    /// <param name="map">Maps one deserialized <typeparamref name="TAvro"/> value into this consumer's own <typeparamref name="TValue"/> - everything past this point (JSON audit, routing, validation) sees only <typeparamref name="TValue"/>.</param>
    /// <param name="getServiceBusRouting">
    /// Routes one mapped <typeparamref name="TValue"/> onto Service Bus - see
    /// <see cref="ISchemaHandler.GetServiceBusRouting"/>. Omit for the common case of routing by the
    /// Kafka record key alone (<see cref="RouteByEventKey{TValue}"/>); pass an explicit delegate when a
    /// schema needs to derive its session/message id from fields inside the value instead (e.g.
    /// <c>BulkInventoryImportConsumerHostedService</c>'s own <c>EventId</c>-based routing).
    /// </param>
    /// <param name="serviceBusQueueName">Per-schema Service Bus queue override - see <see cref="CreateSchemaHandler{TValue}"/>'s remarks.</param>
    /// <param name="validateAsync">Business-rule validation for one mapped <typeparamref name="TValue"/> - see <see cref="CreateSchemaHandler{TValue}"/>'s remarks for the throw-vs-return-false distinction.</param>
    /// <param name="getOrderArchiveKey">Computes the <c>OrderArchive</c> category key for one mapped <typeparamref name="TValue"/> - see <see cref="CreateSchemaHandler{TValue}"/>'s remarks.</param>
    protected ISchemaHandler CreateSchemaHandler<TAvro, TValue>(
        Func<TAvro, TValue> map,
        Func<TValue, string?, (string SessionId, string MessageId)>? getServiceBusRouting = null,
        string? serviceBusQueueName = null,
        Func<TValue, CancellationToken, Task<bool>>? validateAsync = null,
        Func<TValue, string?>? getOrderArchiveKey = null)
        where TAvro : Avro.Specific.ISpecificRecord
    {
        var factory = specificRecordDeserializerFactory
            ?? throw new InvalidOperationException(
                $"{ConsumerName} has no {nameof(ISpecificRecordDeserializerFactory)} configured - required for Avro schema '{typeof(TAvro).Name}'.");

        var deserializer = schemaRegistryClient is null
            ? BuildFirstAvroDeserializer<TAvro>(factory)
            : factory.Create<TAvro>(schemaRegistryClient);

        return new MappedSchemaHandler<TAvro, TValue>(
            deserializer,
            map,
            value => JsonSerializer.Serialize(value, RelayJsonOptions),
            getServiceBusRouting ?? RouteByEventKey,
            validateAsync,
            serviceBusQueueName,
            getOrderArchiveKey);
    }

    /// <summary>First Avro schema registered on this consumer instance - builds (and caches, via <see cref="schemaRegistryClient"/>) the Schema Registry client every later schema on the same consumer reuses.</summary>
    private IDeserializer<TAvro> BuildFirstAvroDeserializer<TAvro>(ISpecificRecordDeserializerFactory factory)
        where TAvro : Avro.Specific.ISpecificRecord
    {
        var deserializer = factory.Create<TAvro>(
            Options.SchemaRegistryUrl
                ?? throw new InvalidOperationException(
                    $"Missing SchemaRegistryUrl for '{ConsumerName}' - configure it at this consumer's own level or the Kafka-level fallback."),
            Options.SchemaRegistryApiKey,
            Options.SchemaRegistryApiSecret,
            out var client);

        schemaRegistryClient = client;
        return deserializer;
    }

    /// <summary>
    /// Default Service Bus routing for <see cref="CreateSchemaHandler{TAvro,TValue}"/>: the Kafka
    /// record key (<c>ConsumeResult.Message.Key</c>) alone, used as both the SessionId and MessageId -
    /// the common case for an Avro schema with no compound aggregate key of its own (e.g.
    /// <c>InventoryStateChanged</c>/<c>InventoryAdjusted</c>, which carry one <c>location</c> but an
    /// array of line items, so routing is keyed on the producer's own Kafka key rather than a field
    /// inside the value).
    /// </summary>
    private static (string SessionId, string MessageId) RouteByEventKey<TValue>(TValue _, string? key)
    {
        var eventKey = key
            ?? throw new InvalidOperationException("Missing Kafka record key for this event - required to route onto Service Bus.");

        return (eventKey, eventKey);
    }

    private readonly IConsumer<string, byte[]> consumer;
    private readonly ISpecificRecordDeserializerFactory? specificRecordDeserializerFactory;
    private ISchemaRegistryClient? schemaRegistryClient;
    private IReadOnlyDictionary<string, ISchemaHandler> schemaHandlers = new Dictionary<string, ISchemaHandler>();
    private readonly ServiceBusClient serviceBusClient;

    // Keyed by queue name, not by schema/event type - two schemas that share a queue (the common
    // case, ISchemaHandler.ServiceBusQueueName left null) reuse the same sender rather than each
    // opening their own. Built lazily via GetServiceBusSender, not eagerly for every schema up front.
    private readonly ConcurrentDictionary<string, ServiceBusSender> serviceBusSenders = new();
    private readonly ResiliencePipelineProvider<string> pipelineProvider;
    private readonly IFileStore hotFileStore;
    private readonly IFileStore coldFileStore;
    private readonly IDeduplicationService deduplicationService;
    private readonly IServiceScopeFactory scopeFactory;
    private readonly ApplicationOptions applicationOptions;

    // Keyed by the same event-type strings as schemaHandlers (built by RegisterSchemaHandlers, from
    // that same dictionary's keys) - one ConsumerHealthState per registered schema/event type, not one
    // per consumer, so ConsumerHealthCheck can report per event type (see GetHealthState). Never
    // supplied through the constructor: unlike every other dependency here, a schema's set of event
    // types isn't known until the derived class's constructor body calls RegisterSchemaHandlers, so
    // there is nothing for a caller to inject yet at that point.
    private IReadOnlyDictionary<string, ConsumerHealthState> healthStates = new Dictionary<string, ConsumerHealthState>();
    private readonly ILogger logger;
    private readonly PartitionOffsetCommitTracker offsetTracker;
    private readonly BlobStorageOptions blobStorageOptions;

    // Guards DisposeKafkaResources against running twice - the DI-driven shutdown path (ServiceProvider
    // prefers DisposeAsync over Dispose when a singleton implements both) only ever triggers one of
    // Dispose()/DisposeAsync(), so this is defensive hardening for a caller that disposes this instance
    // directly more than once, not something the app's own lifecycle hits.
    private bool disposed;

    /// <summary>Settings this consumer was configured with.</summary>
    protected ConsumerOptions Options { get; }

    /// <summary>
    /// Display name used in log messages and the cold/hot-tier blob path - distinguishes this
    /// consumer's log lines and audit records from other consumers sharing the same pod/process.
    /// Public (not just <c>protected</c>) so it doubles as <see cref="IServiceBusSenderCacheSource.ConsumerName"/>
    /// for the Service Bus sender cache admin endpoint.
    /// </summary>
    public string ConsumerName { get; }

    /// <summary>Builds the Kafka consumer and the Service Bus sender it relays onto.</summary>
    /// <param name="options">Topic, consumer group, enabled flag, worker/channel sizing, and Service Bus queue settings for this consumer.</param>
    /// <param name="infrastructure">
    /// The six dependencies every consumer needs and which never vary between them (Service Bus
    /// client, Polly pipeline provider, hot/cold file stores, Blob Storage options, dedup service) -
    /// see <see cref="ConsumerRelayInfrastructure"/>'s own doc comment for why this is a single
    /// facade parameter instead of six.
    /// </param>
    /// <param name="logger">Logger for consume/relay/failure events.</param>
    /// <param name="specificRecordDeserializerFactory">
    /// Builds the Avro deserializers <see cref="CreateSchemaHandler{TAvro,TValue}"/> needs - only an
    /// Avro-contract consumer passes this; a JSON-contract consumer (e.g. <c>KafkaConsumerHostedService</c>)
    /// leaves it <see langword="null"/> and never calls that method.
    /// </param>
    /// <remarks>
    /// Does not itself register any schema - <see cref="ConsumerName"/> derives from the concrete
    /// derived type's name (<see cref="DeriveConsumerName"/>), but the derived class must still call
    /// <see cref="RegisterSchemaHandlers"/> from its own constructor <i>body</i> (after this base
    /// constructor returns) before the host starts calling <see cref="ExecuteAsync"/> - schema handlers
    /// built via <see cref="CreateSchemaHandler{TAvro,TValue}"/> need this instance's own <see cref="Options"/>
    /// and <paramref name="specificRecordDeserializerFactory"/>, neither of which is available yet inside
    /// this constructor's own argument list. This is also why <see cref="ConsumerHealthState"/> is not a
    /// constructor parameter here - see <see cref="RegisterSchemaHandlers"/> and <see cref="GetHealthState"/>.
    /// </remarks>
    protected ConsumerHostedService(
        ConsumerOptions options,
        ConsumerRelayInfrastructure infrastructure,
        ILogger logger,
        ISpecificRecordDeserializerFactory? specificRecordDeserializerFactory = null)
    {
        Options = options;
        ConsumerName = DeriveConsumerName(GetType());
        pipelineProvider = infrastructure.PipelineProvider;
        hotFileStore = infrastructure.HotFileStore;
        coldFileStore = infrastructure.ColdFileStore;
        blobStorageOptions = infrastructure.BlobStorageOptions.Value;
        deduplicationService = infrastructure.DeduplicationService;
        scopeFactory = infrastructure.ScopeFactory;
        applicationOptions = infrastructure.ApplicationOptions;
        this.logger = logger;
        this.specificRecordDeserializerFactory = specificRecordDeserializerFactory;

        var config = new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers
                ?? throw new InvalidOperationException(
                    $"Missing BootstrapServers for '{ConsumerName}' - configure it at this consumer's own level or the Kafka-level fallback."),
            GroupId = options.ConsumerGroup,
            // Defensive fallback, not just belt-and-braces: false/Earliest is what this service's
            // manual-commit architecture requires (integration-resiliency.instructions.md §1) -
            // Confluent.Kafka's own defaults if these were left null are true/Latest, the opposite.
            // MessagingServiceCollectionExtensions.AddMessaging's PostConfigure already pins the
            // Kafka-level fallback to these same values, so this only matters for an options POCO
            // built directly in a unit test, bypassing that PostConfigure pipeline.
            EnableAutoCommit = options.EnableAutoCommit ?? false,
            AutoOffsetReset = options.AutoOffsetReset ?? AutoOffsetReset.Earliest,
            // Left null (Confluent.Kafka's own Plaintext/no-SASL default) when unconfigured - correct
            // for the local emulator, never for a real cluster. SecurityProtocol/SaslMechanism being
            // set inconsistently with each other (e.g. a mechanism without a SASL protocol) is
            // rejected by Confluent.Kafka itself when the consumer below is built, not re-validated here.
            SecurityProtocol = options.Protocol,
            SaslMechanism = options.AuthenticationMode,
            SaslUsername = options.Username,
            SaslPassword = options.Password,
        };

        // Raw bytes, not any one schema's type - Confluent.Kafka's built-in default byte[]
        // deserializer never throws, so a bad payload is only ever discovered later, in
        // ProcessMessageAsync, once the right schema handler has been resolved. See the class-level
        // remarks for why this moved out of the consumer builder.
        consumer = new ConsumerBuilder<string, byte[]>(config).Build();

        serviceBusClient = infrastructure.ServiceBusClient;
        offsetTracker = new PartitionOffsetCommitTracker(offsets => consumer.Commit(offsets));
    }

    /// <summary>
    /// Registers this consumer's schema handlers, built via <see cref="CreateSchemaHandler{TValue}"/>/
    /// <see cref="CreateSchemaHandler{TAvro,TValue}"/> and keyed by the Kafka
    /// <see cref="KafkaHeaderNames.Type"/> header value each corresponds to (or
    /// <see cref="DefaultEventType"/> for a schema that applies regardless of that header's value) -
    /// see <see cref="ProcessMessageAsync"/> for the lookup order. Call this once, from the derived
    /// class's own constructor body immediately after the <c>base(...)</c> call - it cannot be supplied
    /// as a <c>base(...)</c> argument itself because <see cref="CreateSchemaHandler{TAvro,TValue}"/>
    /// needs this instance's own <see cref="Options"/>/deserializer factory, which don't exist yet at
    /// that point in construction. Also builds one <see cref="ConsumerHealthState"/> per key in
    /// <paramref name="handlers"/> - see <see cref="GetHealthState"/> and <see cref="RunPollLoopAsync"/>.
    /// </summary>
    protected void RegisterSchemaHandlers(IReadOnlyDictionary<string, ISchemaHandler> handlers)
    {
        schemaHandlers = handlers;
        healthStates = handlers.Keys.ToDictionary(eventType => eventType, _ => new ConsumerHealthState());
    }

    /// <summary>
    /// The <see cref="ConsumerHealthState"/> for one event type this consumer registered via
    /// <see cref="RegisterSchemaHandlers"/> - resolved by <c>MessagingServiceCollectionExtensions</c>
    /// when it builds this consumer's <see cref="ConsumerHealthCheck"/> instances, one per event type,
    /// rather than this state being handed to the consumer through its own constructor (nothing could
    /// supply it there - see <see cref="RegisterSchemaHandlers"/>'s remarks).
    /// </summary>
    /// <param name="eventType">One of the keys this consumer passed to <see cref="RegisterSchemaHandlers"/>.</param>
    internal ConsumerHealthState GetHealthState(string eventType) =>
        healthStates.TryGetValue(eventType, out var state)
            ? state
            : throw new InvalidOperationException(
                $"{ConsumerName} has no schema handler registered for event type '{eventType}' - check RegisterSchemaHandlers matches the health checks configured for this consumer.");

    /// <summary>Derives <see cref="ConsumerName"/> from the concrete derived type's name, e.g. <c>InventoryStateChangedConsumerHostedService</c> becomes <c>InventoryStateChanged</c>.</summary>
    private static string DeriveConsumerName(Type type)
    {
        return type.Name;
        //const string Suffix = "ConsumerHostedService";
        //return type.Name.EndsWith(Suffix, StringComparison.Ordinal) ? type.Name[..^Suffix.Length] : type.Name;
    }

    /// <summary>
    /// Resolves (creating and caching on first use) the <see cref="ServiceBusSender"/> for
    /// <paramref name="queueName"/> - one sender per distinct queue name actually used across every
    /// registered schema, not one per schema, so two schemas sharing a queue share a sender too.
    /// </summary>
    private ServiceBusSender GetServiceBusSender(string queueName) =>
        serviceBusSenders.GetOrAdd(queueName, serviceBusClient.CreateSender);

    /// <summary>
    /// Polls the subscribed topic in a loop until cancellation, handing each message to the worker
    /// pool for relaying - a no-op if <see cref="ConsumerOptions.Enabled"/> is <see langword="false"/>.
    /// </summary>
    /// <param name="stoppingToken">Signaled when the host is shutting down.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (Options.Enabled != true)
        {
            logger.LogInformation("{ConsumerName} is disabled via configuration ('Enabled: false') - not starting.", ConsumerName);
            return;
        }

        consumer.Subscribe(Options.Topic);

        // Both fall back to a hardcoded default here, not just to the Kafka-level config value -
        // see ConsumerOptions' class remarks on why call sites don't assert non-null even though the
        // Kafka-level PostConfigure guarantees it in the normal DI-resolved path.
        var channel = Channel.CreateBounded<ConsumeResult<string, byte[]>>(new BoundedChannelOptions(Options.ChannelCapacity ?? 1_000)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true,
        });

        var workers = Enumerable.Range(0, Options.WorkerCount ?? 1)
            .Select(_ => RunWorkerAsync(channel.Reader))
            .ToArray();

        try
        {
            await RunPollLoopAsync(channel.Writer, stoppingToken);
        }
        finally
        {
            channel.Writer.TryComplete();

            // Awaited even after the poll loop stops, so every worker's ReadAllAsync loop above
            // actually observes the completed channel and exits - if we returned immediately instead,
            // those tasks would be silently abandoned still holding a channel reader.
            await Task.WhenAll(workers);
        }
    }

    /// <summary>Single-threaded poll loop - reads raw messages from Kafka and hands each result to the worker pool via the channel, applying backpressure when it's full.</summary>
    private async Task RunPollLoopAsync(ChannelWriter<ConsumeResult<string, byte[]>> writer, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, byte[]>? polled;

            try
            {
                polled = consumer.Consume(Options.PollTimeout);
            }
            catch (ConsumeException ex)
            {
                var consumerRecord = ex.ConsumerRecord;

                if (consumerRecord is null)
                {
                    logger.LogError(ex, "{ConsumerName}: Kafka consume error on topic {Topic}.", ConsumerName, Options.Topic);
                    continue;
                }

                // A genuine Kafka-level consume failure (e.g. the key failed to deserialize as
                // UTF-8) - the message value's own deserialization can no longer fail here since the
                // consumer only ever reads raw bytes now (see the constructor remarks); that failure
                // mode is handled in ProcessMessageAsync instead. Still routed through the offset
                // tracker as a normal completion - marking it done, not committing directly - so it
                // folds correctly into whatever the partition's low-water mark already is
                // (integration-resiliency.instructions.md §1).
                var topicPartitionOffset = consumerRecord.TopicPartitionOffset;
                var rawValue = consumerRecord.Message?.Value is { } bytes ? Encoding.UTF8.GetString(bytes) : "<null>";

                logger.LogCritical(ex,
                    "{ConsumerName}: Kafka-level consume error at {Topic}:{Partition}:{Offset} - message skipped. Raw payload: {RawPayload}",
                    ConsumerName, topicPartitionOffset.Topic, topicPartitionOffset.Partition.Value, topicPartitionOffset.Offset.Value, rawValue);

                offsetTracker.EstablishBaseline(topicPartitionOffset.TopicPartition, topicPartitionOffset.Offset.Value);
                offsetTracker.Complete(topicPartitionOffset);

                continue;
            }

            // No message within the poll timeout is not a failure - an idle topic keeps the
            // consumer healthy (integration-resiliency.instructions.md §8). Every registered event
            // type's state is touched here, not just whichever type (if any) this poll happened to
            // return - the poll loop has no way to know which type would have arrived, and an idle
            // topic must not make any of this consumer's event types look stale.
            var pollUtc = DateTimeOffset.UtcNow;

            foreach (var state in healthStates.Values)
            {
                state.LastSuccessfulPollUtc = pollUtc;
            }

            if (polled is not { Message: not null } result)
            {
                continue;
            }

            // Established here, in the single-threaded poll loop, before the message reaches any
            // worker - see PartitionOffsetCommitTracker.EstablishBaseline for why that ordering is
            // what makes it correct.
            offsetTracker.EstablishBaseline(result.TopicPartition, result.Offset.Value);

            await writer.WriteAsync(result, stoppingToken);
        }
    }

    /// <summary>
    /// One worker draining the channel - runs the full per-message flow until the channel completes.
    /// No longer stops the consumer on a per-message failure (see the class-level remarks); the only
    /// way this loop now ends is the channel completing on shutdown.
    /// </summary>
    private async Task RunWorkerAsync(ChannelReader<ConsumeResult<string, byte[]>> reader)
    {
        // Intentionally CancellationToken.None, not the poll loop's stopping token: once a message has
        // been dispatched into the channel it should still be relayed and committed on a graceful
        // shutdown, not abandoned mid-flight only to be redelivered on restart.
        await foreach (var result in reader.ReadAllAsync(CancellationToken.None))
        {
            await ProcessMessageAsync(result, CancellationToken.None);
        }
    }

    /// <summary>
    /// Runs the full per-message flow: resolve the schema handler for this message's event type, log
    /// metadata, deserialize, write the cold-tier audit log, check deduplication, validate, map and
    /// publish to Service Bus, then report completion to the offset tracker. See the class-level
    /// remarks for the exact step order and failure handling.
    /// </summary>
    /// <param name="result">The consumed raw Kafka message, with its topic/partition/offset metadata.</param>
    /// <param name="cancellationToken">Token to cancel the publish.</param>
    private async Task ProcessMessageAsync(ConsumeResult<string, byte[]> result, CancellationToken cancellationToken)
    {
        var totalStopwatch = Stopwatch.StartNew();

        var headers = result.Message.Headers;
        var rawCorrelationId = TryGetHeader(headers, KafkaHeaderNames.CorrelationId);
        var correlationId = rawCorrelationId ?? $"-{Guid.NewGuid().ToString()}";
        var deduplicationId = TryGetHeader(headers, KafkaHeaderNames.DeduplicationId) ?? string.Empty;
        var eventType = TryGetHeader(headers, KafkaHeaderNames.Type) ?? string.Empty;

        // Falls back to this service's own configured identity (ApplicationOptions.ApplicationId)
        // when the producer didn't set an App-Id header, so a relayed event never carries a blank
        // AppId downstream - see ApplicationOptions' own remarks.
        var rawAppId = TryGetHeader(headers, KafkaHeaderNames.AppId);
        var appId = string.IsNullOrEmpty(rawAppId) ? applicationOptions.AppId : rawAppId;

        var eventKey = TryGetHeader(headers, KafkaHeaderNames.EventKey) ?? string.Empty;

        // Scoped for the lifetime of this one message, since the hosted service itself is a
        // singleton but ICorrelationContext is scoped - disposed automatically at every return
        // point below via the `using` declaration, same pattern as ServiceBusConsumerHostedService.
        using var scope = scopeFactory.CreateScope();
        var correlationContext = scope.ServiceProvider.GetRequiredService<ICorrelationContext>();
        correlationContext.Set(correlationId, appId, [eventType, ConsumerName]);

        // Pushed into Serilog's ambient LogContext (integration-resiliency.instructions.md §7),
        // same as CorrelationIdMiddleware does at the HTTP boundary - every log line for the rest of
        // this message's processing carries these without needing to be passed as its own template
        // argument. Disposed (popped) at method return via the `using` declarations, same as `scope`.
        using var correlationIdLogContext = LogContext.PushProperty("CorrelationId", correlationId);
        using var appIdLogContext = LogContext.PushProperty("AppId", appId);
        using var eventTypeLogContext = LogContext.PushProperty("EventType", eventType);
        using var eventTypesLogContext = LogContext.PushProperty("Types", string.Join(", ", correlationContext.Types));

        logger.LogInformation(
            "{ConsumerName}: consumed message from {Topic}:{Partition}:{Offset}. CorrelationId: {CorrelationId}, EventType: {EventType}, AppId: {AppId}",
            ConsumerName, result.Topic, result.Partition.Value, result.Offset.Value, correlationId, eventType, appId);

        // Config-driven ignore-list, checked against the raw header (an empty/missing header always
        // passes, regardless of configuration - see ConsumerOptions.IsCorrelationIdIgnored). Runs
        // before schema resolution/deserialize - it's a cross-cutting filter, not a per-schema concern.
        if (Options.IsCorrelationIdIgnored(rawCorrelationId))
        {
            logger.LogInformation(
                "{ConsumerName}: ignoring message at {Topic}:{Partition}:{Offset} - CorrelationId {CorrelationId} matched a configured ignore prefix/suffix. EventType: {EventType}, AppId: {AppId}",
                ConsumerName, result.Topic, result.Partition.Value, result.Offset.Value, correlationId, eventType, appId);

            offsetTracker.Complete(result.TopicPartitionOffset);
            return;
        }

        // Exact Type header value first, DefaultEventType as the fallback for a consumer that
        // registers one schema regardless of that header - see the class-level remarks.
        if (!schemaHandlers.TryGetValue(eventType, out var schemaHandler)
            && !schemaHandlers.TryGetValue(DefaultEventType, out schemaHandler))
        {
            logger.LogCritical(
                "{ConsumerName}: no registered schema for Type header '{EventType}' at {Topic}:{Partition}:{Offset} - writing raw bytes to hot storage and committing. CorrelationId: {CorrelationId}, AppId: {AppId}",
                ConsumerName, eventType, result.Topic, result.Partition.Value, result.Offset.Value, correlationId, appId);

            await WriteBlobAsync(
                hotFileStore, blobStorageOptions.ConsumerDeadLetterContainerName,
                string.IsNullOrEmpty(eventType) ? "UnknownEventType" : eventType, correlationId, "bin",
                new MemoryStream(result.Message.Value ?? []), cancellationToken);

            offsetTracker.Complete(result.TopicPartitionOffset);
            return;
        }

        object value;
        TimeSpan deserializeDuration;

        try
        {
            var deserializeStopwatch = Stopwatch.StartNew();
            var rawValue = result.Message.Value;
            value = schemaHandler.Deserialize(rawValue, new SerializationContext(MessageComponentType.Value, result.Topic, headers));
            deserializeDuration = deserializeStopwatch.Elapsed;
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex,
                "{ConsumerName}: Deserialize failed for message at {Topic}:{Partition}:{Offset} - writing raw bytes to hot storage and committing. CorrelationId: {CorrelationId}, EventType: {EventType}, AppId: {AppId}",
                ConsumerName, result.Topic, result.Partition.Value, result.Offset.Value, correlationId, eventType, appId);

            await WriteBlobAsync(
                hotFileStore, blobStorageOptions.ConsumerDeadLetterContainerName, schemaHandler.SchemaName, correlationId, "bin",
                new MemoryStream(result.Message.Value ?? []), cancellationToken);

            offsetTracker.Complete(result.TopicPartitionOffset);
            return;
        }

        string json;

        try
        {
            json = schemaHandler.SerializeToJson(value);
        }
        catch (Exception ex)
        {
            // No JSON to fall back to here (that's the very thing that just failed) - raw bytes,
            // same as a deserialize failure, so this message isn't lost outright.
            logger.LogCritical(ex,
                "{ConsumerName}: Serialize failed for message at {Topic}:{Partition}:{Offset} - writing raw bytes to hot storage and committing. CorrelationId: {CorrelationId}, EventType: {EventType}, AppId: {AppId}",
                ConsumerName, result.Topic, result.Partition.Value, result.Offset.Value, correlationId, eventType, appId);

            await WriteBlobAsync(
                hotFileStore, blobStorageOptions.ConsumerDeadLetterContainerName, schemaHandler.SchemaName, correlationId, "bin",
                new MemoryStream(result.Message.Value ?? []), cancellationToken);

            offsetTracker.Complete(result.TopicPartitionOffset);
            return;
        }

        await WriteBlobAsync(
            coldFileStore, blobStorageOptions.RequestAuditContainerName, schemaHandler.SchemaName, correlationId, "json",
            new MemoryStream(Encoding.UTF8.GetBytes(json)), cancellationToken);

        bool isDuplicate;

        if (Options.DeduplicationCheckEnabled == false)
        {
            // Explicitly turned off for this topic (event level or inherited from Kafka level) -
            // not the same as the fail-open path below, which still attempts the check. The
            // downstream Service Bus consumer's own idempotency check (§2) is still the correctness
            // backstop either way; this flag only trades away the latency/cost optimization.
            isDuplicate = false;
        }
        else
        {
            try
            {
                isDuplicate = await deduplicationService.IsDuplicateAsync($"IIS_WMS_{eventType}", deduplicationId, correlationId, cancellationToken);
            }
            catch (Exception ex)
            {
                // Fail open, not closed: the dedup check is a best-effort layer backed by an external
                // dependency (Nexus) that can itself be unavailable. Treating that as fatal would mean a
                // Nexus outage dead-letters the entire topic; treating it as "duplicate" would silently
                // drop every message while Nexus is down. Proceeding as "not a duplicate" instead relies
                // on the downstream Service Bus consumer's own idempotency check (§2) as the backstop -
                // this consumer's dedup check is a latency/cost optimization on top of that, not the only
                // line of defense against a redelivered message being applied twice.
                logger.LogWarning(ex,
                    "{ConsumerName}: deduplication check failed for message at {Topic}:{Partition}:{Offset} - proceeding as not a duplicate. CorrelationId: {CorrelationId}, EventType: {EventType}, AppId: {AppId}",
                    ConsumerName, result.Topic, result.Partition.Value, result.Offset.Value, correlationId, eventType, appId);
                isDuplicate = false;
            }
        }

        if (isDuplicate)
        {
            logger.LogInformation(
                "{ConsumerName}: skipping {Topic}:{Partition}:{Offset} - duplicate. CorrelationId: {CorrelationId}, EventType: {EventType}, AppId: {AppId}",
                ConsumerName, result.Topic, result.Partition.Value, result.Offset.Value, correlationId, eventType, appId);

            offsetTracker.Complete(result.TopicPartitionOffset);
            return;
        }

        TimeSpan validationDuration;
        bool isValid;

        try
        {
            var validationStopwatch = Stopwatch.StartNew();
            isValid = await schemaHandler.ValidateAsync(value, cancellationToken);
            validationDuration = validationStopwatch.Elapsed;
        }
        catch (Exception ex)
        {
            await HandleNonFatalFailureAsync("Validation", ex, json, schemaHandler.SchemaName, correlationId, eventType, appId, result, cancellationToken);
            return;
        }

        if (!isValid)
        {
            // Valid data, deliberately not relayed - not the same as the hard-failure path above.
            // No hot-tier write: nothing failed, this consumer just chose not to forward it.
            logger.LogInformation(
                "{ConsumerName}: ValidateAsync returned false for message at {Topic}:{Partition}:{Offset} - skipping Service Bus publish. CorrelationId: {CorrelationId}, EventType: {EventType}, AppId: {AppId}",
                ConsumerName, result.Topic, result.Partition.Value, result.Offset.Value, correlationId, eventType, appId);

            offsetTracker.Complete(result.TopicPartitionOffset);
            return;
        }

        // Opt-in per schema (CreateSchemaHandler's getOrderArchiveKey) - a schema that doesn't supply
        // one never computes this and every message skips straight to Service Bus, same as before this
        // archive step existed.
        var orderArchiveKey = schemaHandler.GetOrderArchiveKey(value);

        if (!string.IsNullOrEmpty(orderArchiveKey))
        {
            try
            {
                var orderArchive = OrderArchive.Create(
                    $"{schemaHandler.SchemaName}_{correlationId}", orderArchiveKey, json, DateTime.UtcNow);

                var orderArchiveRepository = scope.ServiceProvider.GetRequiredService<IOrderArchiveRepository>();
                await orderArchiveRepository.UpsertAsync(orderArchive, cancellationToken);
            }
            catch (Exception ex)
            {
                await HandleNonFatalFailureAsync("OrderArchiveUpsert", ex, json, schemaHandler.SchemaName, correlationId, eventType, appId, result, cancellationToken);
                return;
            }
        }

        try
        {
            var (sessionId, messageId) = schemaHandler.GetServiceBusRouting(value, result.Message.Key);

            // Wraps the schema's own JSON with the full CorrelationContext this consumer built above -
            // see ServiceBusRelayEnvelope's own remarks for why this travels in the body in addition to
            // (not instead of) the ApplicationProperties property below.
            using var payloadDocument = JsonDocument.Parse(json);
            var envelope = new ServiceBusRelayEnvelope
            {
                CorrelationId = correlationId,
                AppId = appId,
                Types = correlationContext.Types,
                Payload = payloadDocument.RootElement.Clone(),
            };

            var message = new ServiceBusMessage(JsonSerializer.Serialize(envelope))
            {
                // Deterministic id from the event payload - this is what makes the Service Bus
                // consumer's dedupe check on redelivery actually work.
                MessageId = messageId,
                SessionId = sessionId,
            };
            message.ApplicationProperties["CorrelationId"] = correlationId;

            var serviceBusSender = GetServiceBusSender(schemaHandler.ServiceBusQueueName ?? Options.ServiceBusQueueName);
            var pipeline = pipelineProvider.GetPipeline(ResiliencePipelines.ServiceBusPublish);

            await pipeline.ExecuteAsync(
                async ct => await serviceBusSender.SendMessageAsync(message, ct), cancellationToken);

            // Reports completion to the tracker rather than committing this offset directly - with
            // multiple workers in flight, this message's offset is not necessarily the next one the
            // partition is waiting on (integration-resiliency.instructions.md §6).
            offsetTracker.Complete(result.TopicPartitionOffset);

            logger.LogInformation(
                "{ConsumerName}: relayed {MessageId} for session {SessionId} from {Topic}:{Partition}:{Offset} to Service Bus. " +
                "CorrelationId: {CorrelationId}, EventType: {EventType}, AppId: {AppId}, DeserializeDurationMs: {DeserializeDurationMs}, ValidationDurationMs: {ValidationDurationMs}, TotalDurationMs: {TotalDurationMs}",
                ConsumerName, messageId, sessionId, result.Topic, result.Partition.Value, result.Offset.Value, correlationId, eventType, appId,
                deserializeDuration.TotalMilliseconds, validationDuration.TotalMilliseconds, totalStopwatch.Elapsed.TotalMilliseconds);
        }
        catch (Exception ex)
        {
            await HandleNonFatalFailureAsync("ServiceBusPublish", ex, json, schemaHandler.SchemaName, correlationId, eventType, appId, result, cancellationToken);
        }
    }

    /// <summary>
    /// Shared handling for a non-fatal per-message failure past the deserialize step (validation or
    /// Service Bus publish, where a deserialized value already exists): logs at
    /// <see cref="LogLevel.Critical"/> tagged with which stage failed, writes the value's JSON to hot
    /// storage, and commits the offset forward - see the class-level remarks for why this never blocks
    /// or stops the consumer, even for a Service Bus publish failure.
    /// </summary>
    private async Task HandleNonFatalFailureAsync(
        string stage, Exception ex, string json, string schemaName, string correlationId, string eventType, string appId, ConsumeResult<string, byte[]> result, CancellationToken cancellationToken)
    {
        logger.LogCritical(ex,
            "{ConsumerName}: {Stage} failed for message at {Topic}:{Partition}:{Offset} - writing to hot storage and committing. CorrelationId: {CorrelationId}, EventType: {EventType}, AppId: {AppId}",
            ConsumerName, stage, result.Topic, result.Partition.Value, result.Offset.Value, correlationId, eventType, appId);

        await WriteBlobAsync(
            hotFileStore, blobStorageOptions.ConsumerDeadLetterContainerName, schemaName, correlationId, "json",
            new MemoryStream(Encoding.UTF8.GetBytes(json)), cancellationToken);

        offsetTracker.Complete(result.TopicPartitionOffset);
    }

    /// <summary>
    /// Writes <paramref name="content"/> to blob storage at
    /// <c>{correlationId}/{ConsumerName}/{schemaName}/{timestamp}_{guid}.{extension}</c> - disposes
    /// <paramref name="content"/> once written. Best-effort: a Blob Storage outage (after the upload
    /// pipeline's own retries are exhausted) is logged and swallowed rather than blocking the dedup
    /// check or the relay itself; the audit trail is a diagnostic aid, not the durability boundary
    /// (Service Bus is, per integration-resiliency.instructions.md §1).
    /// </summary>
    private async Task WriteBlobAsync(
        IFileStore fileStore, string containerName, string schemaName, string correlationId, string extension, Stream content, CancellationToken cancellationToken)
    {
        var blobName = $"{correlationId}/{schemaName}/{ConsumerName}/{DateTime.UtcNow:yyyyMMddHHmmssfff}_{Guid.NewGuid():N}.{extension}";

        await using (content)
        {
            try
            {
                await fileStore.UploadAsync(containerName, blobName, content, cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex,
                    "{ConsumerName}: failed to write audit blob {ContainerName}/{BlobName} - continuing without it. CorrelationId: {CorrelationId}",
                    ConsumerName, containerName, blobName, correlationId);
            }
        }
    }

    /// <summary>Reads a Kafka header's value as a UTF-8 string, if present.</summary>
    /// <param name="headers">Headers on the consumed Kafka message, or <see langword="null"/> if none were sent.</param>
    /// <param name="key">Header name - see <see cref="KafkaHeaderNames"/>.</param>
    /// <returns>The header value, or <see langword="null"/> if absent.</returns>
    private static string? TryGetHeader(Headers? headers, string key)
    {
        return headers is not null && headers.TryGetLastBytes(key, out var bytes)
            ? Encoding.UTF8.GetString(bytes)
            : null;
    }

    /// <summary>
    /// Closes the underlying Kafka consumer and disposes the Schema Registry client
    /// <see cref="CreateSchemaHandler{TAvro,TValue}"/> built (if any) - shared by both
    /// <see cref="Dispose()"/> and <see cref="DisposeAsync"/> so the cleanup isn't duplicated between
    /// them. Guarded by <see cref="disposed"/> so a caller that disposes this instance more than once
    /// (through either interface, or both) only closes the consumer once - the DI-driven shutdown path
    /// never does this itself (see <see cref="DisposeAsync"/>'s remarks), but nothing stops a test or
    /// other caller from calling <see cref="Dispose()"/>/<see cref="DisposeAsync"/> directly more than
    /// once.
    /// </summary>
    private void DisposeKafkaResources()
    {
        if (disposed)
        {
            return;
        }

        consumer.Close();
        consumer.Dispose();
        schemaRegistryClient?.Dispose();

        disposed = true;
    }

    /// <summary>
    /// Synchronous fallback for a caller that disposes this instance directly (e.g. a <c>using</c> in
    /// a unit test) rather than through the host's own shutdown. Cannot release the cached
    /// <see cref="ServiceBusSender"/>s here - <see cref="ServiceBusSender"/> only exposes
    /// <see cref="ServiceBusSender.DisposeAsync"/>, no synchronous <c>Dispose</c> - so those are left
    /// for the GC/broker-side idle timeout to reclaim on this path. <see cref="DisposeAsync"/> is what
    /// actually runs on a normal AKS shutdown (see its own remarks) and is the path that matters.
    /// </summary>
    public override void Dispose()
    {
        DisposeKafkaResources();
        base.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Disposes every cached <see cref="ServiceBusSender"/> (see <see cref="serviceBusSenders"/>)
    /// alongside the Kafka consumer/Schema Registry client - this is the path the host actually takes
    /// on a graceful shutdown: this class implements <see cref="IAsyncDisposable"/> alongside
    /// <see cref="IDisposable"/>, and the default <c>Microsoft.Extensions.DependencyInjection</c>
    /// <c>ServiceProvider</c> prefers <see cref="IAsyncDisposable.DisposeAsync"/> over
    /// <see cref="IDisposable.Dispose"/> when a singleton implements both, calling it once as the root
    /// provider itself is disposed at the end of <c>IHost.StopAsync</c> - a Kubernetes-initiated pod
    /// termination (redeploy, scale-down, `kubectl delete pod`) sends SIGTERM first and waits up to
    /// `terminationGracePeriodSeconds` (kubernetes-deployment-best-practices.instructions.md) before
    /// SIGKILL, which is exactly the window the generic host uses to run this. <b>A hard
    /// crash/SIGKILL/OOM-kill runs no code at all</b> - no `Dispose`/`DisposeAsync` override on any
    /// type can run in that case; the AMQP links these senders hold are instead reclaimed by Service
    /// Bus's own idle-connection timeout on the broker side, and a fresh process/pod opens fresh
    /// senders on restart via <see cref="GetServiceBusSender"/>. Also callable directly (e.g. the
    /// admin endpoint that lists/clears cached senders) without disposing the rest of this instance -
    /// see <see cref="ClearServiceBusSendersAsync"/>.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        DisposeKafkaResources();
        await ClearServiceBusSendersAsync();
        base.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Queue names this consumer currently holds a cached <see cref="ServiceBusSender"/> for - read by
    /// the admin endpoint that lists cached senders across every registered consumer.
    /// </summary>
    public IReadOnlyCollection<string> CachedServiceBusSenderQueueNames => [.. serviceBusSenders.Keys];

    /// <summary>
    /// Disposes and evicts every <see cref="ServiceBusSender"/> this consumer currently has cached -
    /// the next publish re-opens a fresh sender for its queue via <see cref="GetServiceBusSender"/>.
    /// Safe to call while the consumer is running: a publish already in flight holds its own reference
    /// to the sender it fetched, so clearing the cache underneath it does not fault that in-flight
    /// send - it only affects senders fetched after this call returns. Removes each entry before
    /// awaiting its disposal, not after, so a concurrent <see cref="GetServiceBusSender"/> call never
    /// observes (and reuses) a sender this method has already started disposing.
    /// </summary>
    public async Task ClearServiceBusSendersAsync()
    {
        foreach (var queueName in serviceBusSenders.Keys.ToArray())
        {
            if (serviceBusSenders.TryRemove(queueName, out var sender))
            {
                await sender.DisposeAsync();
            }
        }
    }
}
