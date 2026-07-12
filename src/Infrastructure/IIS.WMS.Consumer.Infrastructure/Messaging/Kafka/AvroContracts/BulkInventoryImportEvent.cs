using Avro;
using Avro.Specific;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka.AvroContracts;

/// <summary>
/// <b>Placeholder Avro contract for the bulk-import topic</b> - hand-written against
/// <see cref="ISpecificRecord"/> directly (the same interface a real <c>avrogen</c>-generated type
/// or a <c>NexusFacades</c>-style package would implement) rather than added as generated code, since
/// no real schema/package exists for this event yet. Swap this out for the real generated type once
/// the upstream bulk-import system's schema is registered - the only things depending on the exact
/// shape are this class, <see cref="Validators.BulkInventoryImportEventValidator"/>, and
/// <see cref="BulkInventoryImportConsumerHostedService"/>. <c>LastUpdatedUtcMillis</c> is a plain
/// Unix-millis <see langword="long"/>, not an Avro logical <c>timestamp-millis</c> type, so this
/// placeholder doesn't depend on the Avro codegen tool's logical-type conversion behavior - a real
/// generated schema would typically use the logical type instead and expose <see cref="DateTime"/>
/// directly.
/// </summary>
public sealed class BulkInventoryImportEvent : ISpecificRecord
{
    private const string SchemaJson = """
        {
          "type": "record",
          "name": "BulkInventoryImportEvent",
          "namespace": "iis.wms.consumer.bulkimport",
          "fields": [
            { "name": "eventId", "type": "string" },
            { "name": "warehouseId", "type": "string" },
            { "name": "sku", "type": "string" },
            { "name": "quantity", "type": "int" },
            { "name": "sourceSystem", "type": "string" },
            { "name": "lastUpdatedUtcMillis", "type": "long" }
          ]
        }
        """;

    /// <summary>Parsed schema every instance reports via <see cref="Schema"/> - required by <see cref="ISpecificRecord"/>.</summary>
    public static readonly Schema AvroSchema = Avro.Schema.Parse(SchemaJson);

    /// <summary>Deterministic id from the upstream system - this consumer's Service Bus message id and Kafka dedup key.</summary>
    public string EventId { get; set; } = default!;

    /// <summary>Warehouse this on-hand figure belongs to.</summary>
    public string WarehouseId { get; set; } = default!;

    /// <summary>SKU this on-hand figure belongs to.</summary>
    public string Sku { get; set; } = default!;

    /// <summary>On-hand quantity as reported by the upstream system.</summary>
    public int Quantity { get; set; }

    /// <summary>Identifies the upstream system that produced this event.</summary>
    public string SourceSystem { get; set; } = default!;

    /// <summary>Unix milliseconds the upstream system last updated this figure.</summary>
    public long LastUpdatedUtcMillis { get; set; }

    /// <inheritdoc />
    public Schema Schema => AvroSchema;

    /// <inheritdoc />
    public object Get(int fieldPos) => fieldPos switch
    {
        0 => EventId,
        1 => WarehouseId,
        2 => Sku,
        3 => Quantity,
        4 => SourceSystem,
        5 => LastUpdatedUtcMillis,
        _ => throw new AvroRuntimeException($"Bad index {fieldPos} in Get() for {nameof(BulkInventoryImportEvent)}."),
    };

    /// <inheritdoc />
    public void Put(int fieldPos, object fieldValue)
    {
        switch (fieldPos)
        {
            case 0: EventId = (string)fieldValue; break;
            case 1: WarehouseId = (string)fieldValue; break;
            case 2: Sku = (string)fieldValue; break;
            case 3: Quantity = (int)fieldValue; break;
            case 4: SourceSystem = (string)fieldValue; break;
            case 5: LastUpdatedUtcMillis = (long)fieldValue; break;
            default: throw new AvroRuntimeException($"Bad index {fieldPos} in Put() for {nameof(BulkInventoryImportEvent)}.");
        }
    }
}
