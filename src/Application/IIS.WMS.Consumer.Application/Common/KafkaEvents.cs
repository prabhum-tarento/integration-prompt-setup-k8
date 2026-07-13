namespace IIS.WMS.Consumer.Application.Common;

public class KafkaEvents
{
    public const string InventoryStateChangedEventType = "inventory.InventoryStateChanged";
    public const string InventoryAdjustedEventType = "inventory.InventoryAdjusted";

    /// <summary>
    /// <c>Kafka:Functions</c> allow-list key for the JSON-contract consumer - this consumer isn't
    /// gated by either Avro event type above, so it needs its own identity distinct from both.
    /// </summary>
    public const string InventoryEventsConsumerKey = "InventoryEvents";

    /// <summary><c>Kafka:Functions</c> allow-list key for the high-volume bulk-import consumer.</summary>
    public const string BulkInventoryImportConsumerKey = "BulkInventoryImport";
}
