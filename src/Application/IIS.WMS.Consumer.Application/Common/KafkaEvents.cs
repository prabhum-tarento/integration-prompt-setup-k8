namespace IIS.WMS.Consumer.Application.Common;

public class KafkaEvents
{
    public const string InventoryStateChangedEventType = "inventory.InventoryStateChanged";
    public const string InventoryAdjustedEventType = "inventory.InventoryAdjusted";
    public const string InternalHallmarkingStatusChangedEventType = "inventory.InternalHallmarkingStatusChanged";
    public const string StockSyncSubmittedEventType = "inventory.StockSyncSubmitted";
    public const string OrderToInventoryAllocatedEventType = "inventory.OrderToInventoryAllocated";
    public const string OrderStatusChangedEventType = "b2b.sales.OrderStatusChanged";
    public const string GoodsInTransitReceivedEventType = "b2b.purchase.GoodsInTransitReceived";
    public const string ConsolidatedOrderShippedEventType = "b2b.sales.ConsolidatedOrderShipped";
    public const string StockOnHandUpdatedEventType = "inventory.StockOnHandUpdated";

    /// <summary>
    /// <c>Kafka:Functions</c> allow-list key for the JSON-contract consumer - this consumer isn't
    /// gated by either Avro event type above, so it needs its own identity distinct from both.
    /// </summary>
    public const string InventoryEventsConsumerKey = "InventoryEvents";

    /// <summary><c>Kafka:Functions</c> allow-list key for the high-volume bulk-import consumer.</summary>
    public const string BulkInventoryImportConsumerKey = "BulkInventoryImport";

    /// <summary><c>Kafka:Functions</c> allow-list key for the internal-hallmarking-status-changed consumer.</summary>
    public const string InternalHallmarkingStatusChangedConsumerKey = "InternalHallmarkingStatusChanged";

    /// <summary><c>Kafka:Functions</c> allow-list key for the order-status-changed consumer.</summary>
    public const string OrderStatusChangedConsumerKey = "OrderStatusChanged";

    /// <summary><c>Kafka:Functions</c> allow-list key for the goods-in-transit-received consumer.</summary>
    public const string GoodsInTransitReceivedConsumerKey = "GoodsInTransitReceived";

    /// <summary><c>Kafka:Functions</c> allow-list key for the consolidated-order-shipped consumer.</summary>
    public const string ConsolidatedOrderShippedConsumerKey = "ConsolidatedOrderShipped";
}
