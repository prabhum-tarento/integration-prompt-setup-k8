namespace IIS.WMS.Common.DynamicValidation;

/// <summary>
/// The known <see cref="IDynamicEventValidator"/> transport folder segments - the set of values every
/// dynamic-validation template's blob path (<c>{transport}/{identifier}.cs</c>) can start with. Shared
/// between each transport's own hosted service (which passes one of these as a literal) and the
/// template CRUD API's request validation (which restricts a caller's <c>Transport</c> field to this
/// same set), so the two can never drift apart.
/// </summary>
public static class DynamicValidationTransports
{
    /// <summary>Kafka consumer templates - blob path <c>Kafka/{eventType}.cs</c>, keyed by the Kafka <c>Type</c> header value.</summary>
    public const string Kafka = "Kafka";

    /// <summary>Service Bus consumer templates - blob path <c>ServiceBus/{queueName}.cs</c>, keyed by the queue name.</summary>
    public const string ServiceBus = "ServiceBus";

    /// <summary>Every known transport - the CRUD API's allow-list source.</summary>
    public static readonly IReadOnlyList<string> All = [Kafka, ServiceBus];
}
