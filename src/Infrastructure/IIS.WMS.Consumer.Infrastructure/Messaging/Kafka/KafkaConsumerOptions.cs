namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>Bound from the <c>Kafka</c> configuration section - settings for the JSON-contract inventory events consumer.</summary>
public sealed class KafkaConsumerOptions : ConsumerOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "Kafka";
}
