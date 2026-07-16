namespace IIS.WMS.Consumer.Application.Messaging.Dtos;

/// <summary>One Kafka relay consumer's currently-cached <c>ServiceBusSender</c> queue names - see <see cref="IServiceBusSenderCacheService"/>.</summary>
/// <param name="ConsumerName">The relaying consumer's display name (e.g. <c>KafkaConsumerHostedService</c>'s own <c>ConsumerName</c>).</param>
/// <param name="QueueNames">Queue names this consumer currently holds a cached sender for.</param>
public sealed record ServiceBusSenderCacheEntry(string ConsumerName, IReadOnlyList<string> QueueNames);
