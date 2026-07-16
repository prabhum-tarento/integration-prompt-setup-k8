namespace IIS.WMS.Consumer.Infrastructure.Messaging;

/// <summary>
/// One Kafka relay consumer's cached-<c>ServiceBusSender</c> surface - implemented by
/// <see cref="Kafka.ConsumerHostedService"/>, and deliberately narrow (not the whole
/// <c>ConsumerHostedService</c> base type) so <see cref="ServiceBusSenderCacheService"/> - and its
/// unit tests - depend on exactly this, not on constructing a real Kafka consumer.
/// </summary>
public interface IServiceBusSenderCacheSource
{
    /// <summary>This consumer's display name - see <c>ConsumerHostedService.ConsumerName</c>.</summary>
    string ConsumerName { get; }

    /// <summary>Queue names this consumer currently holds a cached <c>ServiceBusSender</c> for.</summary>
    IReadOnlyCollection<string> CachedServiceBusSenderQueueNames { get; }

    /// <summary>Disposes and evicts every cached sender - see <c>ConsumerHostedService.ClearServiceBusSendersAsync</c>.</summary>
    Task ClearServiceBusSendersAsync();
}
