using IIS.WMS.Consumer.Application.Messaging;
using IIS.WMS.Consumer.Application.Messaging.Dtos;

namespace IIS.WMS.Consumer.Infrastructure.Messaging;

/// <summary>
/// Implements <see cref="IServiceBusSenderCacheService"/> by fanning out to every
/// <see cref="IServiceBusSenderCacheSource"/> registered in this process - today, the one shared
/// <c>ServiceBusRelayPublisher</c> singleton every Kafka consumer relays through
/// (<c>MessagingServiceCollectionExtensions.AddMessaging</c> registers it under this interface once,
/// not per consumer, since sender caching itself is no longer split per caller). See
/// <see cref="IServiceBusSenderCacheService"/>'s own remarks for the single-process scope this covers.
/// </summary>
public sealed class ServiceBusSenderCacheService(IEnumerable<IServiceBusSenderCacheSource> sources) : IServiceBusSenderCacheService
{
    /// <inheritdoc />
    public IReadOnlyList<ServiceBusSenderCacheEntry> ListCachedSenders() =>
        sources
            .Select(source => new ServiceBusSenderCacheEntry(source.ConsumerName, [.. source.CachedServiceBusSenderQueueNames]))
            .ToList();

    /// <inheritdoc />
    public async Task ClearCachedSendersAsync(CancellationToken cancellationToken)
    {
        foreach (var source in sources)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await source.ClearServiceBusSendersAsync();
        }
    }
}
