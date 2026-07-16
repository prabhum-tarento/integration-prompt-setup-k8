using IIS.WMS.Consumer.Application.Messaging;
using IIS.WMS.Consumer.Application.Messaging.Dtos;

namespace IIS.WMS.Consumer.Infrastructure.Messaging;

/// <summary>
/// Implements <see cref="IServiceBusSenderCacheService"/> by fanning out to every
/// <see cref="IServiceBusSenderCacheSource"/> registered in this process - one per Kafka relay
/// consumer actually started (<c>MessagingServiceCollectionExtensions.AddKafkaConsumer</c> registers
/// each concrete consumer under this interface too, forwarding to the same singleton instance it
/// registers under <c>IHostedService</c>). See <see cref="IServiceBusSenderCacheService"/>'s own
/// remarks for the single-process scope this covers.
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
