using System.Threading.Channels;
using IIS.WMS.Consumer.Domain.Aggregates;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.MessageArchiving;

/// <inheritdoc cref="IMessageArchiveWriter"/>
/// <remarks>
/// Wraps the bounded <see cref="Channel{T}"/> registered by <see cref="MessageArchiveServiceCollectionExtensions"/> -
/// see that class and <see cref="MessageArchiveBackgroundService"/> for the rest of the pipeline.
/// <see cref="Enqueue"/> uses <see cref="ChannelWriter{T}.TryWrite"/>, never <c>WriteAsync</c> -
/// <c>TryWrite</c> never blocks, which is what makes this genuinely non-blocking for the calling worker
/// rather than merely "usually fast."
/// </remarks>
public sealed class MessageArchiveWriter(Channel<MessageArchive> channel, ILogger<MessageArchiveWriter> logger) : IMessageArchiveWriter
{
    /// <inheritdoc />
    public void Enqueue(MessageArchive entry)
    {
        if (channel.Writer.TryWrite(entry))
        {
            return;
        }

        // The channel is full - the background worker is falling behind write volume (or the process is
        // shutting down and the writer has already been completed). Dropping here rather than awaiting
        // keeps the guarantee that the calling worker is never slowed down by archiving; this Critical
        // log is the operator-facing signal that MessageArchive:ChannelCapacity needs raising or a
        // destination's throughput needs investigating, per this repo's own "never block, but never
        // silently swallow" philosophy (integration-resiliency.instructions.md §1/§5).
        logger.LogCritical(
            "MessageArchive channel is full or closed - dropping archive entry {Id}/{Category}.",
            entry.Id, entry.Category);
    }
}
