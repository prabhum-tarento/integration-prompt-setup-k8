using System.Collections.Concurrent;
using Confluent.Kafka;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;

/// <summary>
/// Commits Kafka offsets in strict per-partition order even though multiple concurrent workers
/// (integration-resiliency.instructions.md §6) can finish processing messages out of order. Each
/// partition tracks a low-water mark - the next offset it's safe to tell Kafka we've fully
/// processed - and only advances (and commits) that mark once every offset below it has completed,
/// folding in any out-of-order completions that arrived early once the gap in front of them closes.
/// </summary>
/// <remarks>
/// A completion for an offset below the current mark is a no-op (already covered by a prior
/// commit); this deliberately does not happen in normal operation and is only a defensive guard.
/// This tracker does not handle partition revocation mid-flight - see the caller's remarks on that
/// limitation.
/// </remarks>
public sealed class PartitionOffsetCommitTracker(Action<IReadOnlyCollection<TopicPartitionOffset>> commit)
{
    private sealed class PartitionState
    {
        /// <summary>The next raw message offset this partition is still waiting on before it can advance.</summary>
        public long? NextOffsetToCommit;

        /// <summary>Offsets that completed ahead of <see cref="NextOffsetToCommit"/>, held until the gap in front of them closes.</summary>
        public readonly SortedSet<long> CompletedAheadOfBaseline = [];
    }

    private readonly ConcurrentDictionary<TopicPartition, PartitionState> partitions = new();

    /// <summary>
    /// Records the lowest offset this partition has ever had dispatched for processing - a no-op if
    /// a baseline is already set. Must be called from the single-threaded poll loop, in offset
    /// order, before the corresponding message is handed to a worker - that ordering guarantee is
    /// what makes the first call for a given partition the correct low-water mark, even though
    /// <see cref="Complete"/> is called later from multiple concurrent workers.
    /// </summary>
    public void EstablishBaseline(TopicPartition partition, long offset)
    {
        var state = GetOrAddState(partition);

        lock (state)
        {
            state.NextOffsetToCommit ??= offset;
        }
    }

    /// <summary>
    /// Marks one offset as fully processed (published downstream, or deliberately skipped as a
    /// poison message). Advances and commits the partition's low-water mark if this closes a gap;
    /// otherwise just records the completion to be folded in once the gap does close.
    /// </summary>
    public void Complete(TopicPartitionOffset topicPartitionOffset)
    {
        var state = GetOrAddState(topicPartitionOffset.TopicPartition);
        var offset = topicPartitionOffset.Offset.Value;

        lock (state)
        {
            if (state.NextOffsetToCommit is null || offset < state.NextOffsetToCommit)
            {
                // Already covered by a prior commit, or completed before any baseline was
                // established - shouldn't happen given the calling convention above; ignore rather
                // than commit backwards.
                return;
            }

            if (offset != state.NextOffsetToCommit)
            {
                state.CompletedAheadOfBaseline.Add(offset);
                return;
            }

            var next = offset + 1;

            while (state.CompletedAheadOfBaseline.Remove(next))
            {
                next++;
            }

            state.NextOffsetToCommit = next;

            // Committed while still holding the per-partition lock, deliberately - this is what
            // guarantees commit calls for one partition are issued in strictly increasing offset
            // order even though they're triggered from different worker threads. Committing outside
            // the lock would let a later, higher-offset commit race ahead of an earlier one still in
            // flight, so a slower call could land last and regress the committed position.
            commit([new TopicPartitionOffset(topicPartitionOffset.TopicPartition, next)]);
        }
    }

    private PartitionState GetOrAddState(TopicPartition partition) =>
        partitions.GetOrAdd(partition, static _ => new PartitionState());
}
