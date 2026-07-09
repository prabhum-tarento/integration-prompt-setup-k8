using Confluent.Kafka;
using IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="PartitionOffsetCommitTracker"/> - the low-water-mark tracker
/// that keeps Kafka offset commits in order even when multiple concurrent workers
/// (integration-resiliency.instructions.md §6) finish processing messages out of order.
/// </summary>
public class PartitionOffsetCommitTrackerTests
{
    private static readonly TopicPartition PartitionZero = new("inventory-events", new Partition(0));
    private static readonly TopicPartition PartitionOne = new("inventory-events", new Partition(1));

    [Fact(DisplayName = "Completions arriving in offset order commit each one immediately")]
    public void Complete_SequentialOffsets_CommitsEachOffsetPlusOneInOrder()
    {
        var commits = new List<long>();
        var tracker = new PartitionOffsetCommitTracker(offsets => commits.Add(offsets.Single().Offset.Value));

        tracker.EstablishBaseline(PartitionZero, 10);

        tracker.Complete(new TopicPartitionOffset(PartitionZero, new Offset(10)));
        tracker.Complete(new TopicPartitionOffset(PartitionZero, new Offset(11)));

        Assert.Equal([11, 12], commits);
    }

    [Fact(DisplayName = "A completion ahead of the low-water mark is held, not committed, until the gap closes")]
    public void Complete_OutOfOrderOffsets_HoldsUntilGapClosesThenCommitsOnce()
    {
        var commits = new List<long>();
        var tracker = new PartitionOffsetCommitTracker(offsets => commits.Add(offsets.Single().Offset.Value));

        tracker.EstablishBaseline(PartitionZero, 10);

        // Offsets 11 and 12 finish before 10 - e.g. two concurrent workers, and the one processing
        // the earliest-dispatched message is slower than the other two.
        tracker.Complete(new TopicPartitionOffset(PartitionZero, new Offset(12)));
        tracker.Complete(new TopicPartitionOffset(PartitionZero, new Offset(11)));

        Assert.Empty(commits);

        tracker.Complete(new TopicPartitionOffset(PartitionZero, new Offset(10)));

        // One commit call, folding the whole contiguous run through to 13 - not three separate
        // calls, and never a call for 11 or 12 alone.
        Assert.Equal([13], commits);
    }

    [Fact(DisplayName = "Each partition's low-water mark advances independently of the others")]
    public void Complete_MultiplePartitions_TracksEachPartitionIndependently()
    {
        var commits = new List<TopicPartitionOffset>();
        var tracker = new PartitionOffsetCommitTracker(offsets => commits.AddRange(offsets));

        tracker.EstablishBaseline(PartitionZero, 5);
        tracker.EstablishBaseline(PartitionOne, 100);

        tracker.Complete(new TopicPartitionOffset(PartitionOne, new Offset(100)));
        tracker.Complete(new TopicPartitionOffset(PartitionZero, new Offset(5)));

        Assert.Equal(2, commits.Count);
        Assert.Contains(commits, c => c.TopicPartition == PartitionOne && c.Offset.Value == 101);
        Assert.Contains(commits, c => c.TopicPartition == PartitionZero && c.Offset.Value == 6);
    }

    [Fact(DisplayName = "Completing an offset with no baseline established is a defensive no-op")]
    public void Complete_WithoutBaselineEstablished_DoesNotCommit()
    {
        var commits = new List<long>();
        var tracker = new PartitionOffsetCommitTracker(offsets => commits.Add(offsets.Single().Offset.Value));

        tracker.Complete(new TopicPartitionOffset(PartitionZero, new Offset(10)));

        Assert.Empty(commits);
    }

    [Fact(DisplayName = "Establishing a baseline twice for the same partition keeps the first (lowest) value")]
    public void EstablishBaseline_CalledAgainForSamePartition_KeepsFirstOffset()
    {
        var commits = new List<long>();
        var tracker = new PartitionOffsetCommitTracker(offsets => commits.Add(offsets.Single().Offset.Value));

        // Simulates the poll loop calling EstablishBaseline for every dispatched message, not just
        // the first, for the same partition.
        tracker.EstablishBaseline(PartitionZero, 10);
        tracker.EstablishBaseline(PartitionZero, 11);

        tracker.Complete(new TopicPartitionOffset(PartitionZero, new Offset(10)));

        Assert.Equal([11], commits);
    }
}
