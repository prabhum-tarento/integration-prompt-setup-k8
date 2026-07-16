using System.Net;
using Microsoft.Azure.Cosmos;

namespace IIS.WMS.Consumer.IntegrationTests.TestDoubles.Cosmos;

/// <summary>
/// Minimal concrete <see cref="FeedResponse{T}"/> over an already-materialized in-memory list - what a
/// test-only <c>ReadNextPageAsync</c> override returns instead of the real <c>ToFeedIterator()</c> +
/// <c>ReadNextAsync</c> pair (cosmos-db.instructions.md §13). Continuation is not supported (always
/// empty) - the integration tests this backs never page across more than one page of results.
/// </summary>
internal sealed class InMemoryFeedResponse<T>(IEnumerable<T> items) : FeedResponse<T>
{
    private readonly List<T> items = items.ToList();

    public override IEnumerator<T> GetEnumerator() => items.GetEnumerator();
    public override string ContinuationToken => string.Empty;
    public override string IndexMetrics => string.Empty;
    public override int Count => items.Count;
    public override CosmosDiagnostics Diagnostics => null!;
    public override Headers Headers => new();
    public override HttpStatusCode StatusCode => HttpStatusCode.OK;
    public override IReadOnlyList<T> Resource => items;
    public override double RequestCharge => 0;
}
