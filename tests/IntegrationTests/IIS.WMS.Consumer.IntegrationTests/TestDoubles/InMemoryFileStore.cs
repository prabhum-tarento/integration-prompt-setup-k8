using System.Collections.Concurrent;
using IIS.WMS.Common.BlobStorage;

namespace IIS.WMS.Consumer.IntegrationTests.TestDoubles;

/// <summary>
/// In-memory <see cref="IFileStore"/> - stands in for the real Blob Storage-backed <c>BlobFileStore</c>
/// in the pipeline integration test (integration-resiliency.instructions.md §9) so the Kafka consumer's
/// unconditional cold-tier audit write (§1) doesn't spend several seconds retrying against an
/// unreachable Azure Storage account before its own best-effort catch swallows the failure - this test
/// isn't exercising Blob Storage, only the Kafka → Service Bus → Cosmos DB path.
/// </summary>
public sealed class InMemoryFileStore : IFileStore
{
    private readonly ConcurrentDictionary<string, byte[]> blobs = new();

    public async Task<string> UploadAsync(string containerName, string blobName, Stream content, CancellationToken cancellationToken = default)
    {
        using var buffer = new MemoryStream();
        await content.CopyToAsync(buffer, cancellationToken);
        blobs[$"{containerName}/{blobName}"] = buffer.ToArray();

        return $"memory://{containerName}/{blobName}";
    }

    public Task<Stream> DownloadAsync(string containerName, string blobName, CancellationToken cancellationToken = default)
    {
        if (!blobs.TryGetValue($"{containerName}/{blobName}", out var bytes))
        {
            throw new FileNotFoundException($"Blob '{blobName}' not found in container '{containerName}'.");
        }

        return Task.FromResult<Stream>(new MemoryStream(bytes));
    }

    public Task<bool> ExistsAsync(string containerName, string blobName, CancellationToken cancellationToken = default) =>
        Task.FromResult(blobs.ContainsKey($"{containerName}/{blobName}"));

    public Task<bool> DeleteAsync(string containerName, string blobName, CancellationToken cancellationToken = default) =>
        Task.FromResult(blobs.TryRemove($"{containerName}/{blobName}", out _));

    public Task<IReadOnlyList<string>> ListAsync(string containerName, string? prefix = null, CancellationToken cancellationToken = default)
    {
        // Keys are stored as {containerName}/{blobName}, so strip the container segment back off -
        // ListAsync's contract returns blob names within the container, not full storage paths.
        var containerPrefix = $"{containerName}/";

        IReadOnlyList<string> names = blobs.Keys
            .Where(key => key.StartsWith(containerPrefix, StringComparison.Ordinal))
            .Select(key => key[containerPrefix.Length..])
            .Where(name => string.IsNullOrEmpty(prefix) || name.StartsWith(prefix, StringComparison.Ordinal))
            .ToArray();

        return Task.FromResult(names);
    }
}
