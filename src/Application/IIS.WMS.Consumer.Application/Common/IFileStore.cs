namespace IIS.WMS.Consumer.Application.Common;

/// <summary>
/// Abstraction over Azure Blob Storage so callers don't depend on the Azure SDK directly
/// (integration-resiliency.instructions.md §5). Covers both the hot tier (imports/exports) and the
/// optional cold tier (request-audit) - the caller supplies the container name, so this interface
/// doesn't encode tier-specific policy itself.
/// </summary>
public interface IFileStore
{
    /// <summary>Uploads <paramref name="content"/> to the given container/blob, overwriting any existing blob at that path.</summary>
    /// <param name="containerName">Name of the target Blob Storage container (e.g. <c>imports</c>, <c>exports</c>, <c>request-audit</c>).</param>
    /// <param name="blobName">Name/path of the blob within the container.</param>
    /// <param name="content">Stream to upload; the caller retains ownership and disposal.</param>
    /// <param name="cancellationToken">Token to cancel the upload.</param>
    /// <returns>The uploaded blob's URI.</returns>
    Task<string> UploadAsync(
        string containerName, string blobName, Stream content, CancellationToken cancellationToken = default);

    /// <summary>Downloads a blob's content as a stream.</summary>
    /// <param name="containerName">Name of the source Blob Storage container.</param>
    /// <param name="blobName">Name/path of the blob within the container.</param>
    /// <param name="cancellationToken">Token to cancel the download.</param>
    /// <returns>A readable stream over the blob's content - the caller is responsible for disposing it.</returns>
    Task<Stream> DownloadAsync(
        string containerName, string blobName, CancellationToken cancellationToken = default);
}
