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

    /// <summary>Checks whether a blob exists at the given container/path.</summary>
    /// <param name="containerName">Name of the Blob Storage container to check.</param>
    /// <param name="blobName">Name/path of the blob within the container.</param>
    /// <param name="cancellationToken">Token to cancel the check.</param>
    /// <returns><see langword="true"/> if the blob exists; <see langword="false"/> if the blob (or the container itself) does not.</returns>
    Task<bool> ExistsAsync(
        string containerName, string blobName, CancellationToken cancellationToken = default);

    /// <summary>Deletes a blob if it exists.</summary>
    /// <param name="containerName">Name of the Blob Storage container to delete from.</param>
    /// <param name="blobName">Name/path of the blob within the container.</param>
    /// <param name="cancellationToken">Token to cancel the delete.</param>
    /// <returns><see langword="true"/> if the blob existed and was deleted; <see langword="false"/> if there was nothing to delete.</returns>
    Task<bool> DeleteAsync(
        string containerName, string blobName, CancellationToken cancellationToken = default);

    /// <summary>Lists the names of every blob in a container whose name starts with <paramref name="prefix"/>.</summary>
    /// <param name="containerName">Name of the Blob Storage container to list.</param>
    /// <param name="prefix">Blob-name prefix to filter on, or <see langword="null"/>/empty for every blob in the container.</param>
    /// <param name="cancellationToken">Token to cancel the listing.</param>
    /// <returns>The matching blob names/paths - empty if none match or the container doesn't exist.</returns>
    Task<IReadOnlyList<string>> ListAsync(
        string containerName, string? prefix = null, CancellationToken cancellationToken = default);
}
