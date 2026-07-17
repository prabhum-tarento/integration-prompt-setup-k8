using Azure.Storage.Blobs;
using IIS.WMS.Common.Resilience;
using Microsoft.Extensions.Logging;
using Polly.Registry;

namespace IIS.WMS.Common.BlobStorage;

/// <inheritdoc cref="IFileStore"/>
public sealed class BlobFileStore(
    BlobServiceClient blobServiceClient, ResiliencePipelineProvider<string> pipelineProvider, ILogger<BlobFileStore> logger)
    : IFileStore
{
    /// <inheritdoc />
    public async Task<string> UploadAsync(
        string containerName, string blobName, Stream content, CancellationToken cancellationToken = default)
    {
        logger.LogDebug("Uploading blob {ContainerName}/{BlobName}.", containerName, blobName);

        var pipeline = pipelineProvider.GetPipeline(ResiliencePipelines.BlobUpload);
        var containerClient = blobServiceClient.GetBlobContainerClient(containerName);
        var blobClient = containerClient.GetBlobClient(blobName);

        await pipeline.ExecuteAsync(
            async ct => await blobClient.UploadAsync(content, overwrite: true, ct), cancellationToken);

        logger.LogInformation("Uploaded blob {ContainerName}/{BlobName} to {Uri}.", containerName, blobName, blobClient.Uri);

        return blobClient.Uri.ToString();
    }

    /// <inheritdoc />
    public async Task<Stream> DownloadAsync(
        string containerName, string blobName, CancellationToken cancellationToken = default)
    {
        logger.LogDebug("Downloading blob {ContainerName}/{BlobName}.", containerName, blobName);

        var pipeline = pipelineProvider.GetPipeline(ResiliencePipelines.BlobUpload);
        var blobClient = blobServiceClient.GetBlobContainerClient(containerName).GetBlobClient(blobName);

        var response = await pipeline.ExecuteAsync(
            async ct => await blobClient.DownloadStreamingAsync(cancellationToken: ct), cancellationToken);

        logger.LogInformation("Downloaded blob {ContainerName}/{BlobName}.", containerName, blobName);

        return response.Value.Content;
    }

    /// <inheritdoc />
    public async Task<bool> ExistsAsync(
        string containerName, string blobName, CancellationToken cancellationToken = default)
    {
        logger.LogDebug("Checking blob {ContainerName}/{BlobName} exists.", containerName, blobName);

        var pipeline = pipelineProvider.GetPipeline(ResiliencePipelines.BlobUpload);
        var blobClient = blobServiceClient.GetBlobContainerClient(containerName).GetBlobClient(blobName);

        // ExistsAsync returns false (rather than throwing) when the blob or its whole container is
        // missing, so a not-yet-provisioned container reads as "no blob", not an error.
        var response = await pipeline.ExecuteAsync(
            async ct => await blobClient.ExistsAsync(ct), cancellationToken);

        return response.Value;
    }

    /// <inheritdoc />
    public async Task<bool> DeleteAsync(
        string containerName, string blobName, CancellationToken cancellationToken = default)
    {
        logger.LogDebug("Deleting blob {ContainerName}/{BlobName}.", containerName, blobName);

        var pipeline = pipelineProvider.GetPipeline(ResiliencePipelines.BlobUpload);
        var blobClient = blobServiceClient.GetBlobContainerClient(containerName).GetBlobClient(blobName);

        var response = await pipeline.ExecuteAsync(
            async ct => await blobClient.DeleteIfExistsAsync(cancellationToken: ct), cancellationToken);

        if (response.Value)
        {
            logger.LogInformation("Deleted blob {ContainerName}/{BlobName}.", containerName, blobName);
        }

        return response.Value;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> ListAsync(
        string containerName, string? prefix = null, CancellationToken cancellationToken = default)
    {
        logger.LogDebug("Listing blobs in {ContainerName} with prefix '{Prefix}'.", containerName, prefix);

        var pipeline = pipelineProvider.GetPipeline(ResiliencePipelines.BlobUpload);
        var containerClient = blobServiceClient.GetBlobContainerClient(containerName);

        try
        {
            // The whole enumeration runs inside one pipeline execution: GetBlobsAsync pages lazily, so
            // a transient fault can surface mid-iteration, not just on the first call.
            return await pipeline.ExecuteAsync<IReadOnlyList<string>>(
                async ct =>
                {
                    var names = new List<string>();

                    await foreach (var blob in containerClient.GetBlobsAsync(
                        Azure.Storage.Blobs.Models.BlobTraits.None, Azure.Storage.Blobs.Models.BlobStates.None, prefix, ct))
                    {
                        names.Add(blob.Name);
                    }

                    return names;
                },
                cancellationToken);
        }
        catch (Azure.RequestFailedException ex) when (ex.Status == 404)
        {
            // A not-yet-provisioned container lists as empty, matching ExistsAsync's semantics -
            // unlike ExistsAsync, the listing API has no built-in "missing container means none" path.
            logger.LogDebug("Container {ContainerName} does not exist - returning an empty listing.", containerName);

            return [];
        }
    }
}
