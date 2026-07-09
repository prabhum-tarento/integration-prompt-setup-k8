using Azure.Storage.Blobs;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Infrastructure.Resilience;
using Microsoft.Extensions.Logging;
using Polly.Registry;

namespace IIS.WMS.Consumer.Infrastructure.BlobStorage;

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
}
