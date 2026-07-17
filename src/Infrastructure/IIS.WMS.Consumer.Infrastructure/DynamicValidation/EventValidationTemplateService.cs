using System.Text;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.EventValidationTemplates;
using IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;
using IIS.WMS.Consumer.Application.Exceptions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.DynamicValidation;

/// <summary>
/// <inheritdoc cref="IEventValidationTemplateService"/>
/// </summary>
/// <remarks>
/// The exists-then-write sequences here are deliberately not transactional - Blob Storage has no
/// cross-call transaction, so two concurrent admins racing the same template can lose one write
/// (last-writer-wins, same as the underlying container). Acceptable for an admin-curated set of
/// templates; ETag-conditional writes are the upgrade path if that ever stops being true. A change
/// takes effect on the consumer once <see cref="DynamicEventValidator"/>'s cache entry for that
/// template expires, not instantly.
/// </remarks>
public sealed class EventValidationTemplateService(
    [FromKeyedServices(BlobStorageServiceCollectionExtensions.HotTierKey)] IFileStore hotFileStore,
    IOptions<BlobStorageOptions> blobStorageOptions,
    IValidationScriptCompiler compiler,
    ILogger<EventValidationTemplateService> logger) : IEventValidationTemplateService
{
    private const string TemplateExtension = ".cs";

    private string ContainerName => blobStorageOptions.Value.ValidationTemplateContainerName;

    /// <inheritdoc />
    public async Task<EventValidationTemplateResponse?> GetAsync(
        string schemaName, string eventType, CancellationToken cancellationToken = default)
    {
        var blobName = ToBlobName(schemaName, eventType);
        logger.LogDebug("Looking up validation template {BlobName}.", blobName);

        if (!await hotFileStore.ExistsAsync(ContainerName, blobName, cancellationToken))
        {
            return null;
        }

        await using var content = await hotFileStore.DownloadAsync(ContainerName, blobName, cancellationToken);
        using var reader = new StreamReader(content);
        var code = await reader.ReadToEndAsync(cancellationToken);

        return new EventValidationTemplateResponse(schemaName, eventType, code);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<EventValidationTemplateSummary>> ListAsync(
        string? schemaName = null, CancellationToken cancellationToken = default)
    {
        // Trailing slash so "inventory" doesn't also match an "inventory-legacy/" folder.
        var prefix = string.IsNullOrEmpty(schemaName) ? null : $"{schemaName}/";
        logger.LogDebug("Listing validation templates with prefix '{Prefix}'.", prefix);

        var blobNames = await hotFileStore.ListAsync(ContainerName, prefix, cancellationToken);

        return blobNames
            .Select(TryParseBlobName)
            .Where(summary => summary is not null)
            .Select(summary => summary!)
            .ToArray();
    }

    /// <inheritdoc />
    public async Task<EventValidationTemplateResponse> CreateAsync(
        CreateEventValidationTemplateRequest request, CancellationToken cancellationToken = default)
    {
        var blobName = ToBlobName(request.SchemaName, request.EventType);

        // Compile before the existence check, not just before the write - a request that would 409
        // anyway still deserves the compile diagnostics if its code is also broken.
        compiler.Compile(blobName, request.Code);

        if (await hotFileStore.ExistsAsync(ContainerName, blobName, cancellationToken))
        {
            throw new ConflictException("EventValidationTemplate", blobName);
        }

        await UploadAsync(blobName, request.Code, cancellationToken);

        logger.LogInformation("Created validation template {BlobName} ({CodeLength} chars).", blobName, request.Code.Length);

        return new EventValidationTemplateResponse(request.SchemaName, request.EventType, request.Code);
    }

    /// <inheritdoc />
    public async Task<EventValidationTemplateResponse> UpdateAsync(
        string schemaName, string eventType, UpdateEventValidationTemplateRequest request, CancellationToken cancellationToken = default)
    {
        var blobName = ToBlobName(schemaName, eventType);

        compiler.Compile(blobName, request.Code);

        if (!await hotFileStore.ExistsAsync(ContainerName, blobName, cancellationToken))
        {
            throw new NotFoundException("EventValidationTemplate", blobName);
        }

        await UploadAsync(blobName, request.Code, cancellationToken);

        logger.LogInformation("Updated validation template {BlobName} ({CodeLength} chars).", blobName, request.Code.Length);

        return new EventValidationTemplateResponse(schemaName, eventType, request.Code);
    }

    /// <inheritdoc />
    public IReadOnlyList<EventValidationTemplateExample> GetExamples() => EventValidationTemplateExamples.All;

    /// <inheritdoc />
    public async Task<bool> DeleteAsync(
        string schemaName, string eventType, CancellationToken cancellationToken = default)
    {
        var blobName = ToBlobName(schemaName, eventType);

        var deleted = await hotFileStore.DeleteAsync(ContainerName, blobName, cancellationToken);

        if (deleted)
        {
            logger.LogInformation("Deleted validation template {BlobName}.", blobName);
        }

        return deleted;
    }

    /// <summary>Uploads one template's code as a UTF-8 blob.</summary>
    private async Task UploadAsync(string blobName, string code, CancellationToken cancellationToken)
    {
        using var content = new MemoryStream(Encoding.UTF8.GetBytes(code));
        await hotFileStore.UploadAsync(ContainerName, blobName, content, cancellationToken);
    }

    /// <summary>The <c>{schemaName}/{eventType}.cs</c> blob path a template is stored under.</summary>
    private static string ToBlobName(string schemaName, string eventType) =>
        $"{schemaName}/{eventType}{TemplateExtension}";

    /// <summary>
    /// Parses a container blob name back into a template identity, or <see langword="null"/> for a
    /// blob that doesn't follow the <c>{schemaName}/{eventType}.cs</c> convention (anything placed in
    /// the container by hand) - listed templates are only ever the ones this service can serve.
    /// </summary>
    private static EventValidationTemplateSummary? TryParseBlobName(string blobName)
    {
        if (!blobName.EndsWith(TemplateExtension, StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        var segments = blobName[..^TemplateExtension.Length].Split('/');

        return segments is [{ Length: > 0 } schemaName, { Length: > 0 } eventType]
            ? new EventValidationTemplateSummary(schemaName, eventType)
            : null;
    }
}
