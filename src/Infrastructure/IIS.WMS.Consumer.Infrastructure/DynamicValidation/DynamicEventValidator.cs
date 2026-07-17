using System.Collections.Concurrent;
using Confluent.Kafka;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.Exceptions;
using Microsoft.CodeAnalysis.Scripting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace IIS.WMS.Consumer.Infrastructure.DynamicValidation;

/// <summary>
/// <inheritdoc cref="IDynamicEventValidator"/>
/// </summary>
/// <remarks>
/// Runs on the consumer hot path, so template lookups are cached per blob path for
/// <see cref="DynamicValidationOptions.CacheDuration"/> - all three lookup outcomes, not just the
/// happy one: a compiled runner (executed per message with zero storage calls), a confirmed-missing
/// marker (so a schema with no template doesn't pay an existence check per message), and a
/// compilation failure (rethrown per message until the entry expires, so one broken template
/// dead-letters its own traffic - the documented validator-throw semantics - without recompiling on
/// every message). Concurrent workers can race the same expired entry and refresh twice; that's an
/// accepted duplicate read of a small blob, cheaper than serializing the hot path behind a lock.
/// </remarks>
public sealed class DynamicEventValidator(
    [FromKeyedServices(BlobStorageServiceCollectionExtensions.HotTierKey)] IFileStore hotFileStore,
    IOptions<BlobStorageOptions> blobStorageOptions,
    IOptions<DynamicValidationOptions> options,
    IValidationScriptCompiler compiler,
    TimeProvider timeProvider,
    ILogger<DynamicEventValidator> logger) : IDynamicEventValidator
{
    /// <summary>One cached template lookup - exactly one of <paramref name="Runner"/>/<paramref name="CompilationFailure"/> is set for a stored template; both are null for a confirmed-missing one.</summary>
    /// <param name="Runner">The compiled template, or <see langword="null"/> if no template is stored (or it didn't compile).</param>
    /// <param name="CompilationFailure">The failure to rethrow per message when the stored template doesn't compile.</param>
    /// <param name="ExpiresAtUtc">When this entry must be re-read from Blob Storage.</param>
    private sealed record CachedTemplate(
        ScriptRunner<bool>? Runner, TemplateCompilationException? CompilationFailure, DateTimeOffset ExpiresAtUtc);

    private readonly ConcurrentDictionary<string, CachedTemplate> cache = new();

    /// <inheritdoc />
    public async Task<bool> ValidateAsync(
        string schemaName, string eventType, object message, Headers? headers, ILogger messageLogger, IServiceProvider scopedServices, CancellationToken cancellationToken)
    {
        // No Type header means no {schemaName}/{eventType}.cs path to look up - not a failure, the
        // message just has no dynamic rule to run, same as a missing template.
        if (!options.Value.Enabled || string.IsNullOrEmpty(schemaName) || string.IsNullOrEmpty(eventType))
        {
            return true;
        }

        var blobName = $"{schemaName}/{eventType}.cs";

        if (!cache.TryGetValue(blobName, out var cached) || cached.ExpiresAtUtc <= timeProvider.GetUtcNow())
        {
            cached = await RefreshAsync(blobName, cancellationToken);
        }

        if (cached.CompilationFailure is not null)
        {
            throw cached.CompilationFailure;
        }

        if (cached.Runner is null)
        {
            return true;
        }

        return await cached.Runner(new ValidationScriptGlobals(message, headers, messageLogger, scopedServices), cancellationToken);
    }

    /// <summary>Re-reads one template from Blob Storage, compiles it if present, and caches the outcome (found/missing/broken) until it expires.</summary>
    private async Task<CachedTemplate> RefreshAsync(string blobName, CancellationToken cancellationToken)
    {
        var containerName = blobStorageOptions.Value.ValidationTemplateContainerName;
        var expiresAtUtc = timeProvider.GetUtcNow() + options.Value.CacheDuration;

        CachedTemplate refreshed;

        if (!await hotFileStore.ExistsAsync(containerName, blobName, cancellationToken))
        {
            logger.LogDebug("No validation template at {ContainerName}/{BlobName} - caching as missing.", containerName, blobName);

            refreshed = new CachedTemplate(null, null, expiresAtUtc);
        }
        else
        {
            await using var content = await hotFileStore.DownloadAsync(containerName, blobName, cancellationToken);
            using var reader = new StreamReader(content);
            var code = await reader.ReadToEndAsync(cancellationToken);

            try
            {
                refreshed = new CachedTemplate(compiler.Compile(blobName, code), null, expiresAtUtc);
            }
            catch (TemplateCompilationException ex)
            {
                // Cached and rethrown per message rather than recompiled per message - a template
                // edited to a broken state behind the API's own compile check (or written to the
                // container directly) shouldn't add a compile per message on top of dead-lettering.
                logger.LogCritical(ex,
                    "Validation template {ContainerName}/{BlobName} failed to compile - every message of this event type hard-fails validation until the template is fixed.",
                    containerName, blobName);

                refreshed = new CachedTemplate(null, ex, expiresAtUtc);
            }
        }

        cache[blobName] = refreshed;

        return refreshed;
    }
}
