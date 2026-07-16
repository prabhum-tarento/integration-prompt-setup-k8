using Asp.Versioning;
using IIS.WMS.Consumer.Application.Messaging;
using IIS.WMS.Consumer.Application.Messaging.Dtos;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace IIS.WMS.Consumer.Api.Controllers;

/// <summary>
/// Admin endpoint over the cached <c>ServiceBusSender</c>s every Kafka relay consumer running in
/// <b>this process</b> holds (integration-resiliency.instructions.md §1) - see
/// <see cref="IServiceBusSenderCacheService"/>'s own remarks for the single-process scope this covers
/// and why it needs to move once the Kafka consumer splits into its own Deployment/Pod. <see cref="Get"/>
/// falls under the global <c>RequireAuthenticatedUser</c> fallback policy (Program.cs) like every other
/// non-health endpoint - it's a read with no side effect. <see cref="ClearAsync"/> additionally requires
/// <see cref="AdminPolicyName"/> (see Program.cs) - clearing sender state is a privileged operation that
/// shouldn't be reachable by every authenticated API consumer, only whichever principals are assigned
/// <see cref="AdminRoleName"/>.
/// </summary>
[ApiController]
[ApiVersion("1.0")]
[Route("api/v{version:apiVersion}/service-bus-senders")]
public sealed class ServiceBusSendersController(
    IServiceBusSenderCacheService cacheService, ILogger<ServiceBusSendersController> logger)
    : ControllerBase
{
    /// <summary>
    /// Authorization policy name (Program.cs) gating <see cref="ClearAsync"/> - requires
    /// <see cref="AdminRoleName"/>. Named here, alongside what it protects, rather than as a bare
    /// string literal in Program.cs and the <see cref="AuthorizeAttribute"/> below, so the two stay in
    /// sync.
    /// </summary>
    public const string AdminPolicyName = "ServiceBusSenders.Admin";

    /// <summary>
    /// Role claim value <see cref="AdminPolicyName"/> requires. Depends on the calling principal's
    /// Microsoft Entra ID token actually carrying this value in its <c>roles</c> claim (an Entra App
    /// Role assigned to that user/service principal) - verify the app registration backing
    /// <c>Authentication:Audience</c> (appsettings.json) has this App Role defined and assigned to
    /// whoever needs to call <see cref="ClearAsync"/>; this code has no way to confirm that from here.
    /// </summary>
    public const string AdminRoleName = "ServiceBusSenders.Admin";

    /// <summary>Lists every registered consumer's currently-cached Service Bus sender queue names.</summary>
    /// <returns><c>200 OK</c> with one entry per Kafka relay consumer running in this process.</returns>
    [HttpGet]
    [ProducesResponseType<IReadOnlyList<ServiceBusSenderCacheEntry>>(StatusCodes.Status200OK)]
    public ActionResult<IReadOnlyList<ServiceBusSenderCacheEntry>> Get()
    {
        var entries = cacheService.ListCachedSenders();

        logger.LogDebug("GET cached Service Bus senders - {ConsumerCount} consumer(s).", entries.Count);

        return Ok(entries);
    }

    /// <summary>
    /// Disposes and evicts every cached Service Bus sender across every registered consumer - each one
    /// re-opens a fresh sender for its queue the next time it publishes. Requires
    /// <see cref="AdminPolicyName"/> - a narrower authorization than the global
    /// <c>RequireAuthenticatedUser</c> fallback every other endpoint (including <see cref="Get"/>) uses.
    /// </summary>
    /// <param name="cancellationToken">Request abort token.</param>
    /// <returns><c>200 OK</c> with the entries that were cleared (the state immediately before clearing), or <c>403 Forbidden</c> if the caller lacks <see cref="AdminRoleName"/>.</returns>
    [HttpDelete]
    [Authorize(Policy = AdminPolicyName)]
    [ProducesResponseType<IReadOnlyList<ServiceBusSenderCacheEntry>>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    public async Task<ActionResult<IReadOnlyList<ServiceBusSenderCacheEntry>>> ClearAsync(CancellationToken cancellationToken)
    {
        var cleared = cacheService.ListCachedSenders();

        await cacheService.ClearCachedSendersAsync(cancellationToken);

        logger.LogInformation(
            "DELETE cleared cached Service Bus senders for {ConsumerCount} consumer(s).", cleared.Count);

        return Ok(cleared);
    }
}
