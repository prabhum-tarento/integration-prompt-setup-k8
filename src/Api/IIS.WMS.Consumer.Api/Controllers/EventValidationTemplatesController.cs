using Asp.Versioning;
using IIS.WMS.Consumer.Application.EventValidationTemplates;
using IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace IIS.WMS.Consumer.Api.Controllers;

/// <summary>
/// CRUD over the event validation templates - the <c>{transport}/{identifier}.cs</c> C# script blobs
/// in the hot-tier validation-template container that each transport's consumer compiles and executes
/// against every matching message right after its own <c>ValidateAsync</c>. Reads fall under
/// the global <c>RequireAuthenticatedUser</c> fallback policy (Program.cs); every write additionally
/// requires <see cref="AdminPolicyName"/> - a stored template is code this service executes, so
/// writing one is privileged in the same way clearing sender state is on
/// <see cref="ServiceBusSendersController"/>, and more so.
/// </summary>
[ApiController]
[ApiVersion("1.0")]
[Route("api/v{version:apiVersion}/event-validation-templates")]
public sealed class EventValidationTemplatesController(
    IEventValidationTemplateService service, ILogger<EventValidationTemplatesController> logger)
    : ControllerBase
{
    /// <summary>
    /// Authorization policy name (Program.cs) gating every write on this controller - requires
    /// <see cref="AdminRoleName"/>. Named here, alongside what it protects, rather than as a bare
    /// string literal in Program.cs, so the two stay in sync - same pattern as
    /// <see cref="ServiceBusSendersController.AdminPolicyName"/>.
    /// </summary>
    public const string AdminPolicyName = "EventValidationTemplates.Admin";

    /// <summary>
    /// Role claim value <see cref="AdminPolicyName"/> requires - an Entra App Role that must be
    /// defined on the app registration backing <c>Authentication:Audience</c> and assigned to
    /// whichever principals may store executable validation code; see
    /// <see cref="ServiceBusSendersController.AdminRoleName"/> for the token-side mechanics.
    /// </summary>
    public const string AdminRoleName = "EventValidationTemplates.Admin";

    /// <summary>Lists every stored template's identity, optionally narrowed to one transport.</summary>
    /// <param name="transport">Transport to filter on (<c>Kafka</c> or <c>ServiceBus</c>), or omit for every template.</param>
    /// <param name="cancellationToken">Request abort token.</param>
    /// <returns><c>200 OK</c> with the matching templates - empty if none are stored.</returns>
    [HttpGet]
    [ProducesResponseType<IReadOnlyList<EventValidationTemplateSummary>>(StatusCodes.Status200OK)]
    public async Task<ActionResult<IReadOnlyList<EventValidationTemplateSummary>>> ListAsync(
        [FromQuery] string? transport, CancellationToken cancellationToken)
    {
        logger.LogDebug("GET validation templates, transport filter '{Transport}'.", transport);

        var templates = await service.ListAsync(transport, cancellationToken);

        return Ok(templates);
    }

    /// <summary>
    /// Returns worked, compile-verified examples of the script contract - one per authoring pattern,
    /// each showing how to use the objects injected into every script run (<c>x</c> the deserialized
    /// message, <c>header</c> the transport-neutral <c>HeaderLookup</c>, <c>_log</c> the consumer's
    /// logger, plus the <c>TryGetHeader</c>/<c>WellKnownHeaderNames</c> helpers) and how each outcome (return
    /// <c>true</c>/<c>false</c>, throw) is handled. Static documentation - the route is a literal
    /// segment, so it never collides with <see cref="GetAsync"/>'s two-segment template identity.
    /// </summary>
    /// <returns><c>200 OK</c> with the examples, simplest first.</returns>
    [HttpGet("examples")]
    [ProducesResponseType<IReadOnlyList<EventValidationTemplateExample>>(StatusCodes.Status200OK)]
    public ActionResult<IReadOnlyList<EventValidationTemplateExample>> GetExamples()
    {
        logger.LogDebug("GET validation template examples.");

        return Ok(service.GetExamples());
    }

    /// <summary>Returns one template, including its C# code.</summary>
    /// <param name="transport">Transport the template applies to (<c>Kafka</c> or <c>ServiceBus</c>).</param>
    /// <param name="identifier">Transport-specific lookup key the template applies to (Kafka <c>Type</c> header value, e.g. <c>inventory.InventoryStateChanged</c>, or Service Bus queue name).</param>
    /// <param name="cancellationToken">Request abort token.</param>
    /// <returns><c>200 OK</c> with the template, or <c>404 Not Found</c> if none is stored under that identity.</returns>
    [HttpGet("{transport}/{identifier}")]
    [ProducesResponseType<EventValidationTemplateResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<ActionResult<EventValidationTemplateResponse>> GetAsync(
        string transport, string identifier, CancellationToken cancellationToken)
    {
        logger.LogDebug("GET validation template {Transport}/{Identifier}.", transport, identifier);

        var template = await service.GetAsync(transport, identifier, cancellationToken);

        return template is null ? NotFound() : Ok(template);
    }

    /// <summary>Compiles and stores a new template - rejected if the code doesn't build or a template already exists under that identity.</summary>
    /// <param name="request">The template's identity and C# code.</param>
    /// <param name="cancellationToken">Request abort token.</param>
    /// <returns><c>201 Created</c> with a <c>Location</c> header pointing at <see cref="GetAsync"/>, <c>400 Bad Request</c> if the code doesn't compile, or <c>409 Conflict</c> if the template already exists.</returns>
    [HttpPost]
    [Authorize(Policy = AdminPolicyName)]
    [ProducesResponseType<EventValidationTemplateResponse>(StatusCodes.Status201Created)]
    [ProducesResponseType<ValidationProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status409Conflict)]
    public async Task<ActionResult<EventValidationTemplateResponse>> CreateAsync(
        CreateEventValidationTemplateRequest request, CancellationToken cancellationToken)
    {
        logger.LogDebug(
            "POST create validation template {Transport}/{Identifier} ({CodeLength} chars).",
            request.Transport, request.Identifier, request.Code.Length);

        var created = await service.CreateAsync(request, cancellationToken);

        // Location built by hand, not CreatedAtAction - see InventoryEventsController.CreateAsync on
        // the Asp.Versioning + CreatedAtAction link-generation friction this sidesteps.
        var version = RouteData.Values["version"];
        var location = $"{Request.PathBase}/api/v{version}/event-validation-templates/" +
                       $"{Uri.EscapeDataString(created.Transport)}/{Uri.EscapeDataString(created.Identifier)}";

        return Created(location, created);
    }

    /// <summary>Compiles and stores a replacement for an existing template's code.</summary>
    /// <param name="transport">Transport the template applies to.</param>
    /// <param name="identifier">Transport-specific lookup key the template applies to.</param>
    /// <param name="request">The replacement C# code.</param>
    /// <param name="cancellationToken">Request abort token.</param>
    /// <returns><c>200 OK</c> with the stored template, <c>400 Bad Request</c> if the code doesn't compile, or <c>404 Not Found</c> if no template exists under that identity.</returns>
    [HttpPut("{transport}/{identifier}")]
    [Authorize(Policy = AdminPolicyName)]
    [ProducesResponseType<EventValidationTemplateResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ValidationProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<ActionResult<EventValidationTemplateResponse>> UpdateAsync(
        string transport, string identifier, UpdateEventValidationTemplateRequest request, CancellationToken cancellationToken)
    {
        logger.LogDebug(
            "PUT update validation template {Transport}/{Identifier} ({CodeLength} chars).",
            transport, identifier, request.Code.Length);

        var updated = await service.UpdateAsync(transport, identifier, request, cancellationToken);

        return Ok(updated);
    }

    /// <summary>Deletes one template - the consumer stops applying it once its cached lookup expires.</summary>
    /// <param name="transport">Transport the template applies to.</param>
    /// <param name="identifier">Transport-specific lookup key the template applies to.</param>
    /// <param name="cancellationToken">Request abort token.</param>
    /// <returns><c>204 No Content</c> once deleted, or <c>404 Not Found</c> if no template exists under that identity.</returns>
    [HttpDelete("{transport}/{identifier}")]
    [Authorize(Policy = AdminPolicyName)]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> DeleteAsync(
        string transport, string identifier, CancellationToken cancellationToken)
    {
        logger.LogDebug("DELETE validation template {Transport}/{Identifier}.", transport, identifier);

        var deleted = await service.DeleteAsync(transport, identifier, cancellationToken);

        return deleted ? NoContent() : NotFound();
    }
}
