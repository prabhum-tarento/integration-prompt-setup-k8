---
description: 'ASP.NET Core Web API layer rules for this inventory integration service: routing, versioning, validation, global exception handling, correlation IDs, health checks.'
applyTo: '**/*.cs'
---

# ASP.NET Core Web API — Api Layer

This file owns the **Api layer only** — the composition root and HTTP
surface. Data access lives in
[cosmos-db.instructions.md](cosmos-db.instructions.md), architecture/DDD in
[dotnet-architecture-good-practices.instructions.md](dotnet-architecture-good-practices.instructions.md),
messaging/resiliency in
[integration-resiliency.instructions.md](integration-resiliency.instructions.md).
Don't restate those here.

## Controllers, not Minimal APIs

This service uses **attribute-routed controllers**, not Minimal APIs. This
is a settled decision for this repo, not a per-task choice — do not
introduce Minimal API endpoints alongside controllers.

Rationale: the API versioning tooling (`Asp.Versioning.Mvc`), typed client
generation for downstream consumers, and the filter pipeline used for
validation and correlation ID propagation are all built around the
controller/filter model in this codebase.

```csharp
[ApiController]
[ApiVersion("1.0")]
[Route("api/v{version:apiVersion}/inventory-events")]
public sealed class InventoryEventsController(IInventoryEventService service) : ControllerBase
{
    [HttpGet("{id}")]
    [ProducesResponseType<InventoryEventResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<ActionResult<InventoryEventResponse>> GetAsync(
        string id, CancellationToken cancellationToken)
    {
        var result = await service.GetAsync(id, cancellationToken);
        return result is null ? NotFound() : Ok(result);
    }
}
```

## API versioning registration

`Asp.Versioning.Mvc` needs explicit registration in `Program.cs` — the
`[ApiVersion]`/`[Route("api/v{version:apiVersion}/...")]` attributes above
do nothing without it:

```csharp
builder.Services.AddApiVersioning(options =>
{
    options.DefaultApiVersion = new ApiVersion(1.0);
    options.AssumeDefaultVersionWhenUnspecified = true;
    options.ReportApiVersions = true;
}).AddMvc().AddApiExplorer(options =>
{
    options.GroupNameFormat = "'v'VVV";
    options.SubstituteApiVersionInUrl = true;
});
```

`AddApiExplorer` is what lets the OpenAPI generator (see API documentation
below) group endpoints by version instead of listing every version's routes
under one undifferentiated document.

## Resource design

* Plural noun resources: `/api/v1/inventory-events`, `/api/v1/allocations`.
* Version in the URL path, mandatory from the first endpoint.
* Use standard HTTP verbs and status codes; a `200 OK` is never returned for
  a failed operation (see Problem Details below).
* For read operations that need a request body (complex filters over a
  large key set), expose `POST /api/v{version}/{resource}/search` — see
  [engineering-standards.instructions.md](engineering-standards.instructions.md) §4
  for why this stands in for the still-draft HTTP `QUERY` method.

## Validation

* FluentValidation validators for every request DTO, registered via DI and
  run through an action filter (`ValidationFilter`) before the action
  executes — controllers never call `Validate()` manually.
* A failed validation returns `400 Bad Request` as RFC 9457 Problem Details
  with a `errors` extension listing field-level failures.
* **`[ApiController]`'s automatic model-state validation must be disabled**
  for `ValidationFilter` to be the thing that actually runs:

  ```csharp
  builder.Services.Configure<ApiBehaviorOptions>(options =>
      options.SuppressModelStateInvalidFilter = true);
  ```

  Without this, `[ApiController]` short-circuits on an invalid `ModelState`
  and returns its own default `400` Problem Details *before* any action
  filter executes — `ValidationFilter` would never run, and the response
  shape (no `errors` extension in the format this doc specifies) wouldn't
  match what's documented above. Set this once in `Program.cs`, not
  per-controller.

## Global exception handling

Implement `IExceptionHandler` (ASP.NET Core's built-in exception-handling
extensibility point) rather than custom try/catch middleware:

```csharp
public sealed class GlobalExceptionHandler(
    IProblemDetailsService problemDetailsService,
    ILogger<GlobalExceptionHandler> logger) : IExceptionHandler
{
    public async ValueTask<bool> TryHandleAsync(
        HttpContext httpContext, Exception exception, CancellationToken cancellationToken)
    {
        // AddExceptionHandler<T>() always registers IExceptionHandler as a singleton, but
        // ICorrelationContext is (correctly) scoped per request — constructor-injecting it
        // instead of resolving it here throws "Cannot consume scoped service ... from
        // singleton" the moment DI validates scopes (Development), and would silently
        // capture one request's instance forever in an environment that doesn't validate
        // (Production). Resolve it from the current request's RequestServices instead.
        var correlationContext = httpContext.RequestServices.GetRequiredService<ICorrelationContext>();

        // Note: request-DTO shape/field validation is rejected earlier by
        // ValidationFilter and never reaches here. A ValidationException
        // caught here means an Application-layer invariant failed *after*
        // the DTO itself passed shape validation (e.g. a cross-field or
        // state-dependent business rule) — a different failure than the
        // filter catches, not a duplicate of it.
        var (status, title) = exception switch
        {
            ValidationException => (StatusCodes.Status400BadRequest, "Validation failed"),
            NotFoundException => (StatusCodes.Status404NotFound, "Resource not found"),
            InsufficientStockException => (StatusCodes.Status409Conflict, "Insufficient stock"),
            ConcurrencyException => (StatusCodes.Status409Conflict, "Concurrent modification"),
            _ => (StatusCodes.Status500InternalServerError, "An unexpected error occurred"),
        };

        // Log level follows the status family, not a blanket LogError: a
        // 409 from a concurrent update or a 404 for a missing resource is
        // expected client-facing behavior, not an operational problem — 
        // logging it at Error severity would drown real 500s in noise on
        // any error-rate dashboard or alert.
        if (status >= StatusCodes.Status500InternalServerError)
        {
            logger.LogError(exception, "Unhandled exception. CorrelationId: {CorrelationId}",
                correlationContext.CorrelationId);
        }
        else
        {
            logger.LogWarning(exception, "Request failed with {StatusCode}. CorrelationId: {CorrelationId}",
                status, correlationContext.CorrelationId);
        }

        httpContext.Response.StatusCode = status;

        return await problemDetailsService.TryWriteAsync(new ProblemDetailsContext
        {
            HttpContext = httpContext,
            Exception = exception,
            ProblemDetails = new ProblemDetails
            {
                Status = status,
                Title = title,
                Extensions = { ["correlationId"] = correlationContext.CorrelationId },
            },
        });
    }
}
```

Rules:

* Register `builder.Services.AddExceptionHandler<GlobalExceptionHandler>()`
  and `builder.Services.AddProblemDetails()` in `Program.cs`, **and** call
  `app.UseExceptionHandler()` in the middleware pipeline — the DI
  registration alone does nothing without the middleware; this is the most
  common way this pattern silently fails to trigger.
* Pipeline order matters: `CorrelationIdMiddleware` runs *before*
  `UseExceptionHandler()`, so the ID exists before any exception can be
  thrown and the handler above can read it from `ICorrelationContext`.
* **Never constructor-inject a Scoped service into `GlobalExceptionHandler`
  (or any other `IExceptionHandler`)** — `AddExceptionHandler<T>()` always
  registers it as a Singleton, so a Scoped dependency like
  `ICorrelationContext` must be resolved from `httpContext.RequestServices`
  inside `TryHandleAsync` instead, exactly as the example above does. This
  is the same "resolve from the per-call scope you do have access to,
  don't inject a scoped service into a singleton's constructor" fix that
  applies to any singleton with a scoped collaborator.
* Routing `IProblemDetailsService.TryWriteAsync` through the registered
  `AddProblemDetails()` pipeline (instead of hand-writing JSON) is what
  actually produces the `application/problem+json` content type RFC 9457
  requires and honors any `CustomizeProblemDetails` hook configured
  elsewhere — writing the response manually would silently drop both.
* Never return a stack trace or exception message text to the client — log
  the detail, return the title and correlation ID only.
* Every mapped exception type is a Domain or Application exception; a raw
  framework/SDK exception reaching this handler and falling into the
  `_ =>` branch is a signal that a lower layer is missing a translation —
  treat repeated 500s from one code path as a bug in that path, not as
  "the handler is doing its job."

## Correlation ID

* `CorrelationIdMiddleware` runs first in the pipeline: reads an inbound
  `X-Correlation-Id` header if present, otherwise generates a new GUID.
* **Validate the inbound value before trusting it** — it's client-supplied
  and flows unmodified into structured logs and trace tags. Without this
  check, an attacker-controlled header value lands directly in Serilog's
  `LogContext` and OpenTelemetry tags — a log-injection and
  log-storage-bloat vector for something that's supposed to be an opaque
  trace ID:

  ```csharp
  var correlationId = context.Request.Headers.TryGetValue("X-Correlation-Id", out var header)
                       && Guid.TryParse(header, out var parsed)
      ? parsed.ToString()
      : Guid.NewGuid().ToString();
  ```

  Only accept a value that parses as a `Guid`; anything else (missing,
  malformed, oversized) is discarded and replaced with a freshly generated
  one rather than passed through.
* Pushes the (validated) ID into an `ICorrelationContext` (scoped service),
  into the Serilog `LogContext`, and as a tag on `Activity.Current` for
  OpenTelemetry.
* Echoes the ID back on the response `X-Correlation-Id` header.
* When this request triggers a Service Bus publish (directly, or via a
  domain event handled later), the same ID is attached as a message
  property so it survives the Kafka → Service Bus → Cosmos DB hop — see
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md).

## Health checks

This process (the Api) is one of three separately-deployed workloads — see
[kubernetes-deployment-best-practices.instructions.md](kubernetes-deployment-best-practices.instructions.md)
for the other two (Kafka consumer, Service Bus consumer), each its own
Deployment/Pod. A Pod's health endpoint can only report on **that
process's own** dependencies — it can't observe another Pod's state, so
each workload registers its own checks rather than sharing one endpoint:

* `/health/live`: process is up — no dependency checks. Used for the
  Kubernetes liveness probe.
* `/health/ready`: for the Api, checks Cosmos DB connectivity and, only for
  the specific endpoints that publish onto Service Bus synchronously (e.g.
  a manual stock-adjustment endpoint), Service Bus connectivity. It does
  **not** check Kafka consumer staleness — that check lives on the Kafka
  consumer's own `/health/ready`, exposed from that Pod, per
  [integration-resiliency.instructions.md](integration-resiliency.instructions.md) §8.
* Registered via `Microsoft.Extensions.Diagnostics.HealthChecks` with named
  checks per dependency so a failing check identifies which dependency is
  down, not just "unhealthy."
* **Both health endpoints must be reachable without authentication.**
  [engineering-standards.instructions.md](engineering-standards.instructions.md) §6
  mandates JWT Bearer auth globally; map the health endpoints with
  `.AllowAnonymous()` explicitly:

  ```csharp
  app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = _ => false })
      .AllowAnonymous();
  app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = _ => true })
      .AllowAnonymous();
  ```

  The AKS kubelet calling these probes has no bearer token — without
  `.AllowAnonymous()`, adding global auth middleware silently turns every
  liveness/readiness check into a `401`, and the Pod gets killed/removed
  from rotation for a reason that has nothing to do with its actual health.

## API documentation

* OpenAPI 3.1 via the built-in `Microsoft.AspNetCore.OpenApi` generator.
* `Scalar.AspNetCore` for the interactive UI in non-production environments
  only — do not expose it in production.
* Every controller action has `[ProducesResponseType]` for each realistic
  status code (success, validation failure, not found, conflict).

## Testing this layer

Unit-test controllers only for request/response mapping and status-code
selection (mock `IInventoryEventService`); everything else is Application
or Domain logic tested at that layer. End-to-end coverage — real
middleware pipeline, real Problem Details shape, real correlation ID
propagation — belongs in integration tests using
`WebApplicationFactory<Program>`; see
[integration-resiliency.instructions.md](integration-resiliency.instructions.md)
for the full test-project setup.
