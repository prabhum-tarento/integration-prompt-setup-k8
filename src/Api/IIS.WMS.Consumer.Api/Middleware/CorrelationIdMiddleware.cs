using System.Diagnostics;
using IIS.WMS.Common.Correlation;
using Microsoft.Extensions.Logging;
using Serilog.Context;

namespace IIS.WMS.Consumer.Api.Middleware;

/// <summary>
/// Runs first in the pipeline (aspnet-rest-apis.instructions.md "Correlation ID"): reads an
/// inbound <c>X-Correlation-Id</c> header if present and valid, otherwise generates a new one.
/// Must run before <c>UseExceptionHandler()</c> so the id exists before any exception can be
/// thrown.
/// </summary>
public sealed class CorrelationIdMiddleware(RequestDelegate next, ILogger<CorrelationIdMiddleware> logger)
{
    private const string HeaderName = "X-Correlation-Id";

    /// <summary>Resolves the correlation id for this request, pushes it into <see cref="ICorrelationContext"/>/Serilog's <c>LogContext</c>/the current <see cref="Activity"/>, and echoes it on the response.</summary>
    /// <param name="context">The current request's HTTP context.</param>
    /// <param name="correlationContext">Scoped service the resolved id is written into for the rest of the request.</param>
    public async Task InvokeAsync(HttpContext context, ICorrelationContext correlationContext)
    {
        var hasHeader = context.Request.Headers.TryGetValue(HeaderName, out var header);

        // Client-supplied and flows unmodified into structured logs and trace tags - only accept
        // a value that parses as a Guid; anything else is discarded and replaced, closing off a
        // log-injection / log-storage-bloat vector for what's supposed to be an opaque trace id.
        string correlationId;

        if (hasHeader && Guid.TryParse(header, out var parsed))
        {
            correlationId = parsed.ToString();
            logger.LogDebug("Using inbound correlation id {CorrelationId}.", correlationId);
        }
        else
        {
            correlationId = Guid.NewGuid().ToString();

            if (hasHeader)
            {
                // A header was present but didn't parse as a Guid - worth flagging, since it's
                // either a misbehaving client or something probing the header for injection.
                logger.LogWarning(
                    "Rejected malformed inbound {HeaderName} value; generated {CorrelationId} instead.",
                    HeaderName, correlationId);
            }
            else
            {
                logger.LogDebug("No inbound correlation id - generated {CorrelationId}.", correlationId);
            }
        }

        correlationContext.Set(correlationId);
        Activity.Current?.SetTag("correlation.id", correlationId);

        using (LogContext.PushProperty("CorrelationId", correlationId))
        {
            context.Response.OnStarting(() =>
            {
                context.Response.Headers[HeaderName] = correlationId;

                return Task.CompletedTask;
            });

            await next(context);
        }
    }
}
