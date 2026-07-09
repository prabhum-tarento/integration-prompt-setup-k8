using FluentValidation;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.Exceptions;
using IIS.WMS.Consumer.Domain.Exceptions;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Mvc;

namespace IIS.WMS.Consumer.Api.ExceptionHandling;

/// <summary>
/// ASP.NET Core's built-in exception-handling extensibility point
/// (aspnet-rest-apis.instructions.md "Global exception handling"). A
/// <see cref="FluentValidation.ValidationException"/> reaching here means an Application-layer
/// invariant failed after the DTO's own shape validation already passed in
/// <see cref="Filters.ValidationFilter"/> - a different failure than that filter catches, not a
/// duplicate of it.
/// </summary>
public sealed class GlobalExceptionHandler(
    IProblemDetailsService problemDetailsService,
    ILogger<GlobalExceptionHandler> logger,
    ICorrelationContext correlationContext) : IExceptionHandler
{
    /// <summary>Maps the exception to a status code/title, logs at a severity matching that status, and writes the response as RFC 9457 Problem Details.</summary>
    /// <param name="httpContext">The current request's HTTP context.</param>
    /// <param name="exception">The unhandled exception.</param>
    /// <param name="cancellationToken">Request abort token.</param>
    /// <returns><see langword="true"/> once the Problem Details response has been written.</returns>
    public async ValueTask<bool> TryHandleAsync(
        HttpContext httpContext, Exception exception, CancellationToken cancellationToken)
    {
        var (status, title) = exception switch
        {
            ValidationException => (StatusCodes.Status400BadRequest, "Validation failed"),
            NotFoundException => (StatusCodes.Status404NotFound, "Resource not found"),
            InsufficientStockException => (StatusCodes.Status409Conflict, "Insufficient stock"),
            ConcurrencyException => (StatusCodes.Status409Conflict, "Concurrent modification"),
            _ => (StatusCodes.Status500InternalServerError, "An unexpected error occurred"),
        };

        // Log level follows the status family: a 409 from a concurrent update or a 404 for a
        // missing resource is expected client-facing behavior, not an operational problem -
        // logging it at Error would drown real 500s in noise on any error-rate alert.
        if (status >= StatusCodes.Status500InternalServerError)
        {
            logger.LogError(exception, "Unhandled exception. CorrelationId: {CorrelationId}", correlationContext.CorrelationId);
        }
        else
        {
            logger.LogWarning(exception, "Request failed with {StatusCode}. CorrelationId: {CorrelationId}", status, correlationContext.CorrelationId);
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
