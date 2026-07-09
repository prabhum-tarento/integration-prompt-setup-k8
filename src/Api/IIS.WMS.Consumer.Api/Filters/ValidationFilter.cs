using FluentValidation;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Api.Filters;

/// <summary>
/// Runs every registered <see cref="IValidator{T}"/> against the matching action argument before
/// the action executes (aspnet-rest-apis.instructions.md "Validation"). Requires
/// <c>ApiBehaviorOptions.SuppressModelStateInvalidFilter = true</c> in <c>Program.cs</c> -
/// otherwise <c>[ApiController]</c>'s automatic model-state check short-circuits first and this
/// filter never runs.
/// </summary>
public sealed class ValidationFilter(IServiceProvider serviceProvider, ILogger<ValidationFilter> logger)
    : IAsyncActionFilter
{
    /// <summary>Validates every non-null action argument that has a registered validator, short-circuiting with <c>400 Bad Request</c> on the first failure.</summary>
    /// <param name="context">The action-executing context, whose <see cref="ActionExecutingContext.Result"/> is set to short-circuit on a validation failure.</param>
    /// <param name="next">Delegate that continues the filter pipeline when validation passes.</param>
    public async Task OnActionExecutionAsync(ActionExecutingContext context, ActionExecutionDelegate next)
    {
        foreach (var argument in context.ActionArguments.Values)
        {
            if (argument is null)
            {
                continue;
            }

            var validatorType = typeof(IValidator<>).MakeGenericType(argument.GetType());

            if (serviceProvider.GetService(validatorType) is not IValidator validator)
            {
                continue;
            }

            var validationContext = new ValidationContext<object>(argument);
            var result = await validator.ValidateAsync(validationContext, context.HttpContext.RequestAborted);

            if (!result.IsValid)
            {
                // A client validation failure is routine, expected input handling, not an
                // operational problem - Debug keeps it out of the way of real warnings/errors.
                logger.LogDebug(
                    "Validation failed for {ArgumentType}: {ErrorCount} error(s).",
                    argument.GetType().Name, result.Errors.Count);

                var problemDetails = new ValidationProblemDetails(
                    result.ToDictionary())
                {
                    Status = StatusCodes.Status400BadRequest,
                    Title = "Validation failed",
                };

                context.Result = new BadRequestObjectResult(problemDetails);

                return;
            }
        }

        await next();
    }
}
