using FluentValidation;
using FluentValidation.Results;
using IIS.WMS.Consumer.Api.Filters;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Abstractions;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Api;

/// <summary>
/// Tests <see cref="ValidationFilter"/> (aspnet-rest-apis.instructions.md "Validation") - the action
/// filter that stands in for <c>[ApiController]</c>'s suppressed automatic model-state validation.
/// </summary>
public class ValidationFilterTests
{
    public sealed record ValidatedDto(string Name);

    public sealed record UnvalidatedDto(string Value);

    private static ActionExecutingContext CreateContext(
        IServiceProvider serviceProvider, IDictionary<string, object?> actionArguments)
    {
        var httpContext = new DefaultHttpContext { RequestServices = serviceProvider };
        var actionContext = new ActionContext(httpContext, new RouteData(), new ActionDescriptor());

        return new ActionExecutingContext(
            actionContext,
            filters: [],
            actionArguments: actionArguments,
            controller: new object());
    }

    private static ValidationFilter CreateSut(IServiceProvider serviceProvider) =>
        new(serviceProvider, Substitute.For<ILogger<ValidationFilter>>());

    [Fact(DisplayName = "OnActionExecutionAsync calls next() without setting Result when the registered validator passes")]
    public async Task OnActionExecutionAsync_ValidatorPasses_CallsNextAndLeavesResultNull()
    {
        var validator = Substitute.For<IValidator<ValidatedDto>>();
        validator.ValidateAsync(Arg.Any<IValidationContext>(), Arg.Any<CancellationToken>())
            .Returns(new ValidationResult());

        var services = new ServiceCollection();
        services.AddSingleton(validator);
        var sut = CreateSut(services.BuildServiceProvider());

        var context = CreateContext(services.BuildServiceProvider(), new Dictionary<string, object?> { ["dto"] = new ValidatedDto("ok") });
        var nextCalled = false;

        await sut.OnActionExecutionAsync(context, () =>
        {
            nextCalled = true;
            return Task.FromResult(new ActionExecutedContext(context, [], context.Controller));
        });

        Assert.True(nextCalled);
        Assert.Null(context.Result);
    }

    [Fact(DisplayName = "OnActionExecutionAsync short-circuits with a 400 ValidationProblemDetails when the registered validator fails")]
    public async Task OnActionExecutionAsync_ValidatorFails_ShortCircuitsWithBadRequestProblemDetails()
    {
        var failures = new List<ValidationFailure> { new("Name", "Name is required") };
        var validator = Substitute.For<IValidator<ValidatedDto>>();
        validator.ValidateAsync(Arg.Any<IValidationContext>(), Arg.Any<CancellationToken>())
            .Returns(new ValidationResult(failures));

        var services = new ServiceCollection();
        services.AddSingleton(validator);
        var sut = CreateSut(services.BuildServiceProvider());

        var context = CreateContext(services.BuildServiceProvider(), new Dictionary<string, object?> { ["dto"] = new ValidatedDto("") });
        var nextCalled = false;

        await sut.OnActionExecutionAsync(context, () =>
        {
            nextCalled = true;
            return Task.FromResult(new ActionExecutedContext(context, [], context.Controller));
        });

        Assert.False(nextCalled);
        var badRequest = Assert.IsType<BadRequestObjectResult>(context.Result);
        var problemDetails = Assert.IsType<ValidationProblemDetails>(badRequest.Value);
        Assert.Equal(StatusCodes.Status400BadRequest, problemDetails.Status);
        Assert.Equal("Validation failed", problemDetails.Title);
        Assert.Contains("Name", problemDetails.Errors.Keys);
    }

    [Fact(DisplayName = "OnActionExecutionAsync calls next() when an argument has no registered validator")]
    public async Task OnActionExecutionAsync_ArgumentWithoutRegisteredValidator_CallsNext()
    {
        var services = new ServiceCollection();
        var sut = CreateSut(services.BuildServiceProvider());

        var context = CreateContext(services.BuildServiceProvider(), new Dictionary<string, object?> { ["dto"] = new UnvalidatedDto("anything") });
        var nextCalled = false;

        await sut.OnActionExecutionAsync(context, () =>
        {
            nextCalled = true;
            return Task.FromResult(new ActionExecutedContext(context, [], context.Controller));
        });

        Assert.True(nextCalled);
        Assert.Null(context.Result);
    }

    [Fact(DisplayName = "OnActionExecutionAsync skips a null action argument and calls next()")]
    public async Task OnActionExecutionAsync_NullActionArgument_SkipsAndCallsNext()
    {
        var services = new ServiceCollection();
        var sut = CreateSut(services.BuildServiceProvider());

        var context = CreateContext(services.BuildServiceProvider(), new Dictionary<string, object?> { ["dto"] = null });
        var nextCalled = false;

        await sut.OnActionExecutionAsync(context, () =>
        {
            nextCalled = true;
            return Task.FromResult(new ActionExecutedContext(context, [], context.Controller));
        });

        Assert.True(nextCalled);
        Assert.Null(context.Result);
    }

    [Fact(DisplayName = "OnActionExecutionAsync validates a valid earlier argument before short-circuiting on a later invalid one")]
    public async Task OnActionExecutionAsync_MultipleArguments_ValidatesEachUntilFirstFailure()
    {
        var passingValidator = Substitute.For<IValidator<UnvalidatedDto>>();
        passingValidator.ValidateAsync(Arg.Any<IValidationContext>(), Arg.Any<CancellationToken>())
            .Returns(new ValidationResult());

        var failingValidator = Substitute.For<IValidator<ValidatedDto>>();
        failingValidator.ValidateAsync(Arg.Any<IValidationContext>(), Arg.Any<CancellationToken>())
            .Returns(new ValidationResult([new ValidationFailure("Name", "Name is required")]));

        var services = new ServiceCollection();
        services.AddSingleton(passingValidator);
        services.AddSingleton(failingValidator);
        var sut = CreateSut(services.BuildServiceProvider());

        var context = CreateContext(
            services.BuildServiceProvider(),
            new Dictionary<string, object?>
            {
                ["first"] = new UnvalidatedDto("ok"),
                ["second"] = new ValidatedDto(""),
            });
        var nextCalled = false;

        await sut.OnActionExecutionAsync(context, () =>
        {
            nextCalled = true;
            return Task.FromResult(new ActionExecutedContext(context, [], context.Controller));
        });

        Assert.False(nextCalled);
        await passingValidator.Received(1).ValidateAsync(Arg.Any<IValidationContext>(), Arg.Any<CancellationToken>());
        Assert.IsType<BadRequestObjectResult>(context.Result);
    }
}
