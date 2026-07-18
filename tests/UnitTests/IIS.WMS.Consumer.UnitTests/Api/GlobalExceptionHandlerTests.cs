using FluentValidation;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Common.DynamicValidation;
using IIS.WMS.Common.Exceptions;
using IIS.WMS.Consumer.Api.ExceptionHandling;
using IIS.WMS.Consumer.Application.Exceptions;
using IIS.WMS.Consumer.Domain.Exceptions;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Api;

/// <summary>
/// Maps every exception type <see cref="GlobalExceptionHandler"/> switches on to its documented
/// status code/title (aspnet-rest-apis.instructions.md "Global exception handling"), and verifies the
/// severity-by-status-family logging rule and the correlation id extension.
/// </summary>
public class GlobalExceptionHandlerTests
{
    private sealed class RecordingLogger : ILogger<GlobalExceptionHandler>
    {
        public List<LogLevel> Levels { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Levels.Add(logLevel);
    }

    private readonly RecordingLogger logger = new();
    private readonly IProblemDetailsService problemDetailsService = Substitute.For<IProblemDetailsService>();
    private readonly GlobalExceptionHandler sut;

    public GlobalExceptionHandlerTests()
    {
        sut = new GlobalExceptionHandler(problemDetailsService, logger);
        problemDetailsService.TryWriteAsync(Arg.Any<ProblemDetailsContext>()).Returns(true);
    }

    private static HttpContext CreateHttpContext(string correlationId)
    {
        var correlationContext = new CorrelationContext();
        correlationContext.Set(correlationId);

        var services = new ServiceCollection();
        services.AddSingleton<ICorrelationContext>(correlationContext);

        return new DefaultHttpContext
        {
            RequestServices = services.BuildServiceProvider(),
        };
    }

    public static TheoryData<Exception, int, string> ExceptionMappings()
    {
        var data = new TheoryData<Exception, int, string>
        {
            { new ValidationException("invalid"), StatusCodes.Status400BadRequest, "Validation failed" },
            { new NotFoundException("InventoryEvent", "WH1:SKU1"), StatusCodes.Status404NotFound, "Resource not found" },
            { new ConflictException("InventoryEvent", "WH1:SKU1"), StatusCodes.Status409Conflict, "Resource already exists" },
            { new InsufficientStockException("WH1", "SKU1", 10, 5), StatusCodes.Status409Conflict, "Insufficient stock" },
            { new ConcurrencyException("WH1:SKU1", "etag-1"), StatusCodes.Status409Conflict, "Concurrent modification" },
            { new InvalidOperationException("boom"), StatusCodes.Status500InternalServerError, "An unexpected error occurred" },
        };

        return data;
    }

    [Theory(DisplayName = "TryHandleAsync maps each exception type to its documented status code and title")]
    [MemberData(nameof(ExceptionMappings))]
    public async Task TryHandleAsync_MappedExceptionType_SetsExpectedStatusAndTitle(
        Exception exception, int expectedStatus, string expectedTitle)
    {
        var httpContext = CreateHttpContext("11111111-1111-1111-1111-111111111111");

        var handled = await sut.TryHandleAsync(httpContext, exception, CancellationToken.None);

        Assert.True(handled);
        Assert.Equal(expectedStatus, httpContext.Response.StatusCode);

        await problemDetailsService.Received(1).TryWriteAsync(Arg.Is<ProblemDetailsContext>(ctx =>
            ctx.ProblemDetails.Status == expectedStatus &&
            ctx.ProblemDetails.Title == expectedTitle &&
            (string?)ctx.ProblemDetails.Extensions["correlationId"] == "11111111-1111-1111-1111-111111111111" &&
            ctx.Exception == exception));
    }

    [Theory(DisplayName = "TryHandleAsync logs Warning for a client-facing (< 500) status and Error for a 500")]
    [MemberData(nameof(ExceptionMappings))]
    public async Task TryHandleAsync_MappedExceptionType_LogsAtSeverityMatchingStatusFamily(
        Exception exception, int expectedStatus, string _)
    {
        var httpContext = CreateHttpContext("22222222-2222-2222-2222-222222222222");

        await sut.TryHandleAsync(httpContext, exception, CancellationToken.None);

        var expectedLevel = expectedStatus >= StatusCodes.Status500InternalServerError ? LogLevel.Error : LogLevel.Warning;
        Assert.Equal(expectedLevel, Assert.Single(logger.Levels));
    }

    [Fact(DisplayName = "TryHandleAsync adds a TemplateCompilationException's compiler errors as a response extension")]
    public async Task TryHandleAsync_TemplateCompilationException_AddsErrorsExtension()
    {
        var httpContext = CreateHttpContext("33333333-3333-3333-3333-333333333333");
        var exception = new TemplateCompilationException("Schema/template-1", ["CS1002: expected ;", "CS0103: unknown name"]);

        var handled = await sut.TryHandleAsync(httpContext, exception, CancellationToken.None);

        Assert.True(handled);
        Assert.Equal(StatusCodes.Status400BadRequest, httpContext.Response.StatusCode);

        await problemDetailsService.Received(1).TryWriteAsync(Arg.Is<ProblemDetailsContext>(ctx =>
            ctx.ProblemDetails.Title == "Validation template compilation failed" &&
            ((IReadOnlyList<string>)ctx.ProblemDetails.Extensions["errors"]!).Count == 2));
    }

    [Fact(DisplayName = "TryHandleAsync does not add an errors extension for exception types other than TemplateCompilationException")]
    public async Task TryHandleAsync_NonTemplateCompilationException_OmitsErrorsExtension()
    {
        var httpContext = CreateHttpContext("44444444-4444-4444-4444-444444444444");

        await sut.TryHandleAsync(httpContext, new NotFoundException("InventoryEvent", "WH1:SKU1"), CancellationToken.None);

        await problemDetailsService.Received(1).TryWriteAsync(Arg.Is<ProblemDetailsContext>(ctx =>
            !ctx.ProblemDetails.Extensions.ContainsKey("errors")));
    }

    [Fact(DisplayName = "TryHandleAsync resolves ICorrelationContext from the current request's RequestServices, not the constructor")]
    public async Task TryHandleAsync_ResolvesCorrelationContextFromRequestServices_NotConstructor()
    {
        var httpContext = CreateHttpContext("55555555-5555-5555-5555-555555555555");

        var handled = await sut.TryHandleAsync(httpContext, new InvalidOperationException(), CancellationToken.None);

        Assert.True(handled);
        await problemDetailsService.Received(1).TryWriteAsync(Arg.Is<ProblemDetailsContext>(ctx =>
            (string?)ctx.ProblemDetails.Extensions["correlationId"] == "55555555-5555-5555-5555-555555555555"));
    }
}
