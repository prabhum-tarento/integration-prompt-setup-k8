using System.Diagnostics;
using IIS.WMS.Common.Correlation;
using IIS.WMS.Consumer.Api.Middleware;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Core;
using Serilog.Events;

namespace IIS.WMS.Consumer.UnitTests.Api;

/// <summary>
/// Tests <see cref="CorrelationIdMiddleware"/> (aspnet-rest-apis.instructions.md "Correlation ID")
/// against a bare <see cref="DefaultHttpContext"/> and a fake <see cref="RequestDelegate"/> - no
/// <c>TestServer</c>. A hand-rolled <see cref="RecordingResponseFeature"/> stands in for the response
/// feature a real server would provide, since <c>DefaultHttpContext</c>'s own default feature has no
/// way to fire its registered <c>OnStarting</c> callbacks outside a real server pipeline.
/// </summary>
public class CorrelationIdMiddlewareTests
{
    private sealed class RecordingLogger : ILogger<CorrelationIdMiddleware>
    {
        public List<LogLevel> Levels { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Levels.Add(logLevel);
    }

    private sealed class RecordingResponseFeature : IHttpResponseFeature
    {
        private readonly List<(Func<object, Task> Callback, object State)> onStarting = [];

        public int StatusCode { get; set; } = StatusCodes.Status200OK;

        public string? ReasonPhrase { get; set; }

        public IHeaderDictionary Headers { get; set; } = new HeaderDictionary();

        public Stream Body { get; set; } = Stream.Null;

        public bool HasStarted => false;

        public void OnStarting(Func<object, Task> callback, object state) => onStarting.Add((callback, state));

        public void OnCompleted(Func<object, Task> callback, object state)
        {
        }

        public async Task FireOnStartingAsync()
        {
            foreach (var (callback, state) in onStarting)
            {
                await callback(state);
            }
        }
    }

    private sealed class DelegatingSink(Action<LogEvent> onEmit) : ILogEventSink
    {
        public void Emit(LogEvent logEvent) => onEmit(logEvent);
    }

    private static (DefaultHttpContext Context, RecordingResponseFeature ResponseFeature) CreateContext()
    {
        var context = new DefaultHttpContext();
        var responseFeature = new RecordingResponseFeature();
        context.Features.Set<IHttpResponseFeature>(responseFeature);
        return (context, responseFeature);
    }

    private static CorrelationIdMiddleware CreateSut(RequestDelegate next, RecordingLogger? logger = null) =>
        new(next, logger ?? new RecordingLogger());

    [Fact(DisplayName = "InvokeAsync generates a new correlation id when no inbound header is present")]
    public async Task InvokeAsync_NoInboundHeader_GeneratesNewGuidCorrelationId()
    {
        var (context, _) = CreateContext();
        var correlationContext = new CorrelationContext();
        var sut = CreateSut(_ => Task.CompletedTask);

        await sut.InvokeAsync(context, correlationContext);

        Assert.True(Guid.TryParse(correlationContext.CorrelationId, out _));
    }

    [Fact(DisplayName = "InvokeAsync honors a well-formed inbound X-Correlation-Id header verbatim")]
    public async Task InvokeAsync_ValidInboundHeader_HonorsValueVerbatim()
    {
        const string inboundId = "11111111-1111-1111-1111-111111111111";
        var (context, _) = CreateContext();
        context.Request.Headers["X-Correlation-Id"] = inboundId;
        var correlationContext = new CorrelationContext();
        var sut = CreateSut(_ => Task.CompletedTask);

        await sut.InvokeAsync(context, correlationContext);

        Assert.Equal(inboundId, correlationContext.CorrelationId);
    }

    [Fact(DisplayName = "InvokeAsync discards a malformed inbound header and generates a new id, logging a warning")]
    public async Task InvokeAsync_MalformedInboundHeader_GeneratesNewIdAndLogsWarning()
    {
        var (context, _) = CreateContext();
        context.Request.Headers["X-Correlation-Id"] = "not-a-guid";
        var correlationContext = new CorrelationContext();
        var logger = new RecordingLogger();
        var sut = CreateSut(_ => Task.CompletedTask, logger);

        await sut.InvokeAsync(context, correlationContext);

        Assert.True(Guid.TryParse(correlationContext.CorrelationId, out _));
        Assert.NotEqual("not-a-guid", correlationContext.CorrelationId);
        Assert.Contains(LogLevel.Warning, logger.Levels);
    }

    [Fact(DisplayName = "InvokeAsync logs at Debug (not Warning) when no inbound header is present at all")]
    public async Task InvokeAsync_NoInboundHeader_LogsDebugNotWarning()
    {
        var (context, _) = CreateContext();
        var correlationContext = new CorrelationContext();
        var logger = new RecordingLogger();
        var sut = CreateSut(_ => Task.CompletedTask, logger);

        await sut.InvokeAsync(context, correlationContext);

        Assert.DoesNotContain(LogLevel.Warning, logger.Levels);
        Assert.Contains(LogLevel.Debug, logger.Levels);
    }

    [Fact(DisplayName = "InvokeAsync echoes the resolved correlation id back on the response header once the response starts")]
    public async Task InvokeAsync_ResponseStarting_EchoesCorrelationIdHeader()
    {
        var (context, responseFeature) = CreateContext();
        var correlationContext = new CorrelationContext();
        var sut = CreateSut(_ => Task.CompletedTask);

        await sut.InvokeAsync(context, correlationContext);
        await responseFeature.FireOnStartingAsync();

        Assert.Equal(correlationContext.CorrelationId, responseFeature.Headers["X-Correlation-Id"].ToString());
    }

    [Fact(DisplayName = "InvokeAsync sets the correlation id as a tag on the current Activity")]
    public async Task InvokeAsync_CurrentActivity_SetsCorrelationIdTag()
    {
        using var activity = new Activity("test-activity");
        activity.Start();
        try
        {
            var (context, _) = CreateContext();
            var correlationContext = new CorrelationContext();
            var sut = CreateSut(_ => Task.CompletedTask);

            await sut.InvokeAsync(context, correlationContext);

            Assert.Equal(correlationContext.CorrelationId, activity.GetTagItem("correlation.id"));
        }
        finally
        {
            activity.Stop();
        }
    }

    [Fact(DisplayName = "InvokeAsync pushes the correlation id into Serilog's LogContext for the duration of next()")]
    public async Task InvokeAsync_DuringNext_PushesCorrelationIdIntoSerilogLogContext()
    {
        var (context, _) = CreateContext();
        var correlationContext = new CorrelationContext();
        var capturedEvents = new List<LogEvent>();
        var serilogLogger = new LoggerConfiguration()
            .Enrich.FromLogContext()
            .WriteTo.Sink(new DelegatingSink(capturedEvents.Add))
            .CreateLogger();

        var sut = CreateSut(_ =>
        {
            serilogLogger.Information("inside next()");
            return Task.CompletedTask;
        });

        await sut.InvokeAsync(context, correlationContext);

        var logged = Assert.Single(capturedEvents);
        Assert.True(logged.Properties.TryGetValue("CorrelationId", out var value));
        Assert.Equal(correlationContext.CorrelationId, value!.ToString().Trim('"'));
    }

    [Fact(DisplayName = "InvokeAsync calls the next delegate")]
    public async Task InvokeAsync_Always_CallsNextDelegate()
    {
        var (context, _) = CreateContext();
        var correlationContext = new CorrelationContext();
        var nextCalled = false;
        var sut = CreateSut(_ =>
        {
            nextCalled = true;
            return Task.CompletedTask;
        });

        await sut.InvokeAsync(context, correlationContext);

        Assert.True(nextCalled);
    }
}
