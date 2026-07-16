using System.Text;
using Confluent.Kafka;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.Exceptions;
using IIS.WMS.Consumer.Infrastructure.DynamicValidation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// One message shape for the script-execution tests. Public and top-level, not nested in the test
/// class - a template dereferences <c>x</c> via <c>dynamic</c>, and the runtime binder resolves
/// members with the script assembly (not this test assembly) as the call site, so a non-public type
/// would fail member access at runtime.
/// </summary>
/// <param name="Reference">Field the example template branches on.</param>
/// <param name="Id">Field the example template null-checks.</param>
public sealed record ScriptTestEvent(string? Reference, string? Id);

/// <summary>Compile/execute tests for <see cref="ValidationScriptCompiler"/> against the <c>x</c>/<c>header</c>/<c>_log</c> script contract.</summary>
public class ValidationScriptCompilerTests
{
    private readonly ValidationScriptCompiler sut = new(Substitute.For<ILogger<ValidationScriptCompiler>>());

    [Fact(DisplayName = "Compile returns a runner that evaluates the message via the x global")]
    public async Task Compile_ScriptReadingMessage_RunnerReturnsVerdictPerMessage()
    {
        var runner = sut.Compile("Schema/event.type", "return !string.IsNullOrEmpty(x.Reference);");

        var withReference = await runner(
            new ValidationScriptGlobals(new ScriptTestEvent("R1", "1"), null, Substitute.For<ILogger>(), Substitute.For<IServiceProvider>()), CancellationToken.None);
        var withoutReference = await runner(
            new ValidationScriptGlobals(new ScriptTestEvent(null, "1"), null, Substitute.For<ILogger>(), Substitute.For<IServiceProvider>()), CancellationToken.None);

        Assert.True(withReference);
        Assert.False(withoutReference);
    }

    [Fact(DisplayName = "Compile supports the full contract: header + TryGetHeader + KafkaHeaderNames + _log, and a template throw propagates")]
    public async Task Compile_ExampleTemplate_UsesHeadersLoggerAndThrowsForInvalidMessage()
    {
        const string Code = """
            if (string.IsNullOrEmpty(x.Reference)) { return true; }
            else if (x.Id == null) { throw new ApplicationException("Invalid Request"); }
            else { _log.LogInformation($"Valid request from {TryGetHeader(header, KafkaHeaderNames.Type)}"); return true; }
            """;
        var headers = new Headers { { "Type", Encoding.UTF8.GetBytes("inventory.InventoryStateChanged") } };
        var runner = sut.Compile("Schema/event.type", Code);

        var emptyReference = await runner(
            new ValidationScriptGlobals(new ScriptTestEvent(null, null), headers, Substitute.For<ILogger>(), Substitute.For<IServiceProvider>()), CancellationToken.None);
        var validMessage = await runner(
            new ValidationScriptGlobals(new ScriptTestEvent("R1", "1"), headers, Substitute.For<ILogger>(), Substitute.For<IServiceProvider>()), CancellationToken.None);
        var invalidMessage = await Assert.ThrowsAsync<ApplicationException>(() => runner(
            new ValidationScriptGlobals(new ScriptTestEvent("R1", null), headers, Substitute.For<ILogger>(), Substitute.For<IServiceProvider>()), CancellationToken.None));

        Assert.True(emptyReference);
        Assert.True(validMessage);
        Assert.Equal("Invalid Request", invalidMessage.Message);
    }

    [Fact(DisplayName = "Compile supports resolving and awaiting services from the injected DI scope via the services global")]
    public async Task Compile_ScriptResolvingService_UsesInjectedScope()
    {
        const string Code = """
            var correlation = services.GetRequiredService<ICorrelationContext>();
            return correlation.CorrelationId == "corr-1";
            """;
        var serviceProvider = new ServiceCollection()
            .AddScoped<ICorrelationContext, CorrelationContext>()
            .BuildServiceProvider();
        serviceProvider.GetRequiredService<ICorrelationContext>().Set("corr-1");

        var runner = sut.Compile("Schema/event.type", Code);
        var result = await runner(
            new ValidationScriptGlobals(new ScriptTestEvent("R1", "1"), null, Substitute.For<ILogger>(), serviceProvider), CancellationToken.None);

        Assert.True(result);
    }

    [Fact(DisplayName = "Compile throws TemplateCompilationException with the compiler diagnostics for broken code")]
    public void Compile_CodeWithCompilerError_ThrowsWithDiagnostics()
    {
        var exception = Assert.Throws<TemplateCompilationException>(
            () => sut.Compile("Schema/event.type", "return string.IsEmpty(x.Reference);"));

        Assert.Equal("Schema/event.type", exception.TemplateName);
        Assert.NotEmpty(exception.Errors);
    }
}
