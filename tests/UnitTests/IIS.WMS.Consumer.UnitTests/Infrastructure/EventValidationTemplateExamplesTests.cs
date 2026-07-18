using IIS.WMS.Common.DynamicValidation;
using IIS.WMS.Consumer.Infrastructure.DynamicValidation;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>Keeps the examples endpoint's documentation honest - every served example must compile against the real script contract.</summary>
public class EventValidationTemplateExamplesTests
{
    [Fact(DisplayName = "Every served example compiles against the real script contract")]
    public void All_EveryExample_CompilesAgainstScriptContract()
    {
        var compiler = new ValidationScriptCompiler(
            [new ConsumerValidationScriptReferenceProvider()], Substitute.For<ILogger<ValidationScriptCompiler>>());

        foreach (var example in EventValidationTemplateExamples.All)
        {
            // Compile throws TemplateCompilationException (failing this test with the diagnostics
            // and the offending example's title) if an example ever drifts from the contract.
            compiler.Compile(example.Title, example.Code);
        }
    }

    [Fact(DisplayName = "The catalog is non-empty and every example carries a title, description, and code")]
    public void All_EveryExample_IsFullyPopulated()
    {
        Assert.NotEmpty(EventValidationTemplateExamples.All);
        Assert.All(EventValidationTemplateExamples.All, example =>
        {
            Assert.False(string.IsNullOrWhiteSpace(example.Title));
            Assert.False(string.IsNullOrWhiteSpace(example.Description));
            Assert.False(string.IsNullOrWhiteSpace(example.Code));
        });
    }
}
