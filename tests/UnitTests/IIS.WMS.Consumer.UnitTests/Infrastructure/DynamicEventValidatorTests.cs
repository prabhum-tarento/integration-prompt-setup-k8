using System.Text;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.Exceptions;
using IIS.WMS.Consumer.Infrastructure.BlobStorage;
using IIS.WMS.Consumer.Infrastructure.DynamicValidation;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>Template lookup/compile/execute/cache tests for <see cref="DynamicEventValidator"/>, with <see cref="IFileStore"/> mocked and the real compiler.</summary>
public class DynamicEventValidatorTests
{
    private const string ContainerName = "validation-templates";
    private const string SchemaName = "InventoryStateChangedEvent";
    private const string EventType = "inventory.InventoryStateChanged";
    private const string BlobName = $"{SchemaName}/{EventType}.cs";

    private readonly IFileStore fileStore = Substitute.For<IFileStore>();
    private readonly TestTimeProvider timeProvider = new();
    private readonly DynamicValidationOptions options = new();
    private readonly ILogger messageLogger = Substitute.For<ILogger>();
    private readonly IServiceProvider scopedServices = Substitute.For<IServiceProvider>();

    private DynamicEventValidator CreateSut() => new(
        fileStore,
        Options.Create(new BlobStorageOptions()),
        Options.Create(options),
        new ValidationScriptCompiler(Substitute.For<ILogger<ValidationScriptCompiler>>()),
        timeProvider,
        Substitute.For<ILogger<DynamicEventValidator>>());

    private void SetStoredTemplate(string code)
    {
        fileStore.ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(true);
        fileStore.DownloadAsync(ContainerName, BlobName, Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult<Stream>(new MemoryStream(Encoding.UTF8.GetBytes(code))));
    }

    [Fact(DisplayName = "ValidateAsync passes a message straight through when no template is stored, and caches the miss")]
    public async Task ValidateAsync_NoTemplate_ReturnsTrueAndCachesMiss()
    {
        fileStore.ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(false);
        var sut = CreateSut();

        var first = await sut.ValidateAsync(SchemaName, EventType, new ScriptTestEvent("R1", "1"), null, messageLogger, scopedServices, CancellationToken.None);
        var second = await sut.ValidateAsync(SchemaName, EventType, new ScriptTestEvent("R1", "1"), null, messageLogger, scopedServices, CancellationToken.None);

        Assert.True(first);
        Assert.True(second);
        await fileStore.Received(1).ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ValidateAsync executes the stored template and returns its verdict, compiling once across messages")]
    public async Task ValidateAsync_StoredTemplate_ExecutesPerMessageAndDownloadsOnce()
    {
        SetStoredTemplate("return !string.IsNullOrEmpty(x.Reference);");
        var sut = CreateSut();

        var valid = await sut.ValidateAsync(SchemaName, EventType, new ScriptTestEvent("R1", "1"), null, messageLogger, scopedServices, CancellationToken.None);
        var invalid = await sut.ValidateAsync(SchemaName, EventType, new ScriptTestEvent(null, "1"), null, messageLogger, scopedServices, CancellationToken.None);

        Assert.True(valid);
        Assert.False(invalid);
        await fileStore.Received(1).DownloadAsync(ContainerName, BlobName, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ValidateAsync lets a template's own throw propagate as a hard validation failure")]
    public async Task ValidateAsync_TemplateThrows_Propagates()
    {
        SetStoredTemplate("""throw new ApplicationException("Invalid Request");""");
        var sut = CreateSut();

        var exception = await Assert.ThrowsAsync<ApplicationException>(() =>
            sut.ValidateAsync(SchemaName, EventType, new ScriptTestEvent("R1", "1"), null, messageLogger, scopedServices, CancellationToken.None));

        Assert.Equal("Invalid Request", exception.Message);
    }

    [Fact(DisplayName = "ValidateAsync rethrows a broken template's compilation failure per message without recompiling")]
    public async Task ValidateAsync_BrokenTemplate_ThrowsCachedCompilationFailure()
    {
        SetStoredTemplate("return string.IsEmpty(x.Reference);");
        var sut = CreateSut();

        await Assert.ThrowsAsync<TemplateCompilationException>(() =>
            sut.ValidateAsync(SchemaName, EventType, new ScriptTestEvent("R1", "1"), null, messageLogger, scopedServices, CancellationToken.None));
        await Assert.ThrowsAsync<TemplateCompilationException>(() =>
            sut.ValidateAsync(SchemaName, EventType, new ScriptTestEvent("R1", "1"), null, messageLogger, scopedServices, CancellationToken.None));

        await fileStore.Received(1).DownloadAsync(ContainerName, BlobName, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ValidateAsync re-reads the template from Blob Storage once the cache entry expires")]
    public async Task ValidateAsync_CacheExpired_RefreshesFromStorage()
    {
        fileStore.ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(false);
        var sut = CreateSut();

        await sut.ValidateAsync(SchemaName, EventType, new ScriptTestEvent("R1", "1"), null, messageLogger, scopedServices, CancellationToken.None);
        timeProvider.UtcNow += options.CacheDuration + TimeSpan.FromSeconds(1);
        await sut.ValidateAsync(SchemaName, EventType, new ScriptTestEvent("R1", "1"), null, messageLogger, scopedServices, CancellationToken.None);

        await fileStore.Received(2).ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "ValidateAsync skips the lookup entirely for a message with no Type header value")]
    public async Task ValidateAsync_EmptyEventType_ReturnsTrueWithoutLookup()
    {
        var sut = CreateSut();

        var result = await sut.ValidateAsync(SchemaName, string.Empty, new ScriptTestEvent("R1", "1"), null, messageLogger, scopedServices, CancellationToken.None);

        Assert.True(result);
        await fileStore.DidNotReceiveWithAnyArgs().ExistsAsync(default!, default!);
    }

    [Fact(DisplayName = "ValidateAsync skips the lookup entirely when dynamic validation is disabled")]
    public async Task ValidateAsync_Disabled_ReturnsTrueWithoutLookup()
    {
        var sut = new DynamicEventValidator(
            fileStore,
            Options.Create(new BlobStorageOptions()),
            Options.Create(new DynamicValidationOptions { Enabled = false }),
            new ValidationScriptCompiler(Substitute.For<ILogger<ValidationScriptCompiler>>()),
            timeProvider,
            Substitute.For<ILogger<DynamicEventValidator>>());

        var result = await sut.ValidateAsync(SchemaName, EventType, new ScriptTestEvent("R1", "1"), null, messageLogger, scopedServices, CancellationToken.None);

        Assert.True(result);
        await fileStore.DidNotReceiveWithAnyArgs().ExistsAsync(default!, default!);
    }

    /// <summary>Manually advanced clock so the cache-expiry test doesn't sleep through a real <see cref="DynamicValidationOptions.CacheDuration"/>.</summary>
    private sealed class TestTimeProvider : TimeProvider
    {
        public DateTimeOffset UtcNow { get; set; } = new(2026, 7, 16, 12, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => UtcNow;
    }
}
