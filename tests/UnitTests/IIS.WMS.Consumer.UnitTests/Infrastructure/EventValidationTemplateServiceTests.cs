using System.Text;
using IIS.WMS.Common.BlobStorage;
using IIS.WMS.Common.DynamicValidation;
using IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;
using IIS.WMS.Consumer.Application.Exceptions;
using IIS.WMS.Consumer.Infrastructure.DynamicValidation;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>CRUD-over-blobs tests for <see cref="EventValidationTemplateService"/>, with <see cref="IFileStore"/> mocked and the real compiler for the compile-before-store gate.</summary>
public class EventValidationTemplateServiceTests
{
    private const string ContainerName = "validation-templates";
    private const string Transport = "Kafka";
    private const string Identifier = "inventory.InventoryStateChanged";
    private const string BlobName = $"{Transport}/{Identifier}.cs";
    private const string ValidCode = "return true;";

    private readonly IFileStore fileStore = Substitute.For<IFileStore>();
    private readonly EventValidationTemplateService sut;

    public EventValidationTemplateServiceTests()
    {
        sut = new EventValidationTemplateService(
            fileStore,
            Options.Create(new BlobStorageOptions()),
            new ValidationScriptCompiler([new ConsumerValidationScriptReferenceProvider()], Substitute.For<ILogger<ValidationScriptCompiler>>()),
            Substitute.For<ILogger<EventValidationTemplateService>>());
    }

    [Fact(DisplayName = "GetAsync returns null when no template is stored")]
    public async Task GetAsync_MissingTemplate_ReturnsNull()
    {
        fileStore.ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(false);

        var result = await sut.GetAsync(Transport, Identifier);

        Assert.Null(result);
    }

    [Fact(DisplayName = "GetAsync returns the stored template's code")]
    public async Task GetAsync_StoredTemplate_ReturnsCode()
    {
        fileStore.ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(true);
        fileStore.DownloadAsync(ContainerName, BlobName, Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult<Stream>(new MemoryStream(Encoding.UTF8.GetBytes(ValidCode))));

        var result = await sut.GetAsync(Transport, Identifier);

        Assert.Equal(new EventValidationTemplateResponse(Transport, Identifier, ValidCode), result);
    }

    [Fact(DisplayName = "ListAsync parses blob names into template identities and skips non-conforming blobs")]
    public async Task ListAsync_MixedBlobNames_ReturnsOnlyConformingTemplates()
    {
        fileStore.ListAsync(ContainerName, null, Arg.Any<CancellationToken>()).Returns(
            [BlobName, "ServiceBus/other-queue.cs", "readme.txt", "no-folder.cs", "a/b/c.cs"]);

        var result = await sut.ListAsync();

        Assert.Equal(
            new EventValidationTemplateSummary[] { new(Transport, Identifier), new("ServiceBus", "other-queue") },
            result);
    }

    [Fact(DisplayName = "ListAsync narrows to one transport via a folder prefix")]
    public async Task ListAsync_TransportFilter_UsesFolderPrefix()
    {
        fileStore.ListAsync(ContainerName, $"{Transport}/", Arg.Any<CancellationToken>()).Returns([BlobName]);

        var result = await sut.ListAsync(Transport);

        Assert.Equal(new EventValidationTemplateSummary[] { new(Transport, Identifier) }, result);
    }

    [Fact(DisplayName = "CreateAsync compiles then uploads a new template under {transport}/{identifier}.cs")]
    public async Task CreateAsync_NewTemplate_UploadsBlob()
    {
        fileStore.ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(false);

        var result = await sut.CreateAsync(new CreateEventValidationTemplateRequest(Transport, Identifier, ValidCode));

        Assert.Equal(new EventValidationTemplateResponse(Transport, Identifier, ValidCode), result);
        await fileStore.Received(1).UploadAsync(ContainerName, BlobName, Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "CreateAsync throws ConflictException when the template already exists")]
    public async Task CreateAsync_ExistingTemplate_ThrowsConflict()
    {
        fileStore.ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(true);

        await Assert.ThrowsAsync<ConflictException>(
            () => sut.CreateAsync(new CreateEventValidationTemplateRequest(Transport, Identifier, ValidCode)));

        await fileStore.DidNotReceiveWithAnyArgs().UploadAsync(default!, default!, default!);
    }

    [Fact(DisplayName = "CreateAsync rejects code that doesn't compile and stores nothing")]
    public async Task CreateAsync_BrokenCode_ThrowsCompilationExceptionWithoutStoring()
    {
        await Assert.ThrowsAsync<TemplateCompilationException>(
            () => sut.CreateAsync(new CreateEventValidationTemplateRequest(Transport, Identifier, "return nonsense!!;")));

        await fileStore.DidNotReceiveWithAnyArgs().UploadAsync(default!, default!, default!);
    }

    [Fact(DisplayName = "UpdateAsync replaces an existing template's code")]
    public async Task UpdateAsync_ExistingTemplate_UploadsReplacement()
    {
        fileStore.ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(true);

        var result = await sut.UpdateAsync(Transport, Identifier, new UpdateEventValidationTemplateRequest(ValidCode));

        Assert.Equal(new EventValidationTemplateResponse(Transport, Identifier, ValidCode), result);
        await fileStore.Received(1).UploadAsync(ContainerName, BlobName, Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "UpdateAsync throws NotFoundException when no template exists to replace")]
    public async Task UpdateAsync_MissingTemplate_ThrowsNotFound()
    {
        fileStore.ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(false);

        await Assert.ThrowsAsync<NotFoundException>(
            () => sut.UpdateAsync(Transport, Identifier, new UpdateEventValidationTemplateRequest(ValidCode)));

        await fileStore.DidNotReceiveWithAnyArgs().UploadAsync(default!, default!, default!);
    }

    [Fact(DisplayName = "DeleteAsync reports whether a template actually existed to delete")]
    public async Task DeleteAsync_PassesThroughStoreResult()
    {
        fileStore.DeleteAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(true, false);

        Assert.True(await sut.DeleteAsync(Transport, Identifier));
        Assert.False(await sut.DeleteAsync(Transport, Identifier));
    }

    [Fact(DisplayName = "GetExamples returns the shared worked-example catalog")]
    public void GetExamples_Always_ReturnsExampleTemplateExamplesAll()
    {
        var result = sut.GetExamples();

        Assert.Same(EventValidationTemplateExamples.All, result);
    }
}
