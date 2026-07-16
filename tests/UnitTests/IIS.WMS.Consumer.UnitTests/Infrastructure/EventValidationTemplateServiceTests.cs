using System.Text;
using IIS.WMS.Consumer.Application.Common;
using IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;
using IIS.WMS.Consumer.Application.Exceptions;
using IIS.WMS.Consumer.Infrastructure.BlobStorage;
using IIS.WMS.Consumer.Infrastructure.DynamicValidation;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>CRUD-over-blobs tests for <see cref="EventValidationTemplateService"/>, with <see cref="IFileStore"/> mocked and the real compiler for the compile-before-store gate.</summary>
public class EventValidationTemplateServiceTests
{
    private const string ContainerName = "validation-templates";
    private const string SchemaName = "InventoryStateChangedEvent";
    private const string EventType = "inventory.InventoryStateChanged";
    private const string BlobName = $"{SchemaName}/{EventType}.cs";
    private const string ValidCode = "return true;";

    private readonly IFileStore fileStore = Substitute.For<IFileStore>();
    private readonly EventValidationTemplateService sut;

    public EventValidationTemplateServiceTests()
    {
        sut = new EventValidationTemplateService(
            fileStore,
            Options.Create(new BlobStorageOptions()),
            new ValidationScriptCompiler(Substitute.For<ILogger<ValidationScriptCompiler>>()),
            Substitute.For<ILogger<EventValidationTemplateService>>());
    }

    [Fact(DisplayName = "GetAsync returns null when no template is stored")]
    public async Task GetAsync_MissingTemplate_ReturnsNull()
    {
        fileStore.ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(false);

        var result = await sut.GetAsync(SchemaName, EventType);

        Assert.Null(result);
    }

    [Fact(DisplayName = "GetAsync returns the stored template's code")]
    public async Task GetAsync_StoredTemplate_ReturnsCode()
    {
        fileStore.ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(true);
        fileStore.DownloadAsync(ContainerName, BlobName, Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult<Stream>(new MemoryStream(Encoding.UTF8.GetBytes(ValidCode))));

        var result = await sut.GetAsync(SchemaName, EventType);

        Assert.Equal(new EventValidationTemplateResponse(SchemaName, EventType, ValidCode), result);
    }

    [Fact(DisplayName = "ListAsync parses blob names into template identities and skips non-conforming blobs")]
    public async Task ListAsync_MixedBlobNames_ReturnsOnlyConformingTemplates()
    {
        fileStore.ListAsync(ContainerName, null, Arg.Any<CancellationToken>()).Returns(
            [BlobName, "OtherSchema/other.EventType.cs", "readme.txt", "no-folder.cs", "a/b/c.cs"]);

        var result = await sut.ListAsync();

        Assert.Equal(
            new EventValidationTemplateSummary[] { new(SchemaName, EventType), new("OtherSchema", "other.EventType") },
            result);
    }

    [Fact(DisplayName = "ListAsync narrows to one schema via a folder prefix")]
    public async Task ListAsync_SchemaFilter_UsesFolderPrefix()
    {
        fileStore.ListAsync(ContainerName, $"{SchemaName}/", Arg.Any<CancellationToken>()).Returns([BlobName]);

        var result = await sut.ListAsync(SchemaName);

        Assert.Equal(new EventValidationTemplateSummary[] { new(SchemaName, EventType) }, result);
    }

    [Fact(DisplayName = "CreateAsync compiles then uploads a new template under {schemaName}/{eventType}.cs")]
    public async Task CreateAsync_NewTemplate_UploadsBlob()
    {
        fileStore.ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(false);

        var result = await sut.CreateAsync(new CreateEventValidationTemplateRequest(SchemaName, EventType, ValidCode));

        Assert.Equal(new EventValidationTemplateResponse(SchemaName, EventType, ValidCode), result);
        await fileStore.Received(1).UploadAsync(ContainerName, BlobName, Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "CreateAsync throws ConflictException when the template already exists")]
    public async Task CreateAsync_ExistingTemplate_ThrowsConflict()
    {
        fileStore.ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(true);

        await Assert.ThrowsAsync<ConflictException>(
            () => sut.CreateAsync(new CreateEventValidationTemplateRequest(SchemaName, EventType, ValidCode)));

        await fileStore.DidNotReceiveWithAnyArgs().UploadAsync(default!, default!, default!);
    }

    [Fact(DisplayName = "CreateAsync rejects code that doesn't compile and stores nothing")]
    public async Task CreateAsync_BrokenCode_ThrowsCompilationExceptionWithoutStoring()
    {
        await Assert.ThrowsAsync<TemplateCompilationException>(
            () => sut.CreateAsync(new CreateEventValidationTemplateRequest(SchemaName, EventType, "return nonsense!!;")));

        await fileStore.DidNotReceiveWithAnyArgs().UploadAsync(default!, default!, default!);
    }

    [Fact(DisplayName = "UpdateAsync replaces an existing template's code")]
    public async Task UpdateAsync_ExistingTemplate_UploadsReplacement()
    {
        fileStore.ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(true);

        var result = await sut.UpdateAsync(SchemaName, EventType, new UpdateEventValidationTemplateRequest(ValidCode));

        Assert.Equal(new EventValidationTemplateResponse(SchemaName, EventType, ValidCode), result);
        await fileStore.Received(1).UploadAsync(ContainerName, BlobName, Arg.Any<Stream>(), Arg.Any<CancellationToken>());
    }

    [Fact(DisplayName = "UpdateAsync throws NotFoundException when no template exists to replace")]
    public async Task UpdateAsync_MissingTemplate_ThrowsNotFound()
    {
        fileStore.ExistsAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(false);

        await Assert.ThrowsAsync<NotFoundException>(
            () => sut.UpdateAsync(SchemaName, EventType, new UpdateEventValidationTemplateRequest(ValidCode)));

        await fileStore.DidNotReceiveWithAnyArgs().UploadAsync(default!, default!, default!);
    }

    [Fact(DisplayName = "DeleteAsync reports whether a template actually existed to delete")]
    public async Task DeleteAsync_PassesThroughStoreResult()
    {
        fileStore.DeleteAsync(ContainerName, BlobName, Arg.Any<CancellationToken>()).Returns(true, false);

        Assert.True(await sut.DeleteAsync(SchemaName, EventType));
        Assert.False(await sut.DeleteAsync(SchemaName, EventType));
    }
}
