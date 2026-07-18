using IIS.WMS.Consumer.Api.Controllers;
using IIS.WMS.Consumer.Application.EventValidationTemplates;
using IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Api;

/// <summary>Request/response-mapping tests for <see cref="EventValidationTemplatesController"/>, with <see cref="IEventValidationTemplateService"/> mocked (aspnet-rest-apis.instructions.md "Testing this layer").</summary>
public class EventValidationTemplatesControllerTests
{
    private const string Transport = "Kafka";
    private const string Identifier = "inventory.InventoryStateChanged";
    private const string Code = "return true;";

    private readonly IEventValidationTemplateService service = Substitute.For<IEventValidationTemplateService>();
    private readonly EventValidationTemplatesController sut;

    public EventValidationTemplatesControllerTests()
    {
        sut = new EventValidationTemplatesController(service, Substitute.For<ILogger<EventValidationTemplatesController>>())
        {
            // CreateAsync reads Request.PathBase/RouteData off the HttpContext to build its Location
            // header - a bare controller instance has neither.
            ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext(),
                RouteData = new RouteData(new RouteValueDictionary { ["version"] = "1.0" }),
            },
        };
    }

    [Fact(DisplayName = "ListAsync returns 200 with every stored template identity")]
    public async Task ListAsync_TemplatesStored_ReturnsOkWithSummaries()
    {
        var summaries = new List<EventValidationTemplateSummary> { new(Transport, Identifier) };
        service.ListAsync(Transport, Arg.Any<CancellationToken>()).Returns(summaries);

        var result = await sut.ListAsync(Transport, CancellationToken.None);

        var okResult = Assert.IsType<OkObjectResult>(result.Result);
        Assert.Same(summaries, okResult.Value);
    }

    [Fact(DisplayName = "GetExamples returns 200 with the example catalog")]
    public void GetExamples_ReturnsOkWithExamples()
    {
        var examples = new List<EventValidationTemplateExample> { new("Title", "Description", Code) };
        service.GetExamples().Returns(examples);

        var result = sut.GetExamples();

        var okResult = Assert.IsType<OkObjectResult>(result.Result);
        Assert.Same(examples, okResult.Value);
    }

    [Fact(DisplayName = "GetAsync returns 200 with the template when it exists")]
    public async Task GetAsync_TemplateExists_ReturnsOk()
    {
        var template = new EventValidationTemplateResponse(Transport, Identifier, Code);
        service.GetAsync(Transport, Identifier, Arg.Any<CancellationToken>()).Returns(template);

        var result = await sut.GetAsync(Transport, Identifier, CancellationToken.None);

        var okResult = Assert.IsType<OkObjectResult>(result.Result);
        Assert.Same(template, okResult.Value);
    }

    [Fact(DisplayName = "GetAsync returns 404 when no template is stored")]
    public async Task GetAsync_MissingTemplate_ReturnsNotFound()
    {
        service.GetAsync(Transport, Identifier, Arg.Any<CancellationToken>())
            .Returns((EventValidationTemplateResponse?)null);

        var result = await sut.GetAsync(Transport, Identifier, CancellationToken.None);

        Assert.IsType<NotFoundResult>(result.Result);
    }

    [Fact(DisplayName = "CreateAsync returns 201 with a Location pointing at the new template")]
    public async Task CreateAsync_NewTemplate_ReturnsCreatedWithLocation()
    {
        var request = new CreateEventValidationTemplateRequest(Transport, Identifier, Code);
        var created = new EventValidationTemplateResponse(Transport, Identifier, Code);
        service.CreateAsync(request, Arg.Any<CancellationToken>()).Returns(created);

        var result = await sut.CreateAsync(request, CancellationToken.None);

        var createdResult = Assert.IsType<CreatedResult>(result.Result);
        Assert.Same(created, createdResult.Value);
        Assert.EndsWith($"/event-validation-templates/{Transport}/{Identifier}", createdResult.Location);
    }

    [Fact(DisplayName = "UpdateAsync returns 200 with the replaced template")]
    public async Task UpdateAsync_ExistingTemplate_ReturnsOk()
    {
        var request = new UpdateEventValidationTemplateRequest(Code);
        var updated = new EventValidationTemplateResponse(Transport, Identifier, Code);
        service.UpdateAsync(Transport, Identifier, request, Arg.Any<CancellationToken>()).Returns(updated);

        var result = await sut.UpdateAsync(Transport, Identifier, request, CancellationToken.None);

        var okResult = Assert.IsType<OkObjectResult>(result.Result);
        Assert.Same(updated, okResult.Value);
    }

    [Fact(DisplayName = "DeleteAsync returns 204 when the template existed")]
    public async Task DeleteAsync_TemplateExisted_ReturnsNoContent()
    {
        service.DeleteAsync(Transport, Identifier, Arg.Any<CancellationToken>()).Returns(true);

        var result = await sut.DeleteAsync(Transport, Identifier, CancellationToken.None);

        Assert.IsType<NoContentResult>(result);
    }

    [Fact(DisplayName = "DeleteAsync returns 404 when there was nothing to delete")]
    public async Task DeleteAsync_MissingTemplate_ReturnsNotFound()
    {
        service.DeleteAsync(Transport, Identifier, Arg.Any<CancellationToken>()).Returns(false);

        var result = await sut.DeleteAsync(Transport, Identifier, CancellationToken.None);

        Assert.IsType<NotFoundResult>(result);
    }
}
