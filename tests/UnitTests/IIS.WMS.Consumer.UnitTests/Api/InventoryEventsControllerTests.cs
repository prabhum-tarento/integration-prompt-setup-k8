using IIS.WMS.Consumer.Api.Controllers;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Logging;
using NSubstitute;

namespace IIS.WMS.Consumer.UnitTests.Api;

/// <summary>Request/response-mapping and status-code tests for <see cref="InventoryEventsController"/>, with <see cref="IInventoryEventService"/> mocked (aspnet-rest-apis.instructions.md "Testing this layer").</summary>
public class InventoryEventsControllerTests
{
    private readonly IInventoryEventService service = Substitute.For<IInventoryEventService>();
    private readonly InventoryEventsController sut;

    public InventoryEventsControllerTests()
    {
        sut = new InventoryEventsController(service, Substitute.For<ILogger<InventoryEventsController>>())
        {
            ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext(),
                RouteData = new RouteData(new RouteValueDictionary { ["version"] = "1.0" }),
            },
        };
    }

    [Fact(DisplayName = "GetAsync returns 200 with the response body when the aggregate exists")]
    public async Task GetAsync_AggregateExists_ReturnsOkWithResponse()
    {
        var response = new InventoryEventResponse("WH1:SKU1", "WH1", "SKU1", 10, DateTime.UtcNow, DateTime.UtcNow);
        service.GetAsync("WH1", "SKU1", Arg.Any<CancellationToken>()).Returns(response);

        var result = await sut.GetAsync("WH1", "SKU1", CancellationToken.None);

        var okResult = Assert.IsType<OkObjectResult>(result.Result);
        Assert.Same(response, okResult.Value);
    }

    [Fact(DisplayName = "GetAsync returns 404 when the aggregate does not exist")]
    public async Task GetAsync_AggregateDoesNotExist_ReturnsNotFound()
    {
        service.GetAsync("WH1", "SKU1", Arg.Any<CancellationToken>()).Returns((InventoryEventResponse?)null);

        var result = await sut.GetAsync("WH1", "SKU1", CancellationToken.None);

        Assert.IsType<NotFoundResult>(result.Result);
    }

    [Fact(DisplayName = "CreateAsync returns 201 with a Location header pointing at GetAsync")]
    public async Task CreateAsync_ValidRequest_ReturnsCreatedWithLocation()
    {
        var response = new InventoryEventResponse("WH1:SKU1", "WH1", "SKU1", 25, DateTime.UtcNow, DateTime.UtcNow);
        service.CreateAsync(Arg.Any<CreateInventoryEventRequest>(), Arg.Any<CancellationToken>()).Returns(response);

        var result = await sut.CreateAsync(new CreateInventoryEventRequest("WH1", "SKU1", 25), CancellationToken.None);

        var createdResult = Assert.IsType<CreatedResult>(result.Result);
        Assert.Equal("/api/v1.0/inventory-events/WH1/SKU1", createdResult.Location);
        Assert.Same(response, createdResult.Value);
    }

    [Fact(DisplayName = "ReserveStockAsync returns 200 with the updated response")]
    public async Task ReserveStockAsync_SufficientStock_ReturnsOkWithResponse()
    {
        var response = new InventoryEventResponse("WH1:SKU1", "WH1", "SKU1", 6, DateTime.UtcNow, DateTime.UtcNow);
        service.ReserveStockAsync("WH1", "SKU1", Arg.Any<ReserveStockRequest>(), Arg.Any<CancellationToken>())
            .Returns(response);

        var result = await sut.ReserveStockAsync("WH1", "SKU1", new ReserveStockRequest("reservation-1", 4), CancellationToken.None);

        var okResult = Assert.IsType<OkObjectResult>(result.Result);
        Assert.Same(response, okResult.Value);
    }
}
