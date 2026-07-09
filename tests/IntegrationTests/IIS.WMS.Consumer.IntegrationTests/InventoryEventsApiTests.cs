using System.Net;
using System.Net.Http.Json;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using Microsoft.AspNetCore.Mvc;

namespace IIS.WMS.Consumer.IntegrationTests;

/// <summary>End-to-end tests against the real middleware pipeline - routing, versioning, validation, exception handling, correlation id propagation (aspnet-rest-apis.instructions.md "Testing this layer").</summary>
public class InventoryEventsApiTests(CustomWebApplicationFactory factory) : IClassFixture<CustomWebApplicationFactory>
{
    private readonly HttpClient client = factory.CreateClient();

    [Fact(DisplayName = "Create then get round-trips the inventory event through the real pipeline")]
    public async Task CreateThenGet_ValidRequest_ReturnsTheCreatedInventoryEvent()
    {
        var warehouseId = $"WH-{Guid.NewGuid():N}";
        var createResponse = await client.PostAsJsonAsync(
            "/api/v1/inventory-events", new CreateInventoryEventRequest(warehouseId, "SKU1", 20));

        Assert.Equal(HttpStatusCode.Created, createResponse.StatusCode);

        var getResponse = await client.GetAsync($"/api/v1/inventory-events/{warehouseId}/SKU1");
        var body = await getResponse.Content.ReadFromJsonAsync<InventoryEventResponse>();

        Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);
        Assert.Equal(20, body!.OnHandQuantity);
    }

    [Fact(DisplayName = "Get for a warehouse/SKU that does not exist returns a 404 Problem Details body")]
    public async Task Get_UnknownWarehouseAndSku_ReturnsNotFoundProblemDetails()
    {
        var response = await client.GetAsync($"/api/v1/inventory-events/WH-unknown-{Guid.NewGuid():N}/SKU1");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact(DisplayName = "Create with a negative initial quantity returns 400 with field-level validation errors")]
    public async Task Create_NegativeInitialQuantity_ReturnsValidationProblemDetails()
    {
        var response = await client.PostAsJsonAsync(
            "/api/v1/inventory-events", new CreateInventoryEventRequest("WH1", "SKU1", -5));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var problem = await response.Content.ReadFromJsonAsync<ValidationProblemDetails>();
        Assert.Contains(nameof(CreateInventoryEventRequest.InitialQuantity), problem!.Errors.Keys);
    }

    [Fact(DisplayName = "Reserving more than on-hand returns 409 with the correlation id on the response")]
    public async Task ReserveStock_QuantityExceedsOnHand_ReturnsConflictWithCorrelationId()
    {
        var warehouseId = $"WH-{Guid.NewGuid():N}";
        await client.PostAsJsonAsync("/api/v1/inventory-events", new CreateInventoryEventRequest(warehouseId, "SKU1", 3));

        using var request = new HttpRequestMessage(
            HttpMethod.Post, $"/api/v1/inventory-events/{warehouseId}/SKU1/reservations")
        {
            Content = JsonContent.Create(new ReserveStockRequest("reservation-1", 10)),
        };
        request.Headers.Add("X-Correlation-Id", Guid.NewGuid().ToString());

        var response = await client.SendAsync(request);

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.True(response.Headers.Contains("X-Correlation-Id"));
    }
}
