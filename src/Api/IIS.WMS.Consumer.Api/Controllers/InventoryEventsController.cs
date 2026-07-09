using Asp.Versioning;
using IIS.WMS.Consumer.Application.InventoryEvents;
using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace IIS.WMS.Consumer.Api.Controllers;

/// <summary>Inventory on-hand state for a warehouse/SKU (aspnet-rest-apis.instructions.md "Controllers, not Minimal APIs").</summary>
[ApiController]
[ApiVersion("1.0")]
[Route("api/v{version:apiVersion}/inventory-events")]
public sealed class InventoryEventsController(IInventoryEventService service, ILogger<InventoryEventsController> logger)
    : ControllerBase
{
    /// <summary>Returns the current on-hand state for a warehouse/SKU.</summary>
    /// <param name="warehouseId">Warehouse to look up.</param>
    /// <param name="sku">SKU to look up.</param>
    /// <param name="cancellationToken">Request abort token.</param>
    /// <returns><c>200 OK</c> with the current state, or <c>404 Not Found</c> if it doesn't exist.</returns>
    [HttpGet("{warehouseId}/{sku}")]
    [ProducesResponseType<InventoryEventResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    public async Task<ActionResult<InventoryEventResponse>> GetAsync(
        string warehouseId, string sku, CancellationToken cancellationToken)
    {
        logger.LogDebug("GET inventory event {WarehouseId}/{Sku}.", warehouseId, sku);

        var result = await service.GetAsync(warehouseId, sku, cancellationToken);

        return result is null ? NotFound() : Ok(result);
    }

    /// <summary>Creates the inventory aggregate for a warehouse/SKU.</summary>
    /// <param name="request">Warehouse, SKU, and initial on-hand quantity.</param>
    /// <param name="cancellationToken">Request abort token.</param>
    /// <returns><c>201 Created</c> with a <c>Location</c> header pointing at <see cref="GetAsync"/>.</returns>
    [HttpPost]
    [ProducesResponseType<InventoryEventResponse>(StatusCodes.Status201Created)]
    [ProducesResponseType<ValidationProblemDetails>(StatusCodes.Status400BadRequest)]
    public async Task<ActionResult<InventoryEventResponse>> CreateAsync(
        CreateInventoryEventRequest request, CancellationToken cancellationToken)
    {
        logger.LogDebug(
            "POST create inventory event {WarehouseId}/{Sku}, initial quantity {InitialQuantity}.",
            request.WarehouseId, request.Sku, request.InitialQuantity);

        var result = await service.CreateAsync(request, cancellationToken);

        // CreatedAtAction/Url.Action route back through the ApiVersionRouteConstraint, which
        // fails link generation with "No route matches the supplied values" for this
        // attribute-routed, versioned action (a known Asp.Versioning + CreatedAtAction friction
        // point). Building the Location URI directly from the current request's own "version"
        // route value sidesteps constraint re-evaluation entirely.
        var version = RouteData.Values["version"];
        var location = $"{Request.PathBase}/api/v{version}/inventory-events/" +
                        $"{Uri.EscapeDataString(result.WarehouseId)}/{Uri.EscapeDataString(result.Sku)}";

        return Created(location, result);
    }

    /// <summary>Reserves quantity against a warehouse/SKU's on-hand balance.</summary>
    /// <param name="warehouseId">Warehouse to reserve against.</param>
    /// <param name="sku">SKU to reserve against.</param>
    /// <param name="request">Reservation id and quantity.</param>
    /// <param name="cancellationToken">Request abort token.</param>
    /// <returns><c>200 OK</c> with the updated state, <c>404 Not Found</c> if the aggregate doesn't exist, or <c>409 Conflict</c> if on-hand can't cover the request.</returns>
    [HttpPost("{warehouseId}/{sku}/reservations")]
    [ProducesResponseType<InventoryEventResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType<ValidationProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status404NotFound)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status409Conflict)]
    public async Task<ActionResult<InventoryEventResponse>> ReserveStockAsync(
        string warehouseId, string sku, ReserveStockRequest request, CancellationToken cancellationToken)
    {
        logger.LogDebug(
            "POST reserve stock {WarehouseId}/{Sku}, reservation {ReservationId}, quantity {Quantity}.",
            warehouseId, sku, request.ReservationId, request.Quantity);

        var result = await service.ReserveStockAsync(warehouseId, sku, request, cancellationToken);

        return Ok(result);
    }
}
