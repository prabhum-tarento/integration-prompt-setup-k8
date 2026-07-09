using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Application.InventoryEvents.Validators;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>Field-level validation rule tests for <see cref="CreateInventoryEventRequestValidator"/>.</summary>
public class CreateInventoryEventRequestValidatorTests
{
    private readonly CreateInventoryEventRequestValidator sut = new();

    [Fact(DisplayName = "Validate succeeds for a well-formed request")]
    public void Validate_WellFormedRequest_HasNoErrors()
    {
        var result = sut.Validate(new CreateInventoryEventRequest("WH1", "SKU1", 10));

        Assert.True(result.IsValid);
    }

    [Fact(DisplayName = "Validate fails when WarehouseId is empty")]
    public void Validate_EmptyWarehouseId_HasError()
    {
        var result = sut.Validate(new CreateInventoryEventRequest(string.Empty, "SKU1", 10));

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(CreateInventoryEventRequest.WarehouseId));
    }

    [Fact(DisplayName = "Validate fails when InitialQuantity is negative")]
    public void Validate_NegativeInitialQuantity_HasError()
    {
        var result = sut.Validate(new CreateInventoryEventRequest("WH1", "SKU1", -1));

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(CreateInventoryEventRequest.InitialQuantity));
    }
}
