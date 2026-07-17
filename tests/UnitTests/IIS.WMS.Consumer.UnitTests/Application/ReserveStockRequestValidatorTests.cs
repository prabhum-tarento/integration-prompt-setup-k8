using IIS.WMS.Consumer.Application.InventoryEvents.Dtos;
using IIS.WMS.Consumer.Application.InventoryEvents.Validators;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>Field-level validation rule tests for <see cref="ReserveStockRequestValidator"/>.</summary>
public class ReserveStockRequestValidatorTests
{
    private readonly ReserveStockRequestValidator sut = new();

    [Fact(DisplayName = "Validate succeeds for a well-formed request")]
    public void Validate_WellFormedRequest_HasNoErrors()
    {
        var result = sut.Validate(new ReserveStockRequest("reservation-1", 5));

        Assert.True(result.IsValid);
    }

    [Fact(DisplayName = "Validate fails when ReservationId is empty")]
    public void Validate_EmptyReservationId_HasError()
    {
        var result = sut.Validate(new ReserveStockRequest(string.Empty, 5));

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(ReserveStockRequest.ReservationId));
    }

    [Fact(DisplayName = "Validate fails when ReservationId exceeds the maximum length")]
    public void Validate_ReservationIdExceedsMaximumLength_HasError()
    {
        var tooLong = new string('r', 129);

        var result = sut.Validate(new ReserveStockRequest(tooLong, 5));

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(ReserveStockRequest.ReservationId));
    }

    [Fact(DisplayName = "Validate succeeds when ReservationId is exactly at the maximum length")]
    public void Validate_ReservationIdAtMaximumLength_HasNoErrors()
    {
        var maxLength = new string('r', 128);

        var result = sut.Validate(new ReserveStockRequest(maxLength, 5));

        Assert.True(result.IsValid);
    }

    [Theory(DisplayName = "Validate fails when Quantity is zero or negative")]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_QuantityNotPositive_HasError(int quantity)
    {
        var result = sut.Validate(new ReserveStockRequest("reservation-1", quantity));

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(ReserveStockRequest.Quantity));
    }
}
