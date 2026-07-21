using IIS.WMS.Consumer.Infrastructure.Messaging.Events.BulkInventoryImport.AvroContracts;
using IIS.WMS.Consumer.Infrastructure.Messaging.Events.BulkInventoryImport.Validators;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Field-level validation rule tests for <see cref="BulkInventoryImportEventValidator"/> - the single
/// validation point for bulk-import data (integration-resiliency.instructions.md §1).
/// </summary>
public class BulkInventoryImportEventValidatorTests
{
    private readonly BulkInventoryImportEventValidator sut = new();

    private static BulkInventoryImportEvent CreateValidEvent() => new()
    {
        EventId = "EVT-1",
        WarehouseId = "WH-1",
        Sku = "SKU-1",
        Quantity = 10,
        SourceSystem = "WMS",
        LastUpdatedUtcMillis = 1_700_000_000_000,
    };

    [Fact(DisplayName = "Validate succeeds for a well-formed event")]
    public void Validate_WellFormedEvent_HasNoErrors()
    {
        var result = sut.Validate(CreateValidEvent());

        Assert.True(result.IsValid);
    }

    [Fact(DisplayName = "Validate fails when EventId is empty")]
    public void Validate_EmptyEventId_HasError()
    {
        var target = CreateValidEvent();
        target.EventId = string.Empty;

        var result = sut.Validate(target);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(BulkInventoryImportEvent.EventId));
    }

    [Fact(DisplayName = "Validate fails when WarehouseId is empty")]
    public void Validate_EmptyWarehouseId_HasError()
    {
        var target = CreateValidEvent();
        target.WarehouseId = string.Empty;

        var result = sut.Validate(target);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(BulkInventoryImportEvent.WarehouseId));
    }

    [Fact(DisplayName = "Validate fails when WarehouseId exceeds the maximum length")]
    public void Validate_WarehouseIdTooLong_HasError()
    {
        var target = CreateValidEvent();
        target.WarehouseId = new string('W', 65);

        var result = sut.Validate(target);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(BulkInventoryImportEvent.WarehouseId));
    }

    [Fact(DisplayName = "Validate fails when Sku is empty")]
    public void Validate_EmptySku_HasError()
    {
        var target = CreateValidEvent();
        target.Sku = string.Empty;

        var result = sut.Validate(target);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(BulkInventoryImportEvent.Sku));
    }

    [Fact(DisplayName = "Validate fails when Sku exceeds the maximum length")]
    public void Validate_SkuTooLong_HasError()
    {
        var target = CreateValidEvent();
        target.Sku = new string('S', 65);

        var result = sut.Validate(target);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(BulkInventoryImportEvent.Sku));
    }

    [Fact(DisplayName = "Validate fails when Quantity is negative")]
    public void Validate_NegativeQuantity_HasError()
    {
        var target = CreateValidEvent();
        target.Quantity = -1;

        var result = sut.Validate(target);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(BulkInventoryImportEvent.Quantity));
    }

    [Fact(DisplayName = "Validate succeeds when Quantity is exactly zero")]
    public void Validate_ZeroQuantity_HasNoError()
    {
        var target = CreateValidEvent();
        target.Quantity = 0;

        var result = sut.Validate(target);

        Assert.DoesNotContain(result.Errors, e => e.PropertyName == nameof(BulkInventoryImportEvent.Quantity));
    }

    [Fact(DisplayName = "Validate fails when SourceSystem is empty")]
    public void Validate_EmptySourceSystem_HasError()
    {
        var target = CreateValidEvent();
        target.SourceSystem = string.Empty;

        var result = sut.Validate(target);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(BulkInventoryImportEvent.SourceSystem));
    }

    [Fact(DisplayName = "Validate fails when SourceSystem exceeds the maximum length")]
    public void Validate_SourceSystemTooLong_HasError()
    {
        var target = CreateValidEvent();
        target.SourceSystem = new string('X', 65);

        var result = sut.Validate(target);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(BulkInventoryImportEvent.SourceSystem));
    }

    [Fact(DisplayName = "Validate fails when LastUpdatedUtcMillis is zero")]
    public void Validate_ZeroLastUpdatedUtcMillis_HasError()
    {
        var target = CreateValidEvent();
        target.LastUpdatedUtcMillis = 0;

        var result = sut.Validate(target);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(BulkInventoryImportEvent.LastUpdatedUtcMillis));
    }

    [Fact(DisplayName = "Validate fails when LastUpdatedUtcMillis is negative")]
    public void Validate_NegativeLastUpdatedUtcMillis_HasError()
    {
        var target = CreateValidEvent();
        target.LastUpdatedUtcMillis = -1;

        var result = sut.Validate(target);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.PropertyName == nameof(BulkInventoryImportEvent.LastUpdatedUtcMillis));
    }
}
