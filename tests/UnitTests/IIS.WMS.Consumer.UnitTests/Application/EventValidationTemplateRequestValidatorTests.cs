using IIS.WMS.Consumer.Application.EventValidationTemplates.Dtos;
using IIS.WMS.Consumer.Application.EventValidationTemplates.Validators;

namespace IIS.WMS.Consumer.UnitTests.Application;

/// <summary>Shape-rule tests for the event validation template request validators.</summary>
public class EventValidationTemplateRequestValidatorTests
{
    private readonly CreateEventValidationTemplateRequestValidator createValidator = new();
    private readonly UpdateEventValidationTemplateRequestValidator updateValidator = new();

    [Fact(DisplayName = "Create accepts a well-formed schema/event type/code triple")]
    public void Validate_WellFormedCreateRequest_Passes()
    {
        var result = createValidator.Validate(
            new CreateEventValidationTemplateRequest("InventoryStateChangedEvent", "inventory.InventoryStateChanged", "return true;"));

        Assert.True(result.IsValid);
    }

    [Theory(DisplayName = "Create rejects identity fields that would escape their blob path segment")]
    [InlineData("", "inventory.InventoryStateChanged")]
    [InlineData("Schema/other", "inventory.InventoryStateChanged")]
    [InlineData("InventoryStateChangedEvent", "")]
    [InlineData("InventoryStateChangedEvent", "../consumer-dead-letter")]
    [InlineData("InventoryStateChangedEvent", "a/b")]
    public void Validate_MalformedIdentity_Fails(string schemaName, string eventType)
    {
        var result = createValidator.Validate(
            new CreateEventValidationTemplateRequest(schemaName, eventType, "return true;"));

        Assert.False(result.IsValid);
    }

    [Fact(DisplayName = "Create and Update both reject empty code")]
    public void Validate_EmptyCode_Fails()
    {
        Assert.False(createValidator.Validate(
            new CreateEventValidationTemplateRequest("Schema", "event.type", string.Empty)).IsValid);
        Assert.False(updateValidator.Validate(
            new UpdateEventValidationTemplateRequest(string.Empty)).IsValid);
    }

    [Fact(DisplayName = "Update accepts non-empty code")]
    public void Validate_WellFormedUpdateRequest_Passes()
    {
        var result = updateValidator.Validate(new UpdateEventValidationTemplateRequest("return true;"));

        Assert.True(result.IsValid);
    }
}
