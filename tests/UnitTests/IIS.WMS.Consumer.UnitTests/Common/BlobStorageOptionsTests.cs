using IIS.WMS.Common.BlobStorage;

namespace IIS.WMS.Consumer.UnitTests.Common;

/// <summary>
/// Correctness tests for <see cref="BlobStorageOptions"/> and <see cref="BlobStorageAccountOptions"/> -
/// bound from the <c>BlobStorage</c> configuration section (integration-resiliency.instructions.md §5).
/// </summary>
public class BlobStorageOptionsTests
{
    [Fact(DisplayName = "A freshly constructed instance exposes the documented container-name defaults and disabled request audit")]
    public void Constructor_NoOverrides_ExposesDefaults()
    {
        var options = new BlobStorageOptions();

        Assert.Equal("request-audit", options.RequestAuditContainerName);
        Assert.Equal("consumer-dead-letter", options.ConsumerDeadLetterContainerName);
        Assert.Equal("audit-dead-letter", options.AuditDeadLetterContainerName);
        Assert.Equal("validation-templates", options.ValidationTemplateContainerName);
        Assert.Equal("large-payload", options.LargePayloadContainerName);
        Assert.False(options.RequestAuditEnabled);
        Assert.NotNull(options.Hot);
        Assert.NotNull(options.Cold);
    }

    [Fact(DisplayName = "Every settable property can be overridden via object initializer")]
    public void ObjectInitializer_AllPropertiesSet_ExposesConfiguredValues()
    {
        var hot = new BlobStorageAccountOptions { AccountUri = "https://hot.blob.core.windows.net" };
        var cold = new BlobStorageAccountOptions { AccountUri = "https://cold.blob.core.windows.net" };

        var options = new BlobStorageOptions
        {
            RequestAuditContainerName = "custom-request-audit",
            ConsumerDeadLetterContainerName = "custom-dead-letter",
            AuditDeadLetterContainerName = "custom-audit-dead-letter",
            ValidationTemplateContainerName = "custom-validation-templates",
            LargePayloadContainerName = "custom-large-payload",
            RequestAuditEnabled = true,
            Hot = hot,
            Cold = cold,
        };

        Assert.Equal("custom-request-audit", options.RequestAuditContainerName);
        Assert.Equal("custom-dead-letter", options.ConsumerDeadLetterContainerName);
        Assert.Equal("custom-audit-dead-letter", options.AuditDeadLetterContainerName);
        Assert.Equal("custom-validation-templates", options.ValidationTemplateContainerName);
        Assert.Equal("custom-large-payload", options.LargePayloadContainerName);
        Assert.True(options.RequestAuditEnabled);
        Assert.Same(hot, options.Hot);
        Assert.Same(cold, options.Cold);
    }

    [Fact(DisplayName = "AccountUri can be set via object initializer")]
    public void AccountOptions_AccountUriSet_ExposesConfiguredValue()
    {
        var account = new BlobStorageAccountOptions { AccountUri = "https://myaccount.blob.core.windows.net" };

        Assert.Equal("https://myaccount.blob.core.windows.net", account.AccountUri);
    }
}
