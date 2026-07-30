namespace IIS.WMS.Consumer.Infrastructure;

/// <summary>
/// Bound from the <c>FeatureFlags</c> configuration section - gates the §3.6/3.7/3.8 downstream
/// publishes ported from the upstream Reflex facade's <c>ApplicationConfig</c> flags of the same
/// names (docs/events/inventory.InventoryStateChanged.md). Unlike Reflex, where the corresponding
/// sends are commented out entirely, this service publishes for real when a flag is enabled.
/// </summary>
public sealed class FeatureFlagsOptions
{
    /// <summary>Configuration section name this options type binds from.</summary>
    public const string SectionName = "FeatureFlags";

    /// <summary>Gates §3.6 B2B adjusted/moved publishing for non-EDC/non-ADC locations.</summary>
    public bool EnableDeltaTowardsSap { get; init; }

    /// <summary>Gates §3.6 B2B adjusted/moved publishing for the CAECOM (third-party-logistics) location.</summary>
    public bool EnableDeltaTowardsAx123Pl { get; init; }

    /// <summary>Gates §3.6 B2B adjusted/moved publishing for the ADC location.</summary>
    public bool EnableAdcDeltaTowardsAx12 { get; init; }

    /// <summary>Gates §3.7 OMS delta publishing for non-3PL locations.</summary>
    public bool EnableDeltaTowardsOms { get; init; }

    /// <summary>Gates §3.7 OMS delta publishing for third-party-logistics locations.</summary>
    public bool EnableDeltaTowardsOms3Pl { get; init; }

    /// <summary>Gates §3.8 Inventory Comparison Report snapshot publishing.</summary>
    public bool EnableSnapshotForIcr { get; init; }
}
