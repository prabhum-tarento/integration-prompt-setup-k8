using System.Text.Json;

namespace IIS.WMS.Consumer.Domain.ValueObjects;

/// <summary>
/// Value Object wrapping one archived event's JSON body on <see cref="Aggregates.OrderArchive"/>. The
/// <c>OrderArchive</c> container holds records for every schema this service relays
/// (<c>InventoryStateChangedEvent</c>, <c>InventoryAdjustedEvent</c>, and future ones), each with its
/// own shape - rather than the aggregate binding to one CLR type, this deliberately stays
/// <see langword="dynamic"/> and lets a caller navigate whatever shape <see cref="Json"/> actually
/// contains. No identity of its own - two instances are equal when their underlying JSON is, which is
/// what <see cref="Data"/> (an <see cref="System.Dynamic.ExpandoObject"/>) doesn't give for free.
/// </summary>
public sealed class OrderDetail : IEquatable<OrderDetail>
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        Converters = { new ExpandoObjectJsonConverter() },
    };

    /// <summary>The archived event's JSON body, verbatim - what actually gets persisted (see <c>OrderArchiveMapper</c>).</summary>
    public string Json { get; }

    /// <summary>Dynamic view over <see cref="Json"/> - navigate it by property/array access without a fixed CLR shape.</summary>
    public dynamic Data { get; }

    private OrderDetail(string json, dynamic data)
    {
        Json = json;
        Data = data;
    }

    /// <summary>Parses <paramref name="json"/> into a Value Object - throws <see cref="JsonException"/> if it isn't well-formed JSON.</summary>
    public static OrderDetail FromJson(string json)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(json);

        var data = JsonSerializer.Deserialize<System.Dynamic.ExpandoObject>(json, SerializerOptions)
            ?? throw new ArgumentException("OrderDetail JSON must not deserialize to a null value.", nameof(json));

        return new OrderDetail(json, data);
    }

    /// <summary>Value equality by underlying JSON text, not by reference or by <see cref="Data"/> (which has none of its own).</summary>
    public bool Equals(OrderDetail? other) => other is not null && Json == other.Json;

    public override bool Equals(object? obj) => Equals(obj as OrderDetail);

    public override int GetHashCode() => Json.GetHashCode(StringComparison.Ordinal);

    public override string ToString() => Json;

    public static bool operator ==(OrderDetail? left, OrderDetail? right) => Equals(left, right);

    public static bool operator !=(OrderDetail? left, OrderDetail? right) => !Equals(left, right);
}
