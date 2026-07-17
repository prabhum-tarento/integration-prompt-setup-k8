using IIS.WMS.Consumer.Domain.ValueObjects;

namespace IIS.WMS.Consumer.UnitTests.Domain;

/// <summary>
/// Tests the <see cref="OrderDetail"/> Value Object - JSON parsing (via <see cref="OrderDetail.FromJson"/>,
/// which also exercises <c>ExpandoObjectJsonConverter</c>'s Read direction - see
/// <see cref="ExpandoObjectJsonConverterTests"/> for that converter's own direct tests) and the
/// by-JSON value-equality contract.
/// </summary>
public class OrderDetailTests
{
    [Fact(DisplayName = "FromJson preserves the original JSON text verbatim and exposes it dynamically via Data")]
    public void FromJson_ValidJson_PreservesJsonAndExposesDataDynamically()
    {
        var detail = OrderDetail.FromJson("{\"sku\":\"SKU1\",\"quantity\":10}");

        Assert.Equal("{\"sku\":\"SKU1\",\"quantity\":10}", detail.Json);
        Assert.Equal("SKU1", (string)detail.Data.sku);
        Assert.Equal(10L, (long)detail.Data.quantity);
    }

    [Theory(DisplayName = "FromJson throws when the JSON is null, empty, or whitespace")]
    [InlineData("")]
    [InlineData(" ")]
    public void FromJson_BlankJson_ThrowsArgumentException(string json)
    {
        Assert.Throws<ArgumentException>(() => OrderDetail.FromJson(json));
    }

    [Fact(DisplayName = "FromJson throws when the JSON is null")]
    public void FromJson_NullJson_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => OrderDetail.FromJson(null!));
    }

    [Fact(DisplayName = "FromJson throws when the JSON is malformed")]
    public void FromJson_MalformedJson_ThrowsJsonException()
    {
        Assert.Throws<System.Text.Json.JsonException>(() => OrderDetail.FromJson("not-json"));
    }

    [Fact(DisplayName = "FromJson throws when the JSON deserializes to a null value")]
    public void FromJson_JsonLiteralNull_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => OrderDetail.FromJson("null"));
    }

    [Fact(DisplayName = "Equals returns true for two instances built from the same JSON text")]
    public void Equals_SameJsonText_ReturnsTrue()
    {
        var first = OrderDetail.FromJson("{\"sku\":\"SKU1\"}");
        var second = OrderDetail.FromJson("{\"sku\":\"SKU1\"}");

        Assert.True(first.Equals(second));
        Assert.True(first.Equals((object)second));
        Assert.True(first == second);
        Assert.False(first != second);
    }

    [Fact(DisplayName = "Equals returns false for instances built from different JSON text")]
    public void Equals_DifferentJsonText_ReturnsFalse()
    {
        var first = OrderDetail.FromJson("{\"sku\":\"SKU1\"}");
        var second = OrderDetail.FromJson("{\"sku\":\"SKU2\"}");

        Assert.False(first.Equals(second));
        Assert.False(first.Equals((object)second));
        Assert.False(first == second);
        Assert.True(first != second);
    }

    [Fact(DisplayName = "Equals returns false when compared against null or an unrelated type")]
    public void Equals_NullOrUnrelatedType_ReturnsFalse()
    {
        var detail = OrderDetail.FromJson("{\"sku\":\"SKU1\"}");

        Assert.False(detail.Equals(null));
        Assert.False(detail.Equals((object?)null));
        Assert.False(detail.Equals((object)"not-an-order-detail"));
    }

    [Fact(DisplayName = "The equality operators treat two null references as equal")]
    public void EqualityOperator_BothNull_ReturnsTrue()
    {
        OrderDetail? left = null;
        OrderDetail? right = null;

        Assert.True(left == right);
        Assert.False(left != right);
    }

    [Fact(DisplayName = "The equality operators treat a null and a non-null reference as unequal")]
    public void EqualityOperator_OneNull_ReturnsFalse()
    {
        var detail = OrderDetail.FromJson("{\"sku\":\"SKU1\"}");

        Assert.False(detail == null);
        Assert.True(detail != null);
        Assert.False(null == detail);
        Assert.True(null != detail);
    }

    [Fact(DisplayName = "GetHashCode is consistent for two instances with equal JSON text")]
    public void GetHashCode_EqualInstances_ReturnsSameValue()
    {
        var first = OrderDetail.FromJson("{\"sku\":\"SKU1\"}");
        var second = OrderDetail.FromJson("{\"sku\":\"SKU1\"}");

        Assert.Equal(first.GetHashCode(), second.GetHashCode());
    }

    [Fact(DisplayName = "ToString returns the underlying JSON text")]
    public void ToString_Always_ReturnsUnderlyingJson()
    {
        var detail = OrderDetail.FromJson("{\"sku\":\"SKU1\"}");

        Assert.Equal("{\"sku\":\"SKU1\"}", detail.ToString());
    }
}
