using System.Dynamic;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using IIS.WMS.Consumer.Domain.ValueObjects;

namespace IIS.WMS.Consumer.UnitTests.Domain;

/// <summary>
/// Direct tests for the internal <c>ExpandoObjectJsonConverter</c>. The Domain project declares no
/// <c>InternalsVisibleTo</c> for this test assembly (adding one would be a production-source change
/// outside this task's scope - see the class's own doc comment for why it stays internal), so the
/// converter type is located by name via reflection and constructed with a non-public constructor;
/// once constructed, it is used purely through its public <see cref="JsonConverter{T}"/> base contract
/// (<c>Read</c>/<c>Write</c> are both public members of that base class), which requires no further
/// reflection. <see cref="OrderDetailTests"/> exercises the same Read path indirectly through
/// <see cref="OrderDetail.FromJson"/>; this file drives the converter directly so the intentionally
/// unsupported <c>Write</c> direction (never reached through <see cref="OrderDetail"/>) is covered too.
/// </summary>
public class ExpandoObjectJsonConverterTests
{
    private static readonly JsonConverter<ExpandoObject> Converter = CreateConverter();

    private static JsonConverter<ExpandoObject> CreateConverter()
    {
        var type = typeof(OrderDetail).Assembly.GetType(
            "IIS.WMS.Consumer.Domain.ValueObjects.ExpandoObjectJsonConverter", throwOnError: true)!;

        return (JsonConverter<ExpandoObject>)Activator.CreateInstance(type, nonPublic: true)!;
    }

    private static ExpandoObject Read(string json)
    {
        var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));
        return Converter.Read(ref reader, typeof(ExpandoObject), new JsonSerializerOptions())!;
    }

    [Fact(DisplayName = "Read converts a JSON object's properties into ExpandoObject dictionary entries")]
    public void Read_JsonObject_ConvertsPropertiesToDictionaryEntries()
    {
        var result = Read("{\"sku\":\"SKU1\",\"quantity\":10}");

        var dictionary = (IDictionary<string, object?>)result;
        Assert.Equal("SKU1", dictionary["sku"]);
        Assert.Equal(10L, dictionary["quantity"]);
    }

    [Fact(DisplayName = "Read converts a nested JSON object into a nested ExpandoObject")]
    public void Read_NestedJsonObject_ConvertsToNestedExpandoObject()
    {
        var result = Read("{\"outer\":{\"inner\":\"value\"}}");

        var outer = (IDictionary<string, object?>)result;
        var inner = Assert.IsType<ExpandoObject>(outer["outer"]);
        Assert.Equal("value", ((IDictionary<string, object?>)inner)["inner"]);
    }

    [Fact(DisplayName = "Read converts a JSON array into a List of converted elements")]
    public void Read_JsonArray_ConvertsToListOfElements()
    {
        var result = Read("{\"tags\":[\"a\",\"b\",1]}");

        var dictionary = (IDictionary<string, object?>)result;
        var list = Assert.IsType<List<object?>>(dictionary["tags"]);
        Assert.Equal(["a", "b", 1L], list);
    }

    [Fact(DisplayName = "Read converts an array of nested objects into a list of ExpandoObjects")]
    public void Read_ArrayOfObjects_ConvertsToListOfExpandoObjects()
    {
        var result = Read("{\"items\":[{\"id\":1},{\"id\":2}]}");

        var dictionary = (IDictionary<string, object?>)result;
        var list = Assert.IsType<List<object?>>(dictionary["items"]);
        Assert.Equal(2, list.Count);
        Assert.Equal(1L, ((IDictionary<string, object?>)Assert.IsType<ExpandoObject>(list[0]))["id"]);
        Assert.Equal(2L, ((IDictionary<string, object?>)Assert.IsType<ExpandoObject>(list[1]))["id"]);
    }

    [Fact(DisplayName = "Read converts a whole-valued JSON number to Int64")]
    public void Read_IntegerValuedNumber_ConvertsToInt64()
    {
        var result = Read("{\"quantity\":42}");

        Assert.IsType<long>(((IDictionary<string, object?>)result)["quantity"]);
    }

    [Fact(DisplayName = "Read converts a fractional JSON number to Double")]
    public void Read_FractionalNumber_ConvertsToDouble()
    {
        var result = Read("{\"price\":3.14}");

        Assert.Equal(3.14, ((IDictionary<string, object?>)result)["price"]);
    }

    [Fact(DisplayName = "Read converts JSON true/false to CLR bool")]
    public void Read_BooleanValues_ConvertToClrBool()
    {
        var result = Read("{\"active\":true,\"deleted\":false}");

        var dictionary = (IDictionary<string, object?>)result;
        Assert.Equal(true, dictionary["active"]);
        Assert.Equal(false, dictionary["deleted"]);
    }

    [Fact(DisplayName = "Read converts JSON null to a CLR null entry")]
    public void Read_JsonNull_ConvertsToNullEntry()
    {
        var result = Read("{\"reason\":null}");

        var dictionary = (IDictionary<string, object?>)result;
        Assert.True(dictionary.ContainsKey("reason"));
        Assert.Null(dictionary["reason"]);
    }

    [Fact(DisplayName = "Write throws NotSupportedException - this converter only supports reading")]
    public void Write_AnyValue_ThrowsNotSupportedException()
    {
        using var stream = new MemoryStream();
        using var writer = new Utf8JsonWriter(stream);
        var value = Read("{\"sku\":\"SKU1\"}");

        var exception = Assert.Throws<NotSupportedException>(
            () => Converter.Write(writer, value, new JsonSerializerOptions()));

        Assert.Contains("only supports reading", exception.Message);
    }
}
