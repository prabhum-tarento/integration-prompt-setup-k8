using System.Dynamic;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace IIS.WMS.Consumer.Domain.ValueObjects;

/// <summary>
/// Deserializes arbitrary JSON into an <see cref="ExpandoObject"/> tree - <c>System.Text.Json</c> has
/// no built-in support for this (unlike Newtonsoft's <c>ExpandoObjectConverter</c>), and pulling
/// Newtonsoft into this project would violate the Domain layer's zero-third-party-dependency rule
/// (dotnet-architecture-good-practices.instructions.md §3) - so <see cref="OrderDetail"/> carries this
/// converter itself rather than reaching for the package already used one layer up in Infrastructure.
/// Read-only: nothing in this codebase serializes an <see cref="OrderDetail"/> back through
/// <see cref="JsonSerializer"/> (Infrastructure re-derives its own JSON from <see cref="OrderDetail.Json"/>
/// directly), so <see cref="Write"/> is intentionally unsupported rather than speculatively implemented.
/// </summary>
internal sealed class ExpandoObjectJsonConverter : JsonConverter<ExpandoObject>
{
    public override ExpandoObject? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        using var document = JsonDocument.ParseValue(ref reader);
        return ConvertElement(document.RootElement) as ExpandoObject;
    }

    public override void Write(Utf8JsonWriter writer, ExpandoObject value, JsonSerializerOptions options) =>
        throw new NotSupportedException($"{nameof(ExpandoObjectJsonConverter)} only supports reading JSON into an {nameof(ExpandoObject)}, never writing one back out.");

    private static object? ConvertElement(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Object => ConvertObject(element),
        JsonValueKind.Array => element.EnumerateArray().Select(ConvertElement).ToList(),
        JsonValueKind.String => element.GetString(),
        JsonValueKind.Number => element.TryGetInt64(out var longValue) ? (object)longValue : element.GetDouble(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null or JsonValueKind.Undefined => null,
        _ => throw new JsonException($"Unsupported JSON value kind '{element.ValueKind}'."),
    };

    private static ExpandoObject ConvertObject(JsonElement element)
    {
        var expando = new ExpandoObject();
        var dictionary = (IDictionary<string, object?>)expando;

        foreach (var property in element.EnumerateObject())
        {
            dictionary[property.Name] = ConvertElement(property.Value);
        }

        return expando;
    }
}
