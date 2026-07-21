using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using IIS.WMS.Consumer.Infrastructure.Messaging.Shared.Kafka;

namespace IIS.WMS.Consumer.UnitTests.Infrastructure;

/// <summary>
/// Correctness tests for <see cref="JsonDeserializer{T}"/> - wires <see cref="JsonSerializer"/> into
/// <see cref="IDeserializer{T}"/> so a JSON-contract Kafka consumer fails the same way an Avro one
/// does: any bad payload throws inside <c>Consume()</c> itself
/// (integration-resiliency.instructions.md §1).
/// </summary>
public class JsonDeserializerTests
{
    private static readonly SerializationContext Context = new(MessageComponentType.Value, "test-topic", new Headers());

    private readonly JsonDeserializer<TestPayload> sut = new();

    [Fact(DisplayName = "Deserialize returns the deserialized value for a well-formed JSON payload")]
    public void Deserialize_ValidJson_ReturnsValue()
    {
        var data = Encoding.UTF8.GetBytes("""{"Name":"widget","Value":42}""");

        var result = sut.Deserialize(data, isNull: false, Context);

        Assert.Equal("widget", result.Name);
        Assert.Equal(42, result.Value);
    }

    [Fact(DisplayName = "Deserialize throws JsonException when isNull is true (Kafka tombstone)")]
    public void Deserialize_IsNullTrue_ThrowsJsonException()
    {
        var ex = Assert.Throws<JsonException>(() => sut.Deserialize(ReadOnlySpan<byte>.Empty, isNull: true, Context));

        Assert.Contains("tombstone", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact(DisplayName = "Deserialize throws JsonException for a malformed JSON payload")]
    public void Deserialize_MalformedJson_ThrowsJsonException()
    {
        var data = Encoding.UTF8.GetBytes("{not-valid-json");

        Assert.Throws<JsonException>(() => sut.Deserialize(data, isNull: false, Context));
    }

    [Fact(DisplayName = "Deserialize throws JsonException when the JSON payload is the literal null")]
    public void Deserialize_JsonNullLiteral_ThrowsJsonException()
    {
        var data = Encoding.UTF8.GetBytes("null");

        var ex = Assert.Throws<JsonException>(() => sut.Deserialize(data, isNull: false, Context));

        Assert.Contains("was null", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private sealed record TestPayload(string Name, int Value);
}
