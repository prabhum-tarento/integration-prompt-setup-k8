using System.Text.Json;
using Confluent.Kafka;

namespace IIS.WMS.Consumer.Infrastructure.Messaging.Kafka;

/// <summary>
/// Wires <see cref="JsonSerializer"/> into <see cref="ConsumerBuilder{TKey,TValue}.SetValueDeserializer"/>
/// so a JSON-contract consumer fails the same way an Avro one does: any bad payload throws inside
/// <c>Consume()</c> itself (surfaced as a <see cref="ConsumeException"/> with <c>ConsumerRecord</c>
/// populated), rather than requiring a separate manual deserialize-and-catch step downstream. This
/// is what lets <see cref="ConsumerHostedService"/> handle poison messages identically for
/// both wire formats.
/// </summary>
public sealed class JsonDeserializer<T> : IDeserializer<T>
{
    /// <summary>Deserializes the message value as JSON, or throws <see cref="JsonException"/> for a null (tombstone) value or malformed payload.</summary>
    /// <param name="data">Raw message value bytes.</param>
    /// <param name="isNull">Whether the message value was a Kafka tombstone (null).</param>
    /// <param name="context">Deserialization context (topic/partition/component) - unused, this deserializer doesn't vary by it.</param>
    /// <returns>The deserialized value.</returns>
    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull)
        {
            throw new JsonException("Message value was null (tombstone) - no JSON payload to deserialize.");
        }

        return JsonSerializer.Deserialize<T>(data) ?? throw new JsonException("Deserialized payload was null.");
    }
}
