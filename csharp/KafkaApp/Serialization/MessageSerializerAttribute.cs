using KafkaApp.Configuration;

namespace KafkaApp.Serialization;

[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class MessageSerializerAttribute(SerializationFormat format) : Attribute
{
    public SerializationFormat Format { get; } = format;
}
