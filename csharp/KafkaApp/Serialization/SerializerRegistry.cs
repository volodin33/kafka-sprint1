using System.Reflection;
using KafkaApp.Configuration;

namespace KafkaApp.Serialization;

/// <summary>
/// Реестр сериализаторов сообщений.
/// </summary>
public sealed class SerializerRegistry
{
    private readonly IReadOnlyDictionary<SerializationFormat, IOrderMessageSerializer> _serializers;
    public SerializerRegistry(Assembly? assembly = null)
    {
        var asm = assembly ?? Assembly.GetAssembly(typeof(SerializerRegistry))
            ?? throw new InvalidOperationException("Cannot determine assembly for serializer discovery.");

        var map = new Dictionary<SerializationFormat, IOrderMessageSerializer>();

        foreach (var type in asm.GetTypes())
        {
            var attr = type.GetCustomAttribute<MessageSerializerAttribute>();
            if (attr is null)
                continue;

            if (!typeof(IOrderMessageSerializer).IsAssignableFrom(type))
                throw new InvalidOperationException(
                    $"Type '{type.FullName}' has [{nameof(MessageSerializerAttribute)}] but does not implement {nameof(IOrderMessageSerializer)}.");

            var instance = (IOrderMessageSerializer)(Activator.CreateInstance(type)
                ?? throw new InvalidOperationException($"Cannot create instance of '{type.FullName}'."));

            map[attr.Format] = instance;
        }

        if (map.Count == 0)
            throw new InvalidOperationException(
                "No serializers found in assembly. Add classes with [MessageSerializer(...)] attribute.");

        _serializers = map;
    }

    public IOrderMessageSerializer Resolve(SerializationFormat format)
    {
        if (_serializers.TryGetValue(format, out var serializer))
            return serializer;

        var registered = string.Join(", ", _serializers.Keys);
        throw new InvalidOperationException(
            $"No serializer registered for format '{format}'. Registered: {registered}.");
    }
}
