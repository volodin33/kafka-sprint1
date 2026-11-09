package kafkaapp.serialization;

import com.google.protobuf.InvalidProtocolBufferException;
import kafkaapp.messages.OrderMessage;

/**
 * Сериализатор {@link OrderMessage} через protobuf-java (schemaless, голые байты,
 * без Schema Registry). Формат wire совместим со схемой:
 * {@code message OrderMessage { int32 id = 1; string description = 2; }}.
 * Default-значения protobuf не кодируются — как в .NET-версии.
 * Tombstone (null) и пустой payload → пустое сообщение (пустой payload —
 * легальный wire-формат), как в .NET-версии.
 */
public final class ProtobufOrderMessageSerializer implements OrderMessageSerde {

    @Override
    public byte[] serialize(String topic, OrderMessage data) {
        if (data == null) {
            return null;
        }
        kafkaapp.proto.OrderMessage proto = kafkaapp.proto.OrderMessage.newBuilder()
            .setId(data.id())
            .setDescription(data.description())
            .build();
        return proto.toByteArray();
    }

    @Override
    public OrderMessage deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return OrderMessage.EMPTY;
        }
        try {
            kafkaapp.proto.OrderMessage proto = kafkaapp.proto.OrderMessage.parseFrom(data);
            return new OrderMessage(proto.getId(), proto.getDescription());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to deserialize OrderMessage from Protobuf", e);
        }
    }
}
