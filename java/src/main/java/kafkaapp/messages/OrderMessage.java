package kafkaapp.messages;

/**
 * DTO для сообщений топика orders.
 * Используется и JSON-, и Protobuf-сериализатором.
 */
public record OrderMessage(int id, String description) {

    public static final OrderMessage EMPTY = new OrderMessage(0, "");
}
