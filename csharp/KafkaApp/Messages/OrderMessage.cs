namespace KafkaApp.Messages;

public sealed class OrderMessage
{
    public int Id { get; set; }
    public string Description { get; set; } = string.Empty;
}
