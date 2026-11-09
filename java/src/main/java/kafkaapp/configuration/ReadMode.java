package kafkaapp.configuration;

/**
 * Режим чтения консюмером.
 * В конфиге задаётся строкой "Single" или "Batch" (без учёта регистра).
 * Неизвестное значение → Batch (режим по умолчанию, как в .NET-версии).
 */
public enum ReadMode {
    Single,
    Batch;

    public static final ReadMode DEFAULT = Batch;

    public static ReadMode fromConfig(String raw) {
        for (ReadMode mode : values()) {
            if (mode.name().equalsIgnoreCase(raw)) {
                return mode;
            }
        }
        return DEFAULT;
    }
}
