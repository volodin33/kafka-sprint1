package kafkaapp.configuration;

/**
 * Режим коммитта offset'ов консюмером.
 * В конфиге задаётся строкой "Manual" или "Auto" (без учёта регистра).
 */
public enum CommitMode {
    Manual,
    Auto;

    public static CommitMode fromConfig(String raw) {
        for (CommitMode mode : values()) {
            if (mode.name().equalsIgnoreCase(raw)) {
                return mode;
            }
        }
        // Как в .NET-версии: strict-валидация удалена, мусорная строка → дефолт.
        return Manual;
    }
}
