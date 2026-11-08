package kafkaapp.services;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import kafkaapp.configuration.KafkaConfigMapper;
import kafkaapp.configuration.KafkaOptions;
import kafkaapp.configuration.ProducerOptions;
import kafkaapp.messages.OrderMessage;
import kafkaapp.serialization.SerializerRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Единый сервис для всех продюсеров.
 * Запускает по одной задаче на каждый включённый (Enabled=true) продюсер параллельно;
 * каждая шлёт сообщения с периодичностью SendIntervalMs до остановки сервиса.
 */
public final class ProducersService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ProducersService.class);

    /**
     * Формат времени в Description сообщений: UTC round-trip ("O" в .NET),
     * 7 дробных знаков и явный сдвиг +00:00.
     */
    private static final DateTimeFormatter UTC_ROUND_TRIP = new DateTimeFormatterBuilder()
        .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
        .appendFraction(ChronoField.NANO_OF_SECOND, 7, 7, true)
        .appendLiteral("+00:00")
        .toFormatter()
        .withZone(ZoneOffset.UTC);

    private final List<ProducerOptions> producers;
    private final KafkaOptions kafkaOptions;
    private final SerializerRegistry serializerRegistry;
    private final Clock clock;
    private final ConcurrentHashMap<String, Integer> counters = new ConcurrentHashMap<>();

    private final List<ProducerHandle> handles = new ArrayList<>();

    public ProducersService(List<ProducerOptions> producers,
                            KafkaOptions kafkaOptions,
                            SerializerRegistry serializerRegistry) {
        this(producers, kafkaOptions, serializerRegistry, Clock.systemUTC());
    }

    ProducersService(List<ProducerOptions> producers,
                     KafkaOptions kafkaOptions,
                     SerializerRegistry serializerRegistry,
                     Clock clock) {
        this.producers = producers;
        this.kafkaOptions = kafkaOptions;
        this.serializerRegistry = serializerRegistry;
        this.clock = clock;
    }

    /** Запускает продюсеров. Не блокирует вызывающий поток. */
    public void start() {
        log.info("ProducersService: starting {} producer(s) in parallel.", producers.size());

        for (ProducerOptions options : producers) {
            log.info("Producer '{}' starting. Topic={}, SerializationFormat={}",
                options.name(), options.topic(), options.serializationFormat());

            KafkaProducer<String, OrderMessage> producer = createProducer(options);
            ScheduledExecutorService timer =
                Executors.newSingleThreadScheduledExecutor(r -> daemonThread(r, options.name()));
            timer.scheduleWithFixedDelay(
                () -> sendMessage(producer, options),
                options.sendIntervalMs(),
                options.sendIntervalMs(),
                TimeUnit.MILLISECONDS);

            synchronized (this) {
                handles.add(new ProducerHandle(options.name(), producer, timer));
            }
        }
    }

    private KafkaProducer<String, OrderMessage> createProducer(ProducerOptions options) {
        var props = KafkaConfigMapper.toProducerProperties(kafkaOptions, options);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Экземпляр serde передаётся через конструктор (аналог SetValueSerializer в .NET):
        // Properties-конфиг Kafka-клиента принимает только имя класса, не экземпляр.
        return new KafkaProducer<>(props, null,
            serializerRegistry.resolve(options.serializationFormat()));
    }

    private void sendMessage(KafkaProducer<String, OrderMessage> producer, ProducerOptions options) {
        int counter = counters.merge(options.name(), 1, Integer::sum);
        String key = UUID.randomUUID().toString().replace("-", "");
        OrderMessage value = new OrderMessage(
            counter,
            "[%s] Hello Kafka @ %s".formatted(options.name(), UTC_ROUND_TRIP.format(Instant.now(clock))));

        try {
            RecordMetadata meta = producer.send(
                new ProducerRecord<>(options.topic(), key, value)).get();
            log.info("Producer '{}' delivered Id={} Description={} to {}[{}]@{} Key={}",
                options.name(), value.id(), value.description(),
                meta.topic(), meta.partition(), meta.offset(), key);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Producer '{}' interrupted while sending Id={}", options.name(), value.id());
        } catch (Exception e) {
            log.error("Producer '{}' failed to send message. Error={}",
                options.name(), e.getMessage(), e);
        }
    }

    private static Thread daemonThread(Runnable r, String producerName) {
        Thread t = new Thread(r, "producer-" + producerName);
        t.setDaemon(true);
        return t;
    }

    /** Останавливает таймеры и флашит все продюсеры (ожидает доставки накопленного). */
    @Override
    public void close() {
        for (ProducerHandle handle : handles) {
            handle.timer().shutdownNow();
        }
        for (ProducerHandle handle : handles) {
            try {
                handle.producer().flush();
                handle.producer().close();
                log.info("Producer '{}' stopped.", handle.name());
            } catch (RuntimeException e) {
                log.warn("Producer '{}': error while closing: {}", handle.name(), e.getMessage());
            }
        }
    }

    private record ProducerHandle(String name,
                                  KafkaProducer<String, OrderMessage> producer,
                                  ScheduledExecutorService timer) {
    }
}
