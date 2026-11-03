package in.yobytech.krushisetu;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class FarmerRegisteredConsumer {

    private static final String GROUP_ID =
            "krushisetu-java-farmer-consumer";

    private final FarmerEventReceiptService receiptService;
    private final String bootstrapServers;
    private final String topic;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile KafkaConsumer<String, String> consumer;
    private volatile Thread worker;

    public FarmerRegisteredConsumer(
            FarmerEventReceiptService receiptService,
            @Value("${app.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${app.kafka.topic}") String topic) {
        this.receiptService = receiptService;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        worker = new Thread(this::consumeLoop, "farmer-registered-consumer");
        worker.setDaemon(false);
        worker.start();
    }

    private void consumeLoop() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        try (KafkaConsumer<String, String> kafkaConsumer =
                     new KafkaConsumer<>(properties)) {
            consumer = kafkaConsumer;
            kafkaConsumer.subscribe(Collections.singletonList(topic));

            System.out.println("[FarmerConsumer] Started"
                    + " | topic=" + topic
                    + " | group=" + GROUP_ID);

            while (running.get()) {
                var records = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    if (!running.get()) {
                        break;
                    }

                    try {
                        // Returns only after the transactional DB method succeeds.
                        String eventId = receiptService.recordEvent(record);

                        TopicPartition partition =
                                new TopicPartition(record.topic(), record.partition());

                        // Commit only this record's next offset.
                        kafkaConsumer.commitSync(Map.of(
                                partition,
                                new OffsetAndMetadata(record.offset() + 1)
                        ));

                        System.out.println("[FarmerConsumer] Receipt saved"
                                + " | eventId=" + eventId
                                + " | topic=" + record.topic()
                                + " | partition=" + record.partition()
                                + " | offset=" + record.offset());
                    } catch (Exception e) {
                        System.err.println("[FarmerConsumer] Processing failed"
                                + " | topic=" + record.topic()
                                + " | partition=" + record.partition()
                                + " | offset=" + record.offset()
                                + " | reason=" + e.getMessage());
                        System.err.println(
                                "[FarmerConsumer] Stopping without advancing this offset.");
                        running.set(false);
                        break;
                    }
                }
            }
        } catch (WakeupException e) {
            if (running.get()) {
                System.err.println("[FarmerConsumer] Unexpected wakeup.");
            }
        } catch (Exception e) {
            System.err.println("[FarmerConsumer] Consumer stopped: "
                    + e.getMessage());
        } finally {
            consumer = null;
            running.set(false);
            System.out.println("[FarmerConsumer] Consumer thread stopped.");
        }
    }

    @PreDestroy
    public void stop() {
        running.set(false);

        KafkaConsumer<String, String> activeConsumer = consumer;
        if (activeConsumer != null) {
            activeConsumer.wakeup();
        }

        Thread activeWorker = worker;
        if (activeWorker != null && activeWorker != Thread.currentThread()) {
            try {
                activeWorker.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
