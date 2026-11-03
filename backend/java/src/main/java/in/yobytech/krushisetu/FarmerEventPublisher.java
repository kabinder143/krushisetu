package in.yobytech.krushisetu;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Service
public class FarmerEventPublisher {

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper;
    private final String topic;

    public FarmerEventPublisher(
            ObjectMapper objectMapper,
            @Value("${app.kafka.topic:farmer.registered}") String topic,
            @Value("${app.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers) {

        this.objectMapper = objectMapper;
        this.topic = topic;

        var properties = new java.util.Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "all");
        properties.put("enable.idempotence", "true");

        this.producer = new KafkaProducer<>(properties);
    }

    public PublishedEvent publish(FarmerRegistrationRequest request, String farmerId)
            throws JsonProcessingException, ExecutionException, InterruptedException {

        String eventId = UUID.randomUUID().toString();
        String correlationId = UUID.randomUUID().toString();

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("farmerId", farmerId);
        data.put("farmerName", request.farmerName());
        data.put("district", request.district());
        data.put("village", request.village());
        data.put("registrationStatus", "SUBMITTED");

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventId", eventId);
        event.put("eventType", "FarmerRegistered");
        event.put("source", "farmer-registration-java");
        event.put("occurredAt", Instant.now().toString());
        event.put("correlationId", correlationId);
        event.put("data", data);

        String json = objectMapper.writeValueAsString(event);

        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, farmerId, json);

        RecordMetadata metadata = producer.send(record).get();

        return new PublishedEvent(
                eventId,
                correlationId,
                farmerId,
                metadata.topic(),
                metadata.partition(),
                metadata.offset()
        );
    }

    public record PublishedEvent(
            String eventId,
            String correlationId,
            String farmerId,
            String topic,
            int partition,
            long offset
    ) {}

    @jakarta.annotation.PreDestroy
    public void close() {
        producer.close();
    }
}
