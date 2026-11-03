package in.yobytech.krushisetu;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class FarmerEventProducer {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "all");
        properties.put("enable.idempotence", "true");

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventId", UUID.randomUUID().toString());
        event.put("eventType", "FarmerRegistered");
        event.put("source", "farmer-registration-java");
        event.put("occurredAt", Instant.now().toString());
        event.put("correlationId", UUID.randomUUID().toString());

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("farmerId", "FARM-10001");
        data.put("district", "Rourkela");
        data.put("registrationStatus", "SUBMITTED");

        event.put("data", data);

        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(event);

        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(properties)) {

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            "farmer.registered",
                            (String) data.get("farmerId"),
                            json);

            RecordMetadata metadata = producer.send(record).get();

            System.out.println("Event published successfully.");
            System.out.println("Topic: " + metadata.topic());
            System.out.println("Partition: " + metadata.partition());
            System.out.println("Offset: " + metadata.offset());
            System.out.println("Event JSON: " + json);
        }
    }
}
