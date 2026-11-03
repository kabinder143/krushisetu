package in.yobytech.krushisetu;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class FarmerEventReceiptService {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public FarmerEventReceiptService(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public String recordEvent(ConsumerRecord<String, String> record)
            throws Exception {

        JsonNode event = objectMapper.readTree(record.value());

        if (event == null || !event.isObject()) {
            throw new IllegalArgumentException("Event must be a JSON object");
        }

        String eventId = requiredText(event, "eventId");
        String eventType = requiredText(event, "eventType");

        if (!"FarmerRegistered".equals(eventType)) {
            throw new IllegalArgumentException(
                    "Unexpected eventType: " + eventType);
        }

        // Support both legacy flat events and newer nested-data events.
        JsonNode data = event.get("data");

        if (data == null || data.isNull()) {
            data = event;
        }

        if (!data.isObject()) {
            throw new IllegalArgumentException(
                    "Event data must be a JSON object");
        }

        String farmerId = requiredText(data, "farmerId");

        String sql = """
                INSERT INTO farmer_event_receipts
                    (event_id, event_type, farmer_id,
                     kafka_topic, kafka_partition, kafka_offset)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (event_id) DO NOTHING
                """;

        jdbcTemplate.update(sql,
                eventId,
                eventType,
                farmerId,
                record.topic(),
                record.partition(),
                record.offset());

        return eventId;
    }

    private String requiredText(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);

        if (value == null || !value.isTextual()
                || value.asText().isBlank()) {
            throw new IllegalArgumentException(
                    "Missing or blank field: " + field);
        }

        return value.asText();
    }
}
