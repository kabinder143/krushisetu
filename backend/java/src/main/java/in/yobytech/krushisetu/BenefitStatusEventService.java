
package in.yobytech.krushisetu;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Set;

@Service
public class BenefitStatusEventService {

    private static final Set<String> ALLOWED_STATUSES = Set.of(
            "PENDING", "APPROVED", "REJECTED", "DISBURSED"
    );

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public BenefitStatusEventService(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public String process(ConsumerRecord<String, String> record)
            throws Exception {

        JsonNode event = objectMapper.readTree(record.value());

        if (event == null || !event.isObject()) {
            throw new IllegalArgumentException("Event must be a JSON object");
        }

        String eventId = requiredText(event, "eventId");
        String eventType = requiredText(event, "eventType");

        if (!"BenefitStatusUpdated".equals(eventType)) {
            throw new IllegalArgumentException(
                    "Unexpected eventType: " + eventType);
        }

        JsonNode data = event.get("data");

        if (data == null || !data.isObject()) {
            throw new IllegalArgumentException(
                    "Missing or invalid data object");
        }

        String farmerId = requiredText(data, "farmerId");
        String status = requiredText(data, "benefitStatus");

        if (!ALLOWED_STATUSES.contains(status)) {
            throw new IllegalArgumentException(
                    "Invalid benefitStatus: " + status);
        }

        Integer farmerCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM farmers WHERE farmer_id = ?",
                Integer.class,
                farmerId
        );

        if (farmerCount == null || farmerCount == 0) {
            throw new IllegalArgumentException(
                    "Farmer does not exist: " + farmerId);
        }

        // Insert benefit status. Duplicate event IDs are ignored.
        jdbcTemplate.update("""
                INSERT INTO benefit_status
                    (farmer_id, benefit_status, event_id, updated_at)
                VALUES (?, ?, ?, NOW())
                ON CONFLICT (event_id) DO NOTHING
                """,
                farmerId,
                status,
                eventId
        );

        // Record the Kafka event for monitoring.
        jdbcTemplate.update("""
                INSERT INTO farmer_event_receipts
                    (event_id, event_type, farmer_id,
                     kafka_topic, kafka_partition, kafka_offset)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (event_id) DO NOTHING
                """,
                eventId,
                eventType,
                farmerId,
                record.topic(),
                record.partition(),
                record.offset()
        );

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
