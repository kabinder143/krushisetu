package in.yobytech.krushisetu;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.validation.Valid;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/farmers")
public class FarmerController {

    private final FarmerRepository farmerRepository;
    private final FarmerEventPublisher eventPublisher;

    public FarmerController(
            FarmerRepository farmerRepository,
            FarmerEventPublisher eventPublisher) {
        this.farmerRepository = farmerRepository;
        this.eventPublisher = eventPublisher;
    }

    // Register a new farmer
    @PostMapping
    public ResponseEntity<RegistrationResponse> registerFarmer(
            @Valid @RequestBody FarmerRegistrationRequest request) {

        String farmerId = "FARM-" +
                UUID.randomUUID().toString()
                        .substring(0, 8)
                        .toUpperCase();

        try {
            farmerRepository.save(farmerId, request);
        } catch (DataAccessException e) {
            throw new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "Unable to save farmer registration to PostgreSQL",
                    e
            );
        }

        try {
            FarmerEventPublisher.PublishedEvent published =
                    eventPublisher.publish(request, farmerId);

            RegistrationResponse response = new RegistrationResponse(
                    "Farmer saved to PostgreSQL and registration event published",
                    farmerId,
                    "SUBMITTED",
                    published.eventId(),
                    published.correlationId(),
                    published.topic(),
                    published.partition(),
                    published.offset()
            );

            return ResponseEntity.status(HttpStatus.CREATED).body(response);

        } catch (JsonProcessingException | ExecutionException e) {
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "Farmer was saved to PostgreSQL, but Kafka event publishing failed",
                    e
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "Farmer was saved to PostgreSQL, but Kafka publishing was interrupted",
                    e
            );
        }
    }

    // Retrieve all farmers
    @GetMapping
    public ResponseEntity<List<FarmerRepository.FarmerRecord>> getAllFarmers() {

        try {
            List<FarmerRepository.FarmerRecord> farmers =
                    farmerRepository.findAll();

            return ResponseEntity.ok(farmers);

        } catch (DataAccessException e) {
            throw new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR,
                    "Unable to retrieve farmer records from PostgreSQL",
                    e
            );
        }
    }

    public record RegistrationResponse(
            String message,
            String farmerId,
            String registrationStatus,
            String eventId,
            String correlationId,
            String topic,
            int partition,
            long offset
    ) {}
}
