package in.yobytech.krushisetu;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class FarmerRepository {

    private final JdbcTemplate jdbcTemplate;

    public FarmerRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // Save a new farmer
    public void save(
            String farmerId,
            FarmerRegistrationRequest request) {

        String sql = """
                INSERT INTO farmers (
                    farmer_id,
                    farmer_name,
                    district,
                    village,
                    registration_status
                )
                VALUES (?, ?, ?, ?, ?)
                """;

        jdbcTemplate.update(
                sql,
                farmerId,
                request.farmerName(),
                request.district(),
                request.village(),
                "SUBMITTED"
        );
    }

    // Retrieve all registered farmers
    public List<FarmerRecord> findAll() {

        String sql = """
                SELECT
                    farmer_id,
                    farmer_name,
                    district,
                    village,
                    registration_status
                FROM farmers
                ORDER BY registered_at DESC
                """;

        return jdbcTemplate.query(
                sql,
                (rs, rowNum) -> new FarmerRecord(
                        rs.getString("farmer_id"),
                        rs.getString("farmer_name"),
                        rs.getString("district"),
                        rs.getString("village"),
                        rs.getString("registration_status")
                )
        );
    }

    // Farmer response model
    public record FarmerRecord(
            String farmerId,
            String farmerName,
            String district,
            String village,
            String registrationStatus
    ) {}
}
