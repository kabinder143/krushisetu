package in.yobytech.krushisetu;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record FarmerRegistrationRequest(

        @NotBlank(message = "Farmer name is required")
        @Size(max = 100, message = "Farmer name must not exceed 100 characters")
        String farmerName,

        @NotBlank(message = "District is required")
        @Size(max = 100, message = "District must not exceed 100 characters")
        String district,

        @NotBlank(message = "Village is required")
        @Size(max = 100, message = "Village must not exceed 100 characters")
        String village

) {
}
