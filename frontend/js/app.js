const API_BASE_URL = "/api/java";

const farmerForm = document.getElementById("farmerForm");
const resultBox = document.getElementById("registrationResult");

if (farmerForm) {

    farmerForm.addEventListener("submit", async function (event) {

        event.preventDefault();

        const farmerData = {
            farmerName: document.getElementById("farmerName").value.trim(),
            district: document.getElementById("district").value.trim(),
            village: document.getElementById("village").value.trim()
        };

        resultBox.innerHTML = `
            <div class="alert alert-info">
                Registering farmer. Please wait...
            </div>
        `;

        try {

            const response = await fetch(
                `${API_BASE_URL}/api/farmers`,
                {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json"
                    },
                    body: JSON.stringify(farmerData)
                }
            );

            const data = await response.json();

            if (!response.ok) {
                throw new Error(data.detail || data.message || "Registration failed");
            }

            resultBox.innerHTML = `
                <div class="alert alert-success">
                    <h5>Farmer Registered Successfully!</h5>
                    <p><strong>Farmer ID:</strong> ${data.farmerId}</p>
                    <p><strong>Status:</strong> ${data.registrationStatus}</p>
                    <p><strong>Kafka Topic:</strong> ${data.topic}</p>
                    <p><strong>Partition:</strong> ${data.partition}</p>
                    <p><strong>Offset:</strong> ${data.offset}</p>
                    <p>${data.message}</p>
                </div>
            `;

            farmerForm.reset();

        } catch (error) {

            resultBox.textContent = "";

            const errorAlert = document.createElement("div");
            errorAlert.className = "alert alert-danger";
            errorAlert.textContent = error.message;

            resultBox.appendChild(errorAlert);

        }

    });

}
