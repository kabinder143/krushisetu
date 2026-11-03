const API_URL = "/api/java/api/farmers";

const tableBody = document.getElementById("farmerTableBody");
const searchInput = document.getElementById("farmerSearch");
const message = document.getElementById("recordsMessage");
const totalFarmers = document.getElementById("totalFarmers");
const submittedFarmers = document.getElementById("submittedFarmers");
const recordCount = document.getElementById("recordCount");
const refreshButton = document.getElementById("refreshButton");

let farmers = [];

function displayFarmers(data) {
    tableBody.innerHTML = "";

    if (data.length === 0) {
        tableBody.innerHTML = `
            <tr>
                <td colspan="5" class="text-center text-muted">
                    No farmer records found.
                </td>
            </tr>`;
        recordCount.textContent = "Showing 0 records";
        return;
    }

    data.forEach(farmer => {
        const row = document.createElement("tr");

        const values = [
            farmer.farmerId,
            farmer.farmerName,
            farmer.district,
            farmer.village,
            farmer.registrationStatus
        ];

        values.forEach((value, index) => {
            const cell = document.createElement("td");
            cell.textContent = value ?? "-";

            if (index === 4) {
                const badge = document.createElement("span");
                badge.className = "badge bg-success";
                badge.textContent = value ?? "UNKNOWN";
                cell.textContent = "";
                cell.appendChild(badge);
            }

            row.appendChild(cell);
        });

        tableBody.appendChild(row);
    });

    recordCount.textContent = `Showing ${data.length} of ${farmers.length} records`;
}

function filterFarmers() {
    const searchTerm = searchInput.value.trim().toLowerCase();

    const filtered = farmers.filter(farmer => {
        return [
            farmer.farmerId,
            farmer.farmerName,
            farmer.district,
            farmer.village,
            farmer.registrationStatus
        ].some(value => String(value ?? "").toLowerCase().includes(searchTerm));
    });

    displayFarmers(filtered);
}

async function loadFarmers() {
    refreshButton.disabled = true;
    refreshButton.textContent = "Loading...";
    message.innerHTML = "";
    tableBody.innerHTML = `
        <tr><td colspan="5" class="text-center">Loading records...</td></tr>`;

    try {
        const response = await fetch(API_URL);

        if (!response.ok) {
            throw new Error(`Server returned HTTP ${response.status}`);
        }

        const data = await response.json();

        if (!Array.isArray(data)) {
            throw new Error("Unexpected API response format");
        }

        farmers = data;

        totalFarmers.textContent = farmers.length;
        submittedFarmers.textContent =
            farmers.filter(f => f.registrationStatus === "SUBMITTED").length;

        filterFarmers();

        message.innerHTML = `
            <div class="alert alert-success py-2">
                Farmer records loaded successfully.
            </div>`;

    } catch (error) {
        tableBody.innerHTML = `
            <tr>
                <td colspan="5" class="text-center text-danger">
                    Unable to load farmer records.
                </td>
            </tr>`;

        message.innerHTML = `
            <div class="alert alert-danger">
                ${error.message}. Please check the Java API and try again.
            </div>`;

        totalFarmers.textContent = "--";
        submittedFarmers.textContent = "--";
        recordCount.textContent = "";

    } finally {
        refreshButton.disabled = false;
        refreshButton.textContent = "↻ Refresh Records";
    }
}

searchInput.addEventListener("input", filterFarmers);
refreshButton.addEventListener("click", loadFarmers);

loadFarmers();
