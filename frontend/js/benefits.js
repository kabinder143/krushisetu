
const FARMERS_API = "/api/java/api/farmers";
const BENEFIT_POST_API = "/api/php/benefit";
const BENEFIT_HISTORY_API = "/api/php/benefits";

const farmerSelect = document.getElementById("farmerSelect");
const benefitForm = document.getElementById("benefitForm");
const benefitStatus = document.getElementById("benefitStatus");
const submitButton = document.getElementById("submitBenefit");

const benefitMessage = document.getElementById("benefitMessage");
const historyMessage = document.getElementById("historyMessage");

const tableBody = document.getElementById("benefitTableBody");
const searchInput = document.getElementById("benefitSearch");
const refreshButton = document.getElementById("refreshBenefits");

const totalBenefits = document.getElementById("totalBenefits");
const pendingBenefits = document.getElementById("pendingBenefits");
const approvedBenefits = document.getElementById("approvedBenefits");
const disbursedBenefits = document.getElementById("disbursedBenefits");

const recordCount = document.getElementById("benefitRecordCount");

let farmers = [];
let benefits = [];

function showMessage(element, message, type) {
    element.textContent = "";

    const alert = document.createElement("div");
    alert.className = `alert alert-${type} py-2`;
    alert.textContent = message;

    element.appendChild(alert);
}

async function loadFarmers() {
    farmerSelect.disabled = true;
    farmerSelect.replaceChildren(
        new Option("Loading farmers...", "")
    );

    try {
        const response = await fetch(FARMERS_API);

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }

        const data = await response.json();

        if (!Array.isArray(data)) {
            throw new Error("Invalid farmer API response");
        }

        farmers = data;
        farmerSelect.replaceChildren(
            new Option("Select a farmer", "")
        );

        farmers.forEach(farmer => {
            const option = new Option(
                `${farmer.farmerName} | ${farmer.farmerId} | ${farmer.district}`,
                farmer.farmerId
            );

            farmerSelect.appendChild(option);
        });

        farmerSelect.disabled = false;

    } catch (error) {
        farmerSelect.replaceChildren(
            new Option("Unable to load farmers", "")
        );

        showMessage(
            benefitMessage,
            "Unable to load farmers. Check the Java API.",
            "danger"
        );
    }
}

function updateStatistics() {
    totalBenefits.textContent = benefits.length;

    pendingBenefits.textContent =
        benefits.filter(b => b.benefit_status === "PENDING").length;

    approvedBenefits.textContent =
        benefits.filter(b => b.benefit_status === "APPROVED").length;

    disbursedBenefits.textContent =
        benefits.filter(b => b.benefit_status === "DISBURSED").length;
}

function displayBenefits(data) {
    tableBody.replaceChildren();

    if (data.length === 0) {
        const row = tableBody.insertRow();
        const cell = row.insertCell();

        cell.colSpan = 6;
        cell.className = "text-center text-muted";
        cell.textContent = "No benefit records found.";

        recordCount.textContent = "Showing 0 records";
        return;
    }

    data.forEach(benefit => {
        const row = tableBody.insertRow();

        const values = [
            benefit.farmer_id,
            benefit.farmer_name,
            benefit.district,
            benefit.benefit_status,
            benefit.event_id,
            benefit.updated_at
        ];

        values.forEach((value, index) => {
            const cell = row.insertCell();
            cell.textContent = value ?? "-";

            if (index === 3) {
                const badge = document.createElement("span");

                const statusClasses = {
                    PENDING: "bg-warning text-dark",
                    APPROVED: "bg-success",
                    REJECTED: "bg-danger",
                    DISBURSED: "bg-primary"
                };

                badge.className =
                    `badge ${statusClasses[value] || "bg-secondary"}`;

                badge.textContent = value ?? "UNKNOWN";

                cell.replaceChildren(badge);
            }
        });
    });

    recordCount.textContent =
        `Showing ${data.length} of ${benefits.length} records`;
}

function filterBenefits() {
    const term = searchInput.value.trim().toLowerCase();

    const filtered = benefits.filter(benefit => {
        return [
            benefit.farmer_id,
            benefit.farmer_name,
            benefit.district,
            benefit.village,
            benefit.benefit_status,
            benefit.event_id
        ].some(value =>
            String(value ?? "").toLowerCase().includes(term)
        );
    });

    displayBenefits(filtered);
}

async function loadBenefits() {
    refreshButton.disabled = true;
    refreshButton.textContent = "Loading...";

    try {
        const response = await fetch(BENEFIT_HISTORY_API);

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }

        const data = await response.json();

        if (!Array.isArray(data.benefitHistory)) {
            throw new Error("Invalid benefit history response");
        }

        benefits = data.benefitHistory;

        updateStatistics();
        filterBenefits();

        showMessage(
            historyMessage,
            "Benefit history loaded successfully.",
            "success"
        );

    } catch (error) {
        benefits = [];

        tableBody.replaceChildren();

        const row = tableBody.insertRow();
        const cell = row.insertCell();

        cell.colSpan = 6;
        cell.className = "text-center text-danger";
        cell.textContent = "Unable to load benefit history.";

        totalBenefits.textContent = "--";
        pendingBenefits.textContent = "--";
        approvedBenefits.textContent = "--";
        disbursedBenefits.textContent = "--";

        recordCount.textContent = "";

        showMessage(
            historyMessage,
            `${error.message}. Check the PHP API and PostgreSQL.`,
            "danger"
        );

    } finally {
        refreshButton.disabled = false;
        refreshButton.textContent = "↻ Refresh History";
    }
}

benefitForm.addEventListener("submit", async event => {
    event.preventDefault();

    const farmerId = farmerSelect.value;
    const status = benefitStatus.value;

    if (!farmerId || !status) {
        showMessage(
            benefitMessage,
            "Please select a farmer and benefit status.",
            "warning"
        );
        return;
    }

    submitButton.disabled = true;
    submitButton.textContent = "Submitting...";

    benefitMessage.replaceChildren();

    try {
        const response = await fetch(BENEFIT_POST_API, {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                farmerId: farmerId,
                benefitStatus: status
            })
        });

        const result = await response.json();

        if (!response.ok) {
            throw new Error(
                result.error || `HTTP ${response.status}`
            );
        }

        showMessage(
            benefitMessage,
            `Benefit event published successfully. Event ID: ${result.event.eventId}`,
            "success"
        );

        benefitForm.reset();

        // Allow the Java consumer time to process the Kafka event.
        await new Promise(resolve => setTimeout(resolve, 1200));

        await loadBenefits();

    } catch (error) {
        showMessage(
            benefitMessage,
            `Submission failed: ${error.message}`,
            "danger"
        );

    } finally {
        submitButton.disabled = false;
        submitButton.textContent = "Submit Benefit Status";
    }
});

searchInput.addEventListener("input", filterBenefits);
refreshButton.addEventListener("click", loadBenefits);

async function initializePage() {
    await Promise.all([
        loadFarmers(),
        loadBenefits()
    ]);
}

initializePage();
