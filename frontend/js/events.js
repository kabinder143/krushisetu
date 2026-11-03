"use strict";

const EVENTS_API = "/api/php/events";

let currentPage = 1;
let totalPages = 1;
let searchTimer = null;

const elements = {
    totalEvents: document.getElementById("totalEvents"),
    totalEventTypes: document.getElementById("totalEventTypes"),
    farmerEvents: document.getElementById("farmerEvents"),
    benefitEvents: document.getElementById("benefitEvents"),
    topicStatistics: document.getElementById("topicStatistics"),
    eventTableBody: document.getElementById("eventTableBody"),
    eventMessage: document.getElementById("eventMessage"),
    searchInput: document.getElementById("searchInput"),
    eventTypeFilter: document.getElementById("eventTypeFilter"),
    pageLimit: document.getElementById("pageLimit"),
    paginationInfo: document.getElementById("paginationInfo"),
    previousPage: document.getElementById("previousPage"),
    nextPage: document.getElementById("nextPage"),
    refreshButton: document.getElementById("refreshButton")
};

function showMessage(message, type = "danger") {
    elements.eventMessage.textContent = message;
    elements.eventMessage.className = `alert alert-${type}`;
}

function hideMessage() {
    elements.eventMessage.textContent = "";
    elements.eventMessage.className = "alert d-none";
}

function addCell(row, value) {
    const cell = document.createElement("td");
    cell.textContent = value ?? "-";
    row.appendChild(cell);
}

function renderStatistics(data) {
    const stats = data.statistics || {};

    elements.totalEvents.textContent = stats.total_events ?? 0;
    elements.totalEventTypes.textContent = stats.event_types ?? 0;

    const typeStats = data.eventTypeStatistics || [];

    const farmer = typeStats.find(
        item => item.event_type === "FarmerRegistered"
    );

    const benefit = typeStats.find(
        item => item.event_type === "BenefitStatusUpdated"
    );

    elements.farmerEvents.textContent = farmer?.total ?? 0;
    elements.benefitEvents.textContent = benefit?.total ?? 0;

    renderTopicStatistics(data.topicStatistics || []);
}

function renderTopicStatistics(topics) {
    elements.topicStatistics.replaceChildren();

    if (topics.length === 0) {
        const message = document.createElement("div");
        message.className = "col-12 text-muted";
        message.textContent = "No Kafka topic records found.";
        elements.topicStatistics.appendChild(message);
        return;
    }

    topics.forEach(topic => {
        const column = document.createElement("div");
        column.className = "col-md-6";

        const card = document.createElement("div");
        card.className = "p-3 rounded border h-100";

        const name = document.createElement("div");
        name.className = "fw-semibold text-success";
        name.textContent = topic.kafka_topic;

        const count = document.createElement("div");
        count.className = "fs-4 fw-bold";
        count.textContent = topic.total;

        const label = document.createElement("small");
        label.className = "text-muted";
        label.textContent = "Recorded events";

        card.append(name, count, label);
        column.appendChild(card);
        elements.topicStatistics.appendChild(column);
    });
}

function renderEvents(events) {
    elements.eventTableBody.replaceChildren();

    if (!events || events.length === 0) {
        const row = document.createElement("tr");
        const cell = document.createElement("td");

        cell.colSpan = 7;
        cell.className = "text-center text-muted py-4";
        cell.textContent = "No events found.";

        row.appendChild(cell);
        elements.eventTableBody.appendChild(row);
        return;
    }

    events.forEach(event => {
        const row = document.createElement("tr");

        addCell(row, event.event_id);
        addCell(row, event.event_type);
        addCell(row, event.farmer_id);
        addCell(row, event.kafka_topic);
        addCell(row, event.kafka_partition);
        addCell(row, event.kafka_offset);
        addCell(row, event.processed_at);

        elements.eventTableBody.appendChild(row);
    });
}

function renderPagination(pagination) {
    currentPage = Number(pagination.page) || 1;
    totalPages = Number(pagination.totalPages) || 0;

    const total = Number(pagination.filteredTotal) || 0;

    elements.paginationInfo.textContent =
        `Page ${currentPage} of ${totalPages} | ${total} event(s)`;

    elements.previousPage.disabled = currentPage <= 1;
    elements.nextPage.disabled =
        totalPages === 0 || currentPage >= totalPages;
}

async function loadEvents() {
    hideMessage();

    elements.refreshButton.disabled = true;
    elements.refreshButton.textContent = "Refreshing...";

    const params = new URLSearchParams({
        page: currentPage,
        limit: elements.pageLimit.value
    });

    const search = elements.searchInput.value.trim();
    const eventType = elements.eventTypeFilter.value;

    // Search across all event IDs and farmer IDs on the server.
    if (search !== "") {
        params.set("search", search);
    }

    if (eventType !== "") {
        params.set("eventType", eventType);
    }

    try {
        const response = await fetch(
            `${EVENTS_API}?${params.toString()}`,
            {
                method: "GET",
                headers: { "Accept": "application/json" },
                cache: "no-store"
            }
        );

        if (!response.ok) {
            throw new Error(`API returned HTTP ${response.status}`);
        }

        const data = await response.json();

        renderStatistics(data);
        renderEvents(data.events || []);
        renderPagination(data.pagination || {});

    } catch (error) {
        console.error("Event monitoring error:", error);

        showMessage(
            "Unable to load event monitoring data. Please check the API connection."
        );

        elements.eventTableBody.replaceChildren();

        const row = document.createElement("tr");
        const cell = document.createElement("td");

        cell.colSpan = 7;
        cell.className = "text-center text-danger py-4";
        cell.textContent = "Event data unavailable.";

        row.appendChild(cell);
        elements.eventTableBody.appendChild(row);

    } finally {
        elements.refreshButton.disabled = false;
        elements.refreshButton.textContent = "↻ Refresh Events";
    }
}

// Manual refresh
elements.refreshButton.addEventListener("click", () => {
    loadEvents();
});

// Event type filter
elements.eventTypeFilter.addEventListener("change", () => {
    currentPage = 1;
    loadEvents();
});

// Records per page
elements.pageLimit.addEventListener("change", () => {
    currentPage = 1;
    loadEvents();
});

// Server-side search with debounce
elements.searchInput.addEventListener("input", () => {
    clearTimeout(searchTimer);

    searchTimer = setTimeout(() => {
        currentPage = 1;
        loadEvents();
    }, 350);
});

// Previous page
elements.previousPage.addEventListener("click", () => {
    if (currentPage > 1) {
        currentPage--;
        loadEvents();
    }
});

// Next page
elements.nextPage.addEventListener("click", () => {
    if (currentPage < totalPages) {
        currentPage++;
        loadEvents();
    }
});

// Initial load and automatic refresh every 30 seconds.
loadEvents();
setInterval(loadEvents, 30000);
