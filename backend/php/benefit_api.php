<?php

declare(strict_types=1);

header('Content-Type: application/json');

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode([
        'error' => 'Method not allowed. Use POST.'
    ]);
    exit;
}

$input = json_decode(file_get_contents('php://input'), true);

if (!is_array($input)) {
    http_response_code(400);
    echo json_encode(['error' => 'Invalid JSON request body']);
    exit;
}

$farmerId = trim((string) ($input['farmerId'] ?? ''));
$benefitStatus = trim((string) ($input['benefitStatus'] ?? ''));

$allowedStatuses = ['PENDING', 'APPROVED', 'REJECTED', 'DISBURSED'];

if ($farmerId === '' || $benefitStatus === '') {
    http_response_code(400);
    echo json_encode([
        'error' => 'farmerId and benefitStatus are required'
    ]);
    exit;
}

if (!preg_match('/^FARM-[A-Z0-9-]{1,30}$/', $farmerId)) {
    http_response_code(400);
    echo json_encode(['error' => 'Invalid farmerId format']);
    exit;
}

if (!in_array($benefitStatus, $allowedStatuses, true)) {
    http_response_code(400);
    echo json_encode([
        'error' => 'Invalid benefitStatus',
        'allowedStatuses' => $allowedStatuses
    ]);
    exit;
}

try {
    $conf = new RdKafka\Conf();
    $kafkaBrokers = getenv('KAFKA_BOOTSTRAP_SERVERS') ?: 'localhost:9092';
    $conf->set('bootstrap.servers', $kafkaBrokers);

    $producer = new RdKafka\Producer($conf);
    $topic = $producer->newTopic('benefit.status.updated');

    $event = [
        'eventId' => bin2hex(random_bytes(16)),
        'eventType' => 'BenefitStatusUpdated',
        'source' => 'benefit-status-php',
        'occurredAt' => gmdate('Y-m-d\TH:i:s\Z'),
        'data' => [
            'farmerId' => $farmerId,
            'benefitStatus' => $benefitStatus
        ]
    ];

    $json = json_encode($event, JSON_THROW_ON_ERROR);

    $topic->produce(
        RD_KAFKA_PARTITION_UA,
        0,
        $json,
        $farmerId
    );

    $remaining = $producer->flush(10000);

    if ($remaining !== RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw new RuntimeException(
            'Kafka delivery timed out; pending messages: ' . $remaining
        );
    }

    http_response_code(201);
    echo json_encode([
        'message' => 'Benefit status event published successfully',
        'event' => $event
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);

} catch (Throwable $e) {
    http_response_code(503);
    echo json_encode([
        'error' => 'Unable to publish benefit status event',
        'details' => $e->getMessage()
    ]);
}
