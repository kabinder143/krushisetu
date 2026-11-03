<?php

declare(strict_types=1);

header('Content-Type: application/json');

if ($_SERVER['REQUEST_METHOD'] !== 'GET') {
    http_response_code(405);
    echo json_encode(['error' => 'Method not allowed. Use GET.']);
    exit;
}

try {
    $config = parse_ini_file('/etc/krushisetu-php/database.ini');

    if ($config === false) {
        throw new RuntimeException('Database configuration unavailable');
    }

    $dsn = sprintf(
        'pgsql:host=%s;port=%s;dbname=%s',
        $config['host'],
        $config['port'],
        $config['name']
    );

    $pdo = new PDO(
        $dsn,
        $config['user'],
        $config['password'],
        [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]
    );

    // Request filters
    $search = trim($_GET['search'] ?? '');
    $farmerId = trim($_GET['farmerId'] ?? '');
    $eventType = trim($_GET['eventType'] ?? '');

    $page = max(1, (int) ($_GET['page'] ?? 1));
    $limit = min(100, max(1, (int) ($_GET['limit'] ?? 20)));
    $offset = ($page - 1) * $limit;

    // Build safe SQL filters
    $where = [];
    $params = [];

    if ($search !== '') {
        $where[] = '(event_id ILIKE :search_event OR farmer_id ILIKE :search_farmer)';
        $params['search_event'] = '%' . $search . '%';
        $params['search_farmer'] = '%' . $search . '%';
    }

    if ($farmerId !== '') {
        $where[] = 'farmer_id = :farmer_id';
        $params['farmer_id'] = $farmerId;
    }

    if ($eventType !== '') {
        $where[] = 'event_type = :event_type';
        $params['event_type'] = $eventType;
    }

    $whereSql = $where
        ? 'WHERE ' . implode(' AND ', $where)
        : '';

    // Overall statistics (not affected by filters)
    $stats = $pdo->query("
        SELECT
            COUNT(*) AS total_events,
            COUNT(DISTINCT event_type) AS event_types,
            COUNT(DISTINCT kafka_topic) AS kafka_topics
        FROM farmer_event_receipts
    ")->fetch(PDO::FETCH_ASSOC);

    $typeStats = $pdo->query("
        SELECT event_type, COUNT(*) AS total
        FROM farmer_event_receipts
        GROUP BY event_type
        ORDER BY event_type
    ")->fetchAll(PDO::FETCH_ASSOC);

    $topicStats = $pdo->query("
        SELECT kafka_topic, COUNT(*) AS total
        FROM farmer_event_receipts
        GROUP BY kafka_topic
        ORDER BY kafka_topic
    ")->fetchAll(PDO::FETCH_ASSOC);

    // Filtered count
    $countStmt = $pdo->prepare("
        SELECT COUNT(*)
        FROM farmer_event_receipts
        $whereSql
    ");

    $countStmt->execute($params);
    $filteredTotal = (int) $countStmt->fetchColumn();

    // Paginated event history
    $historyStmt = $pdo->prepare("
        SELECT
            event_id,
            event_type,
            farmer_id,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            processed_at
        FROM farmer_event_receipts
        $whereSql
        ORDER BY processed_at DESC, kafka_topic, kafka_partition, kafka_offset DESC
        LIMIT :limit OFFSET :offset
    ");

    foreach ($params as $key => $value) {
        $historyStmt->bindValue(':' . $key, $value);
    }

    $historyStmt->bindValue(':limit', $limit, PDO::PARAM_INT);
    $historyStmt->bindValue(':offset', $offset, PDO::PARAM_INT);
    $historyStmt->execute();

    $events = $historyStmt->fetchAll(PDO::FETCH_ASSOC);

    echo json_encode([
        'statistics' => $stats,
        'eventTypeStatistics' => $typeStats,
        'topicStatistics' => $topicStats,
        'pagination' => [
            'page' => $page,
            'limit' => $limit,
            'filteredTotal' => $filteredTotal,
            'totalPages' => (int) ceil($filteredTotal / $limit)
        ],
        'events' => $events
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);

} catch (Throwable $e) {
    http_response_code(500);

    error_log('[KrushiSetu Events API] ' . $e->getMessage());

    echo json_encode([
        'error' => 'Unable to retrieve event monitoring data'
    ]);
}
