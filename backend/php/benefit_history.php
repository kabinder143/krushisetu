<?php

declare(strict_types=1);

header('Content-Type: application/json');

if ($_SERVER['REQUEST_METHOD'] !== 'GET') {
    http_response_code(405);
    echo json_encode([
        'error' => 'Method not allowed. Use GET.'
    ]);
    exit;
}

try {
    // Load existing database configuration
    $config = parse_ini_file('/etc/krushisetu-php/database.ini');

    if ($config === false) {
        throw new RuntimeException('Database configuration unavailable.');
    }

    // PostgreSQL connection
    $dsn = sprintf(
        'pgsql:host=%s;port=%s;dbname=%s',
        $config['host'],
        $config['port'],
        $config['name']
    );

    $db = new PDO(
        $dsn,
        $config['user'],
        $config['password'],
        [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_EMULATE_PREPARES => false
        ]
    );

    // Optional farmer ID filter
    $farmerId = trim($_GET['farmerId'] ?? '');

    $sql = "
        SELECT
            b.farmer_id,
            f.farmer_name,
            f.district,
            f.village,
            b.benefit_status,
            b.event_id,
            b.updated_at
        FROM benefit_status b
        INNER JOIN farmers f
            ON b.farmer_id = f.farmer_id
    ";

    if ($farmerId !== '') {
        if (!preg_match('/^FARM-[A-Z0-9-]{1,30}$/', $farmerId)) {
            http_response_code(400);
            echo json_encode([
                'error' => 'Invalid farmerId format'
            ]);
            exit;
        }

        $sql .= " WHERE b.farmer_id = :farmer_id";
    }

    $sql .= " ORDER BY b.updated_at DESC LIMIT 200";

    $stmt = $db->prepare($sql);

    if ($farmerId !== '') {
        $stmt->execute([
            ':farmer_id' => $farmerId
        ]);
    } else {
        $stmt->execute();
    }

    $records = $stmt->fetchAll(PDO::FETCH_ASSOC);

    http_response_code(200);

    echo json_encode([
        'totalRecords' => count($records),
        'benefitHistory' => $records
    ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);

} catch (Throwable $e) {

    error_log('Benefit History API error: ' . $e->getMessage());

    http_response_code(500);

    echo json_encode([
        'error' => 'Unable to retrieve benefit history'
    ]);
}
