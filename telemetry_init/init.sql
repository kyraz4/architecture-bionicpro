CREATE TABLE IF NOT EXISTS telemetry_signals (
    prosthesis_id VARCHAR(255),
    created_at TIMESTAMP,
    recognized BOOLEAN,
    response_ms FLOAT,
    error BOOLEAN
);

INSERT INTO telemetry_signals (prosthesis_id, created_at, recognized, response_ms, error)
VALUES
('prosthesis_001', NOW(), true, 85.5, false),
('prosthesis_001', NOW(), true, 92.1, false),
('prosthesis_001', NOW(), false, 120.0, true);
