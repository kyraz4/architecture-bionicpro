CREATE TABLE IF NOT EXISTS reports_mart.prosthesis_usage_daily
(
    user_id          String,           -- ID клиента из CRM (ключ доступа)
    prosthesis_id    String,           -- ID протеза
    report_date      Date,             -- Дата агрегации
    total_signals    UInt64,           -- Общее число миосигналов за день
    recognized_moves UInt64,           -- Распознанных движений
    avg_response_ms  Float32,          -- Среднее время отклика
    error_count      UInt32            -- Ошибки распознавания
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(report_date)
ORDER BY (user_id, report_date, prosthesis_id);