-- Adds passive per-account upload usage tracking.
-- Safe to run more than once on MySQL 8.x.

SET @add_size_bytes_sql = (
    SELECT IF(
        COUNT(*) = 0,
        'ALTER TABLE file_tasks ADD COLUMN size_bytes BIGINT UNSIGNED NULL AFTER file_path',
        'SELECT ''file_tasks.size_bytes already exists'' AS migration_note'
    )
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'file_tasks'
      AND COLUMN_NAME = 'size_bytes'
);

PREPARE add_size_bytes_stmt FROM @add_size_bytes_sql;
EXECUTE add_size_bytes_stmt;
DEALLOCATE PREPARE add_size_bytes_stmt;

CREATE TABLE IF NOT EXISTS account_upload_events (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    account_name VARCHAR(128) NOT NULL,
    file_task_id BIGINT UNSIGNED NOT NULL,
    size_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
    uploaded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_account_upload_events_task (file_task_id),
    KEY idx_account_upload_events_account_time (account_name, uploaded_at),
    KEY idx_account_upload_events_uploaded_at (uploaded_at),
    CONSTRAINT fk_account_upload_events_account
        FOREIGN KEY (account_name) REFERENCES drive_accounts(account_name)
        ON UPDATE CASCADE,
    CONSTRAINT fk_account_upload_events_file_task
        FOREIGN KEY (file_task_id) REFERENCES file_tasks(id)
        ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
