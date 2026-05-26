-- Minimal runnable schema inferred from main.py SQL usage.
-- Covers the current Center APIs and scheduler paths:
--   - worker_registry
--   - file_tasks
--   - drive_accounts

CREATE TABLE IF NOT EXISTS worker_registry (
    node_name VARCHAR(128) NOT NULL,
    api_url VARCHAR(255) NOT NULL,
    last_active DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (node_name),
    KEY idx_worker_registry_last_active (last_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS drive_accounts (
    account_name VARCHAR(128) NOT NULL,
    role ENUM('uploader', 'cleaner', 'vip') NOT NULL DEFAULT 'uploader',
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    cleaner_mapping VARCHAR(128) NULL,
    cooldown_until DATETIME NULL,
    dispatch_locked_until DATETIME NULL,
    quota_blocked_until DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (account_name),
    KEY idx_drive_accounts_role_active (role, is_active),
    KEY idx_drive_accounts_cleaner_mapping (cleaner_mapping),
    KEY idx_drive_accounts_dispatch_locked_until (dispatch_locked_until),
    KEY idx_drive_accounts_quota_blocked_until (quota_blocked_until)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS file_tasks (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    file_path VARCHAR(1024) NOT NULL,
    file_path_hash CHAR(64) AS (SHA2(file_path, 256)) STORED,
    status ENUM('pending', 'uploading', 'staged', 'completed', 'failed', 'not_found') NOT NULL,
    worker_node VARCHAR(128) NOT NULL,
    upload_method ENUM('copy', 'move') NOT NULL DEFAULT 'move',
    account_used VARCHAR(128) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_file_tasks_file_path_hash (file_path_hash),
    KEY idx_file_tasks_status_worker_created (status, worker_node, created_at),
    KEY idx_file_tasks_account_status (account_used, status),
    KEY idx_file_tasks_updated_at (updated_at),
    KEY idx_file_tasks_worker_node (worker_node),
    CONSTRAINT fk_file_tasks_worker_node
        FOREIGN KEY (worker_node) REFERENCES worker_registry(node_name)
        ON UPDATE CASCADE,
    CONSTRAINT fk_file_tasks_account_used
        FOREIGN KEY (account_used) REFERENCES drive_accounts(account_name)
        ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
