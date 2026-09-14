-- 增加协议版本、调度租约及来源根目录身份。
-- 部署协议 v2 前执行；依赖用量统计迁移，字段和索引检查允许 MySQL 8.x 重复执行。

SET @sql = (
    SELECT IF(COUNT(*) = 0,
        'ALTER TABLE worker_registry ADD COLUMN protocol_version SMALLINT UNSIGNED NOT NULL DEFAULT 1 AFTER api_url',
        'SELECT ''worker_registry.protocol_version already exists'' AS migration_note')
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='worker_registry' AND COLUMN_NAME='protocol_version'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) = 0,
        'ALTER TABLE account_upload_events ADD COLUMN dispatch_id VARCHAR(32) NOT NULL DEFAULT ''legacy'' AFTER file_task_id',
        'SELECT ''account_upload_events.dispatch_id already exists'' AS migration_note')
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='account_upload_events' AND COLUMN_NAME='dispatch_id'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) = 0,
        'CREATE UNIQUE INDEX uk_account_upload_events_task_dispatch ON account_upload_events (file_task_id, dispatch_id)',
        'SELECT ''uk_account_upload_events_task_dispatch already exists'' AS migration_note')
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='account_upload_events' AND INDEX_NAME='uk_account_upload_events_task_dispatch'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) > 0,
        'ALTER TABLE account_upload_events DROP INDEX uk_account_upload_events_task',
        'SELECT ''uk_account_upload_events_task already removed'' AS migration_note')
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='account_upload_events' AND INDEX_NAME='uk_account_upload_events_task'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) = 0,
        'ALTER TABLE file_tasks ADD COLUMN source_mtime DOUBLE NULL AFTER size_bytes',
        'SELECT ''file_tasks.source_mtime already exists'' AS migration_note')
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='file_tasks' AND COLUMN_NAME='source_mtime'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) = 0,
        'ALTER TABLE drive_accounts ADD COLUMN dispatch_locked_until DATETIME NULL AFTER cooldown_until',
        'SELECT ''drive_accounts.dispatch_locked_until already exists'' AS migration_note')
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='drive_accounts' AND COLUMN_NAME='dispatch_locked_until'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) = 0,
        'ALTER TABLE drive_accounts ADD COLUMN dispatch_cooldown_until DATETIME NULL AFTER dispatch_locked_until',
        'SELECT ''drive_accounts.dispatch_cooldown_until already exists'' AS migration_note')
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='drive_accounts' AND COLUMN_NAME='dispatch_cooldown_until'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) = 0,
        'ALTER TABLE drive_accounts ADD COLUMN quota_blocked_until DATETIME NULL AFTER dispatch_cooldown_until',
        'SELECT ''drive_accounts.quota_blocked_until already exists'' AS migration_note')
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='drive_accounts' AND COLUMN_NAME='quota_blocked_until'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) = 0,
        'ALTER TABLE drive_accounts ADD COLUMN current_dispatch_id CHAR(32) NULL AFTER quota_blocked_until',
        'SELECT ''drive_accounts.current_dispatch_id already exists'' AS migration_note')
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='drive_accounts' AND COLUMN_NAME='current_dispatch_id'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) = 0,
        'ALTER TABLE drive_accounts ADD COLUMN current_worker_node VARCHAR(128) NULL AFTER current_dispatch_id',
        'SELECT ''drive_accounts.current_worker_node already exists'' AS migration_note')
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='drive_accounts' AND COLUMN_NAME='current_worker_node'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) = 0,
        'ALTER TABLE file_tasks ADD COLUMN source_base_dir VARCHAR(1024) NULL AFTER file_path',
        'SELECT ''file_tasks.source_base_dir already exists'' AS migration_note')
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='file_tasks' AND COLUMN_NAME='source_base_dir'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) = 0,
        'ALTER TABLE file_tasks ADD COLUMN dispatch_id CHAR(32) NULL AFTER account_used',
        'SELECT ''file_tasks.dispatch_id already exists'' AS migration_note')
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='file_tasks' AND COLUMN_NAME='dispatch_id'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- file_path 仍是全局目标文件身份；source_base_dir 仅记录来源位置，
-- 不把不同来源的同一目标路径变成重复的上传任务。

SET @sql = (
    SELECT IF(COUNT(*) = 0,
        'CREATE INDEX idx_drive_accounts_dispatch_cooldown_until ON drive_accounts (dispatch_cooldown_until)',
        'SELECT ''idx_drive_accounts_dispatch_cooldown_until already exists'' AS migration_note')
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='drive_accounts' AND INDEX_NAME='idx_drive_accounts_dispatch_cooldown_until'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) = 0,
        'CREATE INDEX idx_file_tasks_dispatch_id ON file_tasks (dispatch_id)',
        'SELECT ''idx_file_tasks_dispatch_id already exists'' AS migration_note')
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='file_tasks' AND INDEX_NAME='idx_file_tasks_dispatch_id'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
