-- 拆分 drive_accounts 的调度执行锁、短期冷却锁与 403 配额封禁锁。
-- 建议在停调度或低峰时执行，并先确认没有需要保留的旧 cooldown_until 语义。
-- 先检查字段和索引是否存在，可在 MySQL 8.x 新建库或旧库重复执行。
-- 本脚本只补充缺失结构，不修改已有账号的锁、冷却时间和配额状态。

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
        'CREATE INDEX idx_drive_accounts_dispatch_locked_until ON drive_accounts (dispatch_locked_until)',
        'SELECT ''idx_drive_accounts_dispatch_locked_until already exists'' AS migration_note')
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='drive_accounts' AND INDEX_NAME='idx_drive_accounts_dispatch_locked_until'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

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
        'CREATE INDEX idx_drive_accounts_quota_blocked_until ON drive_accounts (quota_blocked_until)',
        'SELECT ''idx_drive_accounts_quota_blocked_until already exists'' AS migration_note')
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='drive_accounts' AND INDEX_NAME='idx_drive_accounts_quota_blocked_until'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 迁移说明：
-- 1. 旧 cooldown_until 在历史数据中混用了“执行锁”和“403 封禁”，无法自动精准拆分。
-- 2. 新代码读取 dispatch_locked_until / dispatch_cooldown_until / quota_blocked_until，不再依赖 cooldown_until。
-- 3. 上线前如需保留当前正在执行的短期锁，可人工将相关账号写入 dispatch_locked_until。
-- 4. 若历史上存在旧版短期调度冷却数据，可人工将相关账号写入 dispatch_cooldown_until。
-- 5. 上线前如需保留已确认的 403 封禁账号，可人工将相关账号写入 quota_blocked_until。
