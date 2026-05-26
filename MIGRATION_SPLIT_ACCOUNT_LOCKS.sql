-- 拆分 drive_accounts 的调度执行锁、短期冷却锁与 403 配额封禁锁。
-- 建议在停调度或低峰时执行，并先确认没有需要保留的旧 cooldown_until 语义。

ALTER TABLE drive_accounts
    ADD COLUMN dispatch_locked_until DATETIME NULL AFTER cooldown_until,
    ADD COLUMN dispatch_cooldown_until DATETIME NULL AFTER dispatch_locked_until,
    ADD COLUMN quota_blocked_until DATETIME NULL AFTER dispatch_cooldown_until;

CREATE INDEX idx_drive_accounts_dispatch_locked_until
    ON drive_accounts (dispatch_locked_until);

CREATE INDEX idx_drive_accounts_dispatch_cooldown_until
    ON drive_accounts (dispatch_cooldown_until);

CREATE INDEX idx_drive_accounts_quota_blocked_until
    ON drive_accounts (quota_blocked_until);

-- 迁移说明：
-- 1. 旧 cooldown_until 在历史数据中混用了“执行锁”和“403 封禁”，无法自动精准拆分。
-- 2. 新代码读取 dispatch_locked_until / dispatch_cooldown_until / quota_blocked_until，不再依赖 cooldown_until。
-- 3. 上线前如需保留当前正在执行的短期锁，可人工将相关账号写入 dispatch_locked_until。
-- 4. 若历史上存在旧版短期调度冷却数据，可人工将相关账号写入 dispatch_cooldown_until。
-- 5. 上线前如需保留已确认的 403 封禁账号，可人工将相关账号写入 quota_blocked_until。
