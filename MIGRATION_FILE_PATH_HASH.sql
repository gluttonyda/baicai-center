-- 补齐 file_tasks.file_path_hash 生成列及其唯一索引。
-- Center 上报接口依赖该唯一索引做 ON DUPLICATE KEY 去重，并按 file_path_hash 回查任务指纹。
-- 兼容的字段和完整唯一索引存在时跳过，可在 MySQL 8.x 重复执行。执行前停止 Center 与 Scanner 并备份。
--
-- 若旧库已存在完全相同的 file_path 多行，本脚本不会删除或合并任何数据，而是以
-- “Table '...MIGRATION_ABORTED_duplicate_file_path_rows_see_docs' doesn't exist” 错误终止。
-- 已有 file_path_hash 列不是 CHAR(64) AS (SHA2(file_path, 256)) STORED，或该列上存在前缀/表达式
-- 唯一索引时，同样在任何 DDL 之前以 MIGRATION_ABORTED_* 错误终止，不覆盖列、不删除索引。
-- 按 docs/center_node.md 的“file_path_hash 迁移”一节人工核对并处理后再重新执行。
-- 新增 STORED 生成列会重建 file_tasks，大表请预留维护窗口。

-- information_schema 保存规范化后的表达式（如 sha2(`file_path`,256)），比较时忽略空白、反引号和大小写。
SET @file_path_hash_column_state = IFNULL((
    SELECT IF(
        DATA_TYPE='char' AND CHARACTER_MAXIMUM_LENGTH=64 AND EXTRA LIKE '%STORED GENERATED%'
            AND LOWER(REPLACE(REGEXP_REPLACE(GENERATION_EXPRESSION, '[[:space:]]+', ''), '`', ''))
                = 'sha2(file_path,256)',
        'compatible', 'incompatible')
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='file_tasks' AND COLUMN_NAME='file_path_hash'
), 'missing');

SET @sql = (
    SELECT IF(@file_path_hash_column_state = 'incompatible',
        'SELECT 1 FROM `MIGRATION_ABORTED_incompatible_file_path_hash_column_see_docs`',
        'SELECT ''file_tasks.file_path_hash is missing or compatible'' AS migration_note')
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 前缀或表达式唯一索引会把完整哈希不同的路径判为重复；只新增完整索引也无法消除，必须人工删除。
SET @prefix_unique_file_path_hash_indexes = (
    SELECT COUNT(DISTINCT INDEX_NAME)
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='file_tasks' AND NON_UNIQUE=0
      AND ((COLUMN_NAME='file_path_hash' AND SUB_PART IS NOT NULL)
           OR LOCATE('file_path_hash', COALESCE(EXPRESSION, '')) > 0)
);

SET @sql = (
    SELECT IF(@prefix_unique_file_path_hash_indexes > 0,
        'SELECT 1 FROM `MIGRATION_ABORTED_prefix_unique_file_path_hash_index_see_docs`',
        'SELECT ''file_tasks has no prefix unique index on file_path_hash'' AS migration_note')
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @has_unique_file_path_hash = (
    SELECT COUNT(*) FROM (
        SELECT INDEX_NAME
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='file_tasks' AND NON_UNIQUE=0
        GROUP BY INDEX_NAME
        HAVING COUNT(*)=1 AND MAX(COLUMN_NAME)='file_path_hash' AND MAX(SUB_PART) IS NULL
    ) unique_hash_indexes
);

SET @duplicate_file_path_groups = (
    SELECT CASE
        WHEN @has_unique_file_path_hash > 0 THEN 0
        ELSE (
            SELECT COUNT(*) FROM (
                SELECT 1
                FROM file_tasks
                GROUP BY SHA2(file_path, 256)
                HAVING COUNT(*) > 1
            ) duplicate_groups
        )
    END
);

SET @sql = (
    SELECT IF(@duplicate_file_path_groups > 0,
        'SELECT 1 FROM `MIGRATION_ABORTED_duplicate_file_path_rows_see_docs`',
        'SELECT ''file_tasks has no duplicate file_path rows'' AS migration_note')
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(COUNT(*) = 0,
        'ALTER TABLE file_tasks ADD COLUMN file_path_hash CHAR(64) AS (SHA2(file_path, 256)) STORED AFTER file_path',
        'SELECT ''file_tasks.file_path_hash already exists'' AS migration_note')
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='file_tasks' AND COLUMN_NAME='file_path_hash'
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = (
    SELECT IF(@has_unique_file_path_hash = 0,
        'CREATE UNIQUE INDEX uk_file_tasks_file_path_hash ON file_tasks (file_path_hash)',
        'SELECT ''unique index on file_tasks.file_path_hash already exists'' AS migration_note')
);
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
