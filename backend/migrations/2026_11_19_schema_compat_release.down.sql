-- Schema兼容发布包：回滚迁移
--
-- 回滚只删除本次新增的三张产物表，不改动 schema_evolve / flink_job 等既有业务表。
-- 执行顺序与 up 相反，先删依赖表再删主表。

BEGIN;

DROP INDEX IF EXISTS uq_flink_schema_usage_active;
DROP INDEX IF EXISTS idx_schema_compat_release_items_status;
DROP INDEX IF EXISTS idx_schema_compat_releases_topic;
DROP INDEX IF EXISTS idx_schema_compat_releases_rule;

DROP TABLE IF EXISTS flink_schema_usage;
DROP TABLE IF EXISTS schema_compat_release_items;
DROP TABLE IF EXISTS schema_compat_releases;

COMMIT;
