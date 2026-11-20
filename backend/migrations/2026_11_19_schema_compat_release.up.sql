-- Schema兼容发布包：向前迁移
--
-- 生产环境把内存 store 换成数据库时，按本文件建表；回滚脚本见
-- 2026_11_19_schema_compat_release.down.sql。
--
-- 设计要点：
-- 1. 发布包与任务项分表：发布包保留旧版读取结果与新字段映射，任务项记录
--    每个任务"是否已映射"，中断后只补剩余项，不整包重做。
-- 2. source_batch 唯一：同一来源批次的重复请求落回同一份发布包。
-- 3. usage 表按 (job_id, package_code) 记录任务使用版本，保留 source_batch，
--    使流式计算跨模块交接时能追溯到来源批次。
-- 4. releases 同时保留 old_read_version 与 new_mapping_version；readable_version
--    只有一个，随切换策略决定其取值。

BEGIN;

CREATE TABLE IF NOT EXISTS schema_compat_releases (
    id                      BIGSERIAL PRIMARY KEY,
    package_code            VARCHAR(64)  NOT NULL,
    rule_id                 BIGINT       NOT NULL,
    rule_code               VARCHAR(64)  NOT NULL,
    topic_code              VARCHAR(64)  NOT NULL,
    source_batch            VARCHAR(128) NOT NULL,
    strategy                VARCHAR(32)  NOT NULL,
    old_read_version        VARCHAR(32)  NOT NULL,
    new_mapping_version     VARCHAR(32)  NOT NULL,
    readable_version        VARCHAR(32)  NOT NULL,
    data_version            VARCHAR(32)  NOT NULL,
    expected_readable_version VARCHAR(32) NOT NULL,
    status                  VARCHAR(32)  NOT NULL,
    visible_columns         JSONB        NOT NULL DEFAULT '[]'::jsonb,
    old_read_payload        JSONB        NOT NULL DEFAULT '{}'::jsonb,
    new_mapping_payload     JSONB        NOT NULL DEFAULT '{}'::jsonb,
    conflict_judgement      JSONB        NOT NULL DEFAULT '{}'::jsonb,
    release_log             JSONB        NOT NULL DEFAULT '[]'::jsonb,
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at             TIMESTAMPTZ,
    CONSTRAINT uq_schema_compat_release_code  UNIQUE (package_code),
    CONSTRAINT uq_schema_compat_release_batch UNIQUE (source_batch),
    CONSTRAINT ck_schema_compat_release_status CHECK (
        status IN ('发布中', '已中断', '已发布', '已回滚')
    ),
    CONSTRAINT ck_schema_compat_release_strategy CHECK (
        strategy IN ('新映射优先', '旧读取优先')
    )
);

COMMENT ON TABLE  schema_compat_releases IS 'Schema兼容发布包：旧版读取结果 + 新字段映射，只声明一个当前可读版本';
COMMENT ON COLUMN schema_compat_releases.source_batch IS '来源批次：中断重试按此恢复同一份发布包';
COMMENT ON COLUMN schema_compat_releases.readable_version IS '当前可读版本（唯一），由切换策略决定';
COMMENT ON COLUMN schema_compat_releases.old_read_payload IS '旧版读取结果：旧任务继续读取所需字段与默认值';

CREATE TABLE IF NOT EXISTS schema_compat_release_items (
    id              BIGSERIAL PRIMARY KEY,
    package_code    VARCHAR(64) NOT NULL REFERENCES schema_compat_releases (package_code) ON DELETE CASCADE,
    rule_id         BIGINT      NOT NULL,
    topic_code      VARCHAR(64) NOT NULL,
    job_id          BIGINT      NOT NULL,
    job_code        VARCHAR(64) NOT NULL,
    output_table    VARCHAR(128),
    source_batch    VARCHAR(128) NOT NULL,
    mapping_status  VARCHAR(16) NOT NULL,
    read_version    VARCHAR(32) NOT NULL,
    visible_columns JSONB       NOT NULL DEFAULT '[]'::jsonb,
    field_mapping   JSONB       NOT NULL DEFAULT '[]'::jsonb,
    applied_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_schema_compat_item_job UNIQUE (package_code, job_id),
    CONSTRAINT ck_schema_compat_item_status CHECK (
        mapping_status IN ('待映射', '已映射', '已回滚')
    )
);

COMMENT ON TABLE schema_compat_release_items IS '发布包任务项：待映射项在中断后补齐，已映射项不重复应用';

CREATE TABLE IF NOT EXISTS flink_schema_usage (
    id                      BIGSERIAL PRIMARY KEY,
    usage_code              VARCHAR(64) NOT NULL,
    job_id                  BIGINT      NOT NULL,
    job_code                VARCHAR(64) NOT NULL,
    topic_code              VARCHAR(64) NOT NULL,
    output_table            VARCHAR(128),
    package_code            VARCHAR(64) NOT NULL REFERENCES schema_compat_releases (package_code),
    source_batch            VARCHAR(128) NOT NULL,
    read_version            VARCHAR(32) NOT NULL,
    data_version            VARCHAR(32) NOT NULL,
    strategy                VARCHAR(32) NOT NULL,
    visible_columns         JSONB       NOT NULL DEFAULT '[]'::jsonb,
    previous_complete_package VARCHAR(64),
    old_read_payload        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    checks                  JSONB       NOT NULL DEFAULT '[]'::jsonb,
    status                  VARCHAR(16) NOT NULL,
    registered_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_flink_schema_usage_code UNIQUE (usage_code),
    CONSTRAINT ck_flink_schema_usage_status CHECK (
        status IN ('已登记', '已替换', '已回滚')
    )
);

COMMENT ON TABLE flink_schema_usage IS '流式计算登记的任务使用版本，保留 source_batch 供跨模块追溯';
COMMENT ON COLUMN flink_schema_usage.old_read_payload IS '回滚依据：登记时的旧版读取结果';

CREATE INDEX IF NOT EXISTS idx_schema_compat_releases_rule
    ON schema_compat_releases (rule_id, id);
CREATE INDEX IF NOT EXISTS idx_schema_compat_releases_topic
    ON schema_compat_releases (topic_code, status);
CREATE INDEX IF NOT EXISTS idx_schema_compat_release_items_status
    ON schema_compat_release_items (package_code, mapping_status);
CREATE UNIQUE INDEX IF NOT EXISTS uq_flink_schema_usage_active
    ON flink_schema_usage (job_id)
    WHERE status IN ('已登记', '已回滚');

COMMIT;
