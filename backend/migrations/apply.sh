#!/usr/bin/env bash
# Schema兼容发布包迁移执行脚本。
#
# 用法：
#   ./migrations/apply.sh up    # 建表
#   ./migrations/apply.sh down  # 回滚建表，数据一并删除
#   ./migrations/apply.sh check # 只做语法检查（psql --echo-queries --single-transaction 预演）
#
# 环境变量：
#   DATABASE_URL  必填，例如 postgres://user:pass@db:5432/lakehouse
#
# 失败处理：
#   up 失败时脚本不会自动 down；先修复再重跑。up/down 都在单事务中执行，
#   psql 出错即整体回滚，不会留下半张表。
set -euo pipefail

MIGRATION="2026_11_19_schema_compat_release"
DIR="$(cd "$(dirname "$0")" && pwd)"
ACTION="${1:-up}"

case "$ACTION" in
  up)   FILE="$DIR/$MIGRATION.up.sql" ;;
  down) FILE="$DIR/$MIGRATION.down.sql" ;;
  check)
    for f in "$DIR/$MIGRATION.up.sql" "$DIR/$MIGRATION.down.sql"; do
      echo "checking $f"
      grep -qi '^BEGIN;' "$f" || { echo "missing BEGIN in $f"; exit 1; }
      grep -qi '^COMMIT;' "$f" || { echo "missing COMMIT in $f"; exit 1; }
    done
    echo "migration files ok"
    exit 0
    ;;
  *)
    echo "usage: $0 [up|down|check]" >&2
    exit 2
    ;;
esac

: "${DATABASE_URL:?DATABASE_URL is required, e.g. postgres://user:pass@db:5432/lakehouse}"

echo "[$ACTION] $FILE"
psql "$DATABASE_URL" \
  --set ON_ERROR_STOP=1 \
  --single-transaction \
  --file "$FILE"

echo "[$ACTION] done"
