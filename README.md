# 实时数据入湖平台

面向流式数据接入、实时转换、增量合并与湖上物化查询的实时数据入湖治理平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 主题注册 | `topic_registry` | 数据主题 | 主题编号、主题名称、消息格式 |
| 流式计算 | `flink_job` | 流式任务 | 任务编号、消费主题、转换逻辑 |
| 消息接入 | `kafka_connector` | 接入配置 | 配置编号、集群地址、消费组名 |
| Schema演进 | `schema_evolve` | 演进规则 | 规则编号、关联主题、新增字段 |
| 增量合并 | `upsert_merge` | 合并策略 | 策略编号、目标表名、合并键列 |
| 湖上分区 | `lake_partition` | 分区定义 | 分区编号、目标表名、分区列名 |
| 流式质量 | `stream_quality` | 质量规则 | 规则编号、监控主题、校验类型 |
| Iceberg管理 | `iceberg_table` | 表元数据 | 表编号、表名称、存储格式 |
| 水位追踪 | `watermark_track` | 水位标记 | 标记编号、数据主题、分区编号 |
| 死信处理 | `dead_letter` | 死信记录 | 记录编号、原始主题、原始分区 |
| 物化查询 | `materialized_view` | 物化视图 | 视图编号、查询SQL、基表名 |
| CDC捕获 | `change_capture` | CDC配置 | 配置编号、源数据库、源表名 |
| 合并清理 | `compact_policy` | 清理策略 | 策略编号、目标表名、触发条件 |
| 流式血缘 | `stream_lineage` | 血缘记录 | 记录编号、上游主题、下游表名 |
| 流式补数 | `backfill_stream` | 补数任务 | 任务编号、目标表名、起始偏移 |
| 下沉分发 | `sink_dispatch` | 分发规则 | 规则编号、湖表名、目标系统 |
| 数据生命周期 | `ttl_policy` | 生命周期 | 策略编号、目标表名、热数据保留 |
| 实时指标 | `realtime_metrics` | 指标定义 | 指标编号、指标名称、聚合窗口 |

## Schema兼容发布包

订单字段演进规则除了推进规则状态，还会生成一份 **Schema兼容发布包**，同时保留：

- **旧版读取结果**：旧任务按旧版本继续读取的字段与默认值；
- **新字段映射**：新任务按新版本读取新增字段的映射。

发布包只能声明一个 **当前可读版本**。切换策略决定它是哪一个：

| 切换策略 | 当前可读版本 / 数据版本 | 任务可见列 | 冲突判定取舍 |
| --- | --- | --- | --- |
| 新映射优先 | 新映射版本 | 旧读取列 + 新增字段 | 先冻结旧读取，以新映射为准 |
| 旧读取优先 | 旧读取版本 | 只有旧读取列 | 新映射暂缓生效，保留旧读取 |

冲突是否存在不随策略变化，但取舍会改变发布包的数据版本，预览接口会同时给出两种结果。

### 发布、中断与恢复

- 写入点只有一个：`backend/app/services/schema_evolve.py` 的 `_apply_release`。
- 每个任务项有 `待映射 / 已映射 / 已回滚` 三种状态；中断只停止推进，已完成项保留 `应用时间`。
- 带 `source_batch` 重试会恢复 **同一份** 发布包（`source_batch` 唯一），不会新建、不会重做已映射项。
- 不带批次重试时，自动续做该规则下最近一份未完成发布包。
- 同一字段内容 + 同一策略已有完整发布包时，重复生成会直接返回原发布包并提示"未重复读取旧字段"。
- 中断期间上一份完整兼容包保持 `已发布` 状态不变，旧任务可继续读取，也可回滚。

### 跨模块交接

`Schema演进` 页导出兼容包，`流式计算` 页核对任务使用版本：

- `GET /api/schema_evolve/releases` / `GET /api/schema_evolve/releases/{发布包编号}`：列表与详情同一摘要口径；
- `GET /api/schema_evolve/releases/export?package=...`：导出兼容包（含旧读取结果、新字段映射、任务项状态）；
- `GET /api/flink_job/schema-usage`：逐个任务给出使用版本与可用发布包；
- `POST /api/flink_job/{任务id}/schema-check`：只读核对，不落登记；
- `POST /api/flink_job/{任务id}/schema-usage`：核对通过后登记使用版本，**保留来源批次**。

订单聚合任务（输出表 `order_agg_hourly` 等）启动前必须先登记使用版本，否则 `启动作业` 会被拦下。

## 迁移与上线

新增的三张表在 `backend/migrations/`：

```text
2026_11_19_schema_compat_release.up.sql    正向建表
2026_11_19_schema_compat_release.down.sql  回滚删表
apply.sh                                   up / down / check
```

上线流程（依赖、环境、数据库、回滚）：

1. **依赖**：无需新增三方依赖。后端仍是标准库 + FastAPI；前端无新 npm 包。
2. **环境**：仅新增可选变量 `DATABASE_URL`（`migrations/apply.sh` 使用）。现有 `.env.example` 可保持不变；
   `APP_ENV`、`VITE_PROXY_TARGET` 的用法不变。
3. **数据库**：
   - 先跑 `make migrate-check`（只做文件级检查）；
   - 再跑 `DATABASE_URL=... backend/migrations/apply.sh up`（单事务，失败整体回滚）；
   - 校验：三张表存在，`schema_compat_releases.source_batch` 有唯一约束。
4. **应用上线顺序**：先建表 → 再发后端 → 最后发前端。后端在表未建好前不写发布包，
   现有模块接口不受影响（新接口只在被调用时读写这三张表）。
5. **回滚**：
   - 应用回滚：回滚到上一版后端/前端镜像即可，新接口不被调用时没有副作用；
   - 数据回滚：先调用 `POST /api/schema_evolve/releases/{编号}/rollback` 把任务退回旧读取版本，
     确认旧任务读取正常后，再执行 `backend/migrations/apply.sh down`。
   - `down` 会删除三张产物表；执行前请先导出兼容包：
     `GET /api/schema_evolve/releases/export`。
6. **灰度建议**：先在单主题（TOPI-0001）生成发布包并中断一次，确认上一份完整包仍在、
   剩余项可恢复，再对所有主题放开。

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 测试

```bash
make test          # 后端单元测试 + 前端类型检查
make test-backend  # 仅后端（pytest）
make typecheck     # 仅前端（vue-tsc）
```

`backend/app/tests/test_schema_compat.py` 覆盖：订单字段演进规则校验、发布包同时保留旧读取与
新版映射、两种切换策略下的可见列与数据版本、发布中断保留上一份完整包、按来源批次恢复并补齐
剩余项、已切换任务不重复读取旧字段、回滚、列表/详情口径一致、流式计算核对与使用版本登记。

领域：数据工程
子方向：数仓与数据湖
