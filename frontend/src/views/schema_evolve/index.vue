<template>
  <section class="page" data-module="schema_evolve">
    <header class="page-head">
      <div>
        <h2>Schema演进管理</h2>
        <p class="page-desc">维护消息Schema的前向/后向兼容演进与字段变更控制，为Schema演进提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记演进规则</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出Schema演进清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" :data-testid="`validate-${row.id}`" @click="validateRule(row)">
              校验订单字段
            </button>
            <button class="link" type="button" :data-testid="`preview-${row.id}`" @click="previewPackage(row)">
              预览兼容包
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无Schema演进数据，可先登记演进规则</td>
        </tr>
      </tbody>
    </table>

    <section v-if="validation" class="panel" data-testid="validation-panel">
      <header class="panel-head">
        <strong>订单字段演进规则校验 · {{ validation.规则编号 }}</strong>
        <span :class="validation.ok ? 'ok-text' : 'error-text'">
          {{ validation.ok ? '校验通过，可生成兼容发布包' : '校验未通过，需先修正' }}
        </span>
      </header>
      <table class="data-table compact">
        <thead><tr><th>校验项</th><th>结果</th><th>说明</th></tr></thead>
        <tbody>
          <tr v-for="(item, index) in validation.校验项" :key="index">
            <td>{{ item.校验项 }}</td>
            <td>{{ item.结果 }}</td>
            <td>{{ item.说明 }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section v-if="preview" class="panel" data-testid="preview-panel">
      <header class="panel-head">
        <strong>兼容发布包预览 · {{ preview.规则编号 }}</strong>
        <span class="muted-text">未落库，切换策略决定唯一可读版本</span>
      </header>
      <dl class="detail-grid">
        <div><dt>旧读取版本</dt><dd>{{ preview.旧读取版本 }}</dd></div>
        <div><dt>新映射版本</dt><dd>{{ preview.新映射版本 }}</dd></div>
        <div><dt>切换策略</dt><dd>{{ preview.切换策略 }}</dd></div>
        <div><dt>当前可读版本</dt><dd><strong>{{ preview.当前可读版本 }}</strong></dd></div>
        <div><dt>数据版本</dt><dd>{{ preview.数据版本 }}</dd></div>
        <div><dt>冲突取舍</dt><dd>{{ preview.冲突判定?.结论 }}</dd></div>
        <div class="wide"><dt>旧读取可见列</dt><dd>{{ (preview.旧读取可见列 || []).join(', ') }}</dd></div>
        <div class="wide"><dt>新映射可见列</dt><dd>{{ (preview.新映射可见列 || []).join(', ') }}</dd></div>
        <div class="wide"><dt>待登记任务</dt><dd>{{ (preview.任务项 || []).map((t: any) => t.任务编号).join('、') || '无' }}</dd></div>
      </dl>
      <div class="panel-actions">
        <button class="btn primary" type="button" data-testid="generate-package" @click="generatePackage">
          生成兼容发布包
        </button>
      </div>
    </section>

    <section class="panel" data-testid="release-panel">
      <header class="panel-head">
        <strong>Schema兼容发布包</strong>
        <span class="muted-text">同时保留旧版读取结果与新字段映射；中断后按来源批次恢复</span>
      </header>
      <table class="data-table compact">
        <thead>
          <tr>
            <th>发布包编号</th><th>来源批次</th><th>规则编号</th><th>状态</th>
            <th>旧读取 / 新映射</th><th>当前可读版本</th><th>数据版本</th>
            <th>进度</th><th>上一份完整兼容包</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in releases"
            :key="item.发布包编号"
            :data-testid="`release-${item.发布包编号}`"
            :data-status="item.状态"
          >
            <td>
              <button class="link" type="button" :data-testid="`open-release-${item.发布包编号}`" @click="openRelease(item.发布包编号)">
                {{ item.发布包编号 }}
              </button>
            </td>
            <td>{{ item.来源批次 }}</td>
            <td>{{ item.规则编号 }}</td>
            <td>{{ item.状态 }}</td>
            <td>{{ item.旧读取版本 }} / {{ item.新映射版本 }}</td>
            <td>{{ item.当前可读版本 }}</td>
            <td>{{ item.数据版本 }}</td>
            <td>{{ item.已完成项数 }}/{{ item.总项数 }}（剩余 {{ item.剩余项数 }}）</td>
            <td>{{ item.上一份完整兼容包 || '—' }}</td>
            <td class="row-actions">
              <button
                v-if="item.状态 === '发布中' || item.状态 === '已中断'"
                class="link" type="button" :data-testid="`apply-${item.发布包编号}`" @click="applyPackage(item)"
              >继续发布</button>
              <button
                v-if="item.状态 === '发布中'"
                class="link" type="button" :data-testid="`interrupt-${item.发布包编号}`" @click="interruptPackage(item)"
              >中断</button>
              <button
                v-if="item.状态 === '已中断'"
                class="link" type="button" :data-testid="`resume-${item.发布包编号}`" @click="resumePackage(item)"
              >恢复发布</button>
              <button
                v-if="item.状态 === '已发布'"
                class="link" type="button" :data-testid="`rollback-${item.发布包编号}`" @click="rollbackPackage(item)"
              >回滚旧读取</button>
              <button
                v-if="item.状态 === '已发布'"
                class="link" type="button" :data-testid="`export-release-${item.发布包编号}`" @click="exportRelease(item.发布包编号)"
              >导出兼容包</button>
            </td>
          </tr>
          <tr v-if="!releases.length">
            <td colspan="10" class="empty-state">暂无兼容发布包，可先校验订单字段演进规则后生成</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section v-if="releaseDetail" class="panel" data-testid="release-detail">
      <header class="panel-head">
        <strong>{{ releaseDetail.发布包编号 }} · {{ releaseDetail.状态 }}</strong>
        <button class="btn ghost" type="button" @click="releaseDetail = null">收起</button>
      </header>
      <dl class="detail-grid">
        <div><dt>来源批次</dt><dd>{{ releaseDetail.来源批次 }}</dd></div>
        <div><dt>关联主题</dt><dd>{{ releaseDetail.关联主题 }}</dd></div>
        <div><dt>切换策略</dt><dd>{{ releaseDetail.切换策略 }}</dd></div>
        <div><dt>当前可读版本</dt><dd><strong>{{ releaseDetail.当前可读版本 }}</strong></dd></div>
        <div><dt>数据版本</dt><dd>{{ releaseDetail.数据版本 }}</dd></div>
        <div><dt>上一份完整兼容包</dt><dd>{{ releaseDetail.上一份完整兼容包 || '—' }}</dd></div>
        <div class="wide"><dt>旧版读取结果</dt><dd><code>{{ pretty(releaseDetail.旧版读取结果) }}</code></dd></div>
        <div class="wide"><dt>新字段映射</dt><dd><code>{{ pretty(releaseDetail.新字段映射) }}</code></dd></div>
        <div class="wide"><dt>冲突判定</dt><dd>{{ releaseDetail.冲突判定?.结论 || '—' }}；{{ (releaseDetail.冲突判定?.原因 || []).join('；') || '无冲突原因' }}</dd></div>
      </dl>
      <table class="data-table compact">
        <thead><tr><th>任务编号</th><th>输出表名</th><th>映射状态</th><th>读取版本</th><th>任务可见列</th><th>来源批次</th></tr></thead>
        <tbody>
          <tr v-for="item in releaseDetail.任务项" :key="item.id">
            <td>{{ item.任务编号 }}</td>
            <td>{{ item.输出表名 }}</td>
            <td>{{ item.映射状态 }}</td>
            <td>{{ item.读取版本 }}</td>
            <td>{{ (item.任务可见列 || []).join(', ') }}</td>
            <td>{{ item.来源批次 }}</td>
          </tr>
          <tr v-if="!releaseDetail.任务项?.length">
            <td colspan="6" class="empty-state">该发布包暂无任务项</td>
          </tr>
        </tbody>
      </table>
      <table v-if="releaseDetail.使用登记?.length" class="data-table compact">
        <thead><tr><th>登记编号</th><th>任务编号</th><th>读取版本</th><th>数据版本</th><th>状态</th><th>登记时间</th></tr></thead>
        <tbody>
          <tr v-for="usage in releaseDetail.使用登记" :key="usage.id">
            <td>{{ usage.登记编号 }}</td>
            <td>{{ usage.任务编号 }}</td>
            <td>{{ usage.读取版本 }}</td>
            <td>{{ usage.数据版本 }}</td>
            <td>{{ usage.状态 }}</td>
            <td>{{ usage.登记时间 }}</td>
          </tr>
        </tbody>
      </table>
      <ol v-if="releaseDetail.发布记录?.length" class="release-log" data-testid="release-log">
        <li v-for="(log, index) in releaseDetail.发布记录" :key="index">
          <span class="muted-text">{{ log.时间 }}</span> · {{ log.事件 }}：{{ log.说明 }}
        </li>
      </ol>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记演进规则</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条Schema演进记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
      <span v-else-if="statusMessage" class="ok-text">{{ statusMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type AnyRow = Record<string, any>

const ENDPOINT = '/api/schema_evolve'
const columns = ["规则编号", "关联主题", "新增字段", "删除字段", "类型变更", "默认值填充", "兼容模式", "生效版本"]
const actions = ["提交校验", "标记生效", "执行回滚"]
const statuses = ["草稿", "待校验", "已生效", "已回滚"]
const metricNames = ["演进规则总数", "待校验规则", "已生效规则"]
const strategy = '新映射优先'

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const statusMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const validation = ref<AnyRow | null>(null)
const preview = ref<AnyRow | null>(null)
const previewRuleId = ref<number | null>(null)
const releases = ref<AnyRow[]>([])
const releaseDetail = ref<AnyRow | null>(null)
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[2]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

function pretty(value: unknown) {
  if (value === null || value === undefined) return '—'
  return typeof value === 'string' ? value : JSON.stringify(value)
}

function clearTransient() {
  errorMessage.value = ''
  statusMessage.value = ''
}

async function exportRows() {
  clearTransient()
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'schema_evolve.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

async function exportRelease(packageCode: string) {
  clearTransient()
  try {
    const response = await request(`${ENDPOINT}/releases/export?package=${encodeURIComponent(packageCode)}`)
    if (!response.ok) throw new Error('兼容包导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = `${packageCode}.csv`
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '兼容包导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  clearTransient()
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  clearTransient()
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  clearTransient()
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || 'Schema演进动作未生效')
    statusMessage.value = payload.message
    await Promise.all([reload(), reloadReleases()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'Schema演进操作失败'
  }
}

async function validateRule(row: Row) {
  clearTransient()
  preview.value = null
  try {
    const response = await request(`${ENDPOINT}/${row.id}/validate`, { method: 'POST', body: '{}' })
    const payload = await response.json()
    validation.value = payload.validation
    if (!payload.ok) throw new Error(payload.message || '校验未通过')
    statusMessage.value = payload.message
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '校验失败'
  }
}

async function previewPackage(row: Row) {
  clearTransient()
  validation.value = null
  try {
    const response = await request(`${ENDPOINT}/releases/preview`, {
      method: 'POST',
      body: JSON.stringify({ rule_id: Number(row.id), strategy }),
    })
    const payload = await response.json()
    if (!payload.ok) throw new Error(payload.message || '预览失败')
    preview.value = payload.preview
    previewRuleId.value = Number(row.id)
    statusMessage.value = payload.message
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '预览失败'
  }
}

async function generatePackage() {
  clearTransient()
  if (previewRuleId.value === null) return
  try {
    const response = await request(`${ENDPOINT}/releases`, {
      method: 'POST',
      body: JSON.stringify({ rule_id: previewRuleId.value, strategy }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '生成发布包失败')
    statusMessage.value = payload.message
    preview.value = null
    await Promise.all([reloadReleases(), reload()])
    if (payload.entry?.发布包编号) await openRelease(payload.entry.发布包编号)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '生成发布包失败'
  }
}

async function releaseAction(packageCode: string, action: 'apply' | 'interrupt' | 'resume' | 'rollback') {
  clearTransient()
  try {
    const response = await request(`${ENDPOINT}/releases/${encodeURIComponent(packageCode)}/${action}`, {
      method: 'POST', body: '{}',
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '操作失败')
    statusMessage.value = payload.message
    await Promise.all([reloadReleases(), reload()])
    await openRelease(packageCode)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '兼容包操作失败'
  }
}

function applyPackage(item: AnyRow) { return releaseAction(item.发布包编号, 'apply') }
function interruptPackage(item: AnyRow) { return releaseAction(item.发布包编号, 'interrupt') }
function resumePackage(item: AnyRow) { return releaseAction(item.发布包编号, 'resume') }
function rollbackPackage(item: AnyRow) { return releaseAction(item.发布包编号, 'rollback') }

async function openRelease(packageCode: string) {
  try {
    const response = await request(`${ENDPOINT}/releases/${encodeURIComponent(packageCode)}`)
    if (!response.ok) throw new Error('发布包详情读取失败')
    releaseDetail.value = await response.json()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '发布包详情读取失败'
  }
}

async function reloadReleases() {
  try {
    const response = await request(`${ENDPOINT}/releases`)
    if (!response.ok) throw new Error('兼容发布包列表读取失败')
    const payload = await response.json()
    releases.value = payload.items ?? []
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '兼容发布包列表读取失败'
  }
}

async function reload() {
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('演进规则列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'Schema演进列表读取失败'
  }
}

onMounted(async () => {
  await Promise.all([reload(), reloadReleases()])
})
</script>

<style scoped>
.panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px; margin: 12px 0; }
.panel-head { display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 8px; }
.panel-actions { display: flex; justify-content: flex-end; margin-top: 10px; }
.data-table.compact th, .data-table.compact td { padding: 6px 8px; font-size: 12px; }
.detail-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px 16px; margin: 0 0 10px; }
.detail-grid .wide { grid-column: 1 / -1; }
.detail-grid dt { font-size: 12px; color: var(--muted); }
.detail-grid dd { margin: 2px 0 0; font-size: 13px; }
.detail-grid code { font-size: 12px; white-space: pre-wrap; word-break: break-all; }
.muted-text { color: var(--muted); font-size: 12px; }
.ok-text { color: #15803d; }
.release-log { margin: 10px 0 0; padding-left: 18px; font-size: 12px; color: #334155; }
.release-log li { margin-bottom: 4px; }
</style>
