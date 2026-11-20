<template>
  <section class="page" data-module="flink_job">
    <header class="page-head">
      <div><h2>流式计算</h2><p class="page-desc">维护Flink流式任务的SQL逻辑、状态与检查点配置，为流式计算提供可验证的数据工程流程。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记流式任务</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>
    <div class="filter-bar">
      <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <button class="btn" type="button" @click="reload">查询</button>
    </div>
    <div class="timeline-wrap">
      <div v-for="item in timelineItems" :key="item.id" class="timeline-row">
        <span class="timeline-label">{{ item.name || item.label }}</span>
        <div class="timeline-track">
          <div class="timeline-bar" :class="`status-${item.status}`" :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }" :data-testid="`bar-${item.id}`" :title="`${item.start} ~ ${item.end}`" @click="selectItem(item)">&nbsp;</div>
        </div>
        <span style="font-size:12px;color:var(--muted);margin-left:8px;">{{ item.start }} ~ {{ item.end }}</span>
      </div>
      <p v-if="!timelineItems.length" style="text-align:center;color:var(--muted)">暂无作业记录</p>
    </div>
    <div v-if="selectedItem" class="timeline-log" data-testid="timeline-log">
      <strong>{{ selectedItem.name || selectedItem.label }}</strong> — 状态：{{ selectedItem.status }}
      <div style="margin-top:4px;">{{ selectedItem.logs || '暂无日志' }}</div>
    </div>

    <section class="panel" data-testid="schema-usage-panel">
      <header class="panel-head">
        <strong>Schema兼容发布包核对</strong>
        <span class="muted-text">核对订单聚合任务使用的发布包版本；登记时保留来源批次，跨模块交接不丢批次</span>
      </header>
      <table class="data-table compact">
        <thead>
          <tr>
            <th>任务编号</th><th>消费主题</th><th>输出表名</th><th>任务状态</th>
            <th>使用版本</th><th>数据版本</th><th>发布包 / 来源批次</th>
            <th>任务可见列</th><th>核对发布包</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in usageRows"
            :key="item.id"
            :data-testid="`usage-${item.id}`"
            :data-has-usage="item.usage ? 'true' : 'false'"
          >
            <td>{{ item.任务编号 }}{{ item.订单任务 ? '（订单聚合）' : '' }}</td>
            <td>{{ item.消费主题 }}</td>
            <td>{{ item.输出表名 }}</td>
            <td>{{ item.status }}</td>
            <td :data-testid="`usage-version-${item.id}`">{{ item.usage?.读取版本 || '未登记' }}</td>
            <td>{{ item.usage?.数据版本 || '—' }}</td>
            <td>{{ item.usage ? `${item.usage.发布包编号} / ${item.usage.来源批次}` : '—' }}</td>
            <td>{{ (item.usage?.任务可见列 || []).join(', ') || '—' }}</td>
            <td>
              <select
                v-if="item.可选发布包?.length"
                v-model="packageChoice[item.id]"
                :data-testid="`package-${item.id}`"
              >
                <option value="">按主题自动选择（{{ item.可用发布包?.发布包编号 || '无已发布包' }}）</option>
                <option v-for="pkg in item.可选发布包" :key="pkg.发布包编号" :value="pkg.发布包编号">
                  {{ pkg.发布包编号 }}（{{ pkg.状态 }} · {{ pkg.当前可读版本 }} · {{ pkg.来源批次 }}）
                </option>
              </select>
              <span v-else class="muted-text">暂无发布包</span>
            </td>
            <td class="row-actions">
              <button class="link" type="button" :data-testid="`check-${item.id}`" @click="checkUsage(item)">
                核对发布包
              </button>
              <button
                v-if="item.可选发布包?.length"
                class="link" type="button" :data-testid="`register-${item.id}`" @click="registerUsage(item)"
              >
                登记使用版本
              </button>
            </td>
          </tr>
        </tbody>
      </table>
      <div v-if="checkResult" class="check-result" data-testid="usage-check">
        <strong>核对结果 · {{ checkResult.任务编号 }}</strong>
        <span :class="checkResult.ok ? 'ok-text' : 'error-text'">{{ checkResult.ok ? '通过' : '未通过' }}</span>
        <ul>
          <li v-for="(check, index) in checkResult.核对项" :key="index">
            {{ check.核对项 }}：期望 {{ check.期望 }}，实际 {{ check.实际 }}
          </li>
        </ul>
      </div>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记流式任务</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
    <footer class="page-foot">
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
      <span v-else-if="statusMessage" class="ok-text">{{ statusMessage }}</span>
    </footer>
  </section>
</template>
<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/flink_job'
const columns = ["任务编号", "消费主题", "转换逻辑", "输出表名", "并行度", "检查点间隔", "状态后端", "水位策略"]
const actions = ["提交校验", "启动作业", "停止作业"]
const timelineItems = ref<any[]>([])
const selectedItem = ref<any>(null)
const dateFrom = ref(''); const dateTo = ref('')
const errorMessage = ref(''); const statusMessage = ref('')
const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const allItems = ref<any[]>([])
const usageRows = ref<any[]>([])
const checkResult = ref<any>(null)
const packageChoice = ref<Record<number, string>>({})
function chosenPackage(item: any) { return packageChoice.value[item.id] || undefined }
function bodyFor(item: any) {
  const chosen = chosenPackage(item)
  return JSON.stringify(chosen ? { package: chosen } : {})
}
function barLeft(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 0; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return ((new Date(item.start).getTime() - min) / range) * 100 }
function barWidth(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 20; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100) }
function selectItem(item: any) { selectedItem.value = selectedItem.value?.id === item.id ? null : item }
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); allItems.value = data.items || []; applyFilter() } catch(e) { errorMessage.value = '加载失败' }
}
function applyFilter() {
  let items = allItems.value
  if(dateFrom.value) items = items.filter((i: any) => i.start >= dateFrom.value)
  if(dateTo.value) items = items.filter((i: any) => i.end <= dateTo.value)
  timelineItems.value = items; selectedItem.value = null
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
async function reloadUsage() {
  try {
    const res = await request(`${ENDPOINT}/schema-usage`)
    if (!res.ok) throw new Error('使用版本读取失败')
    const data = await res.json()
    usageRows.value = data.items || []
  } catch (e) {
    errorMessage.value = e instanceof Error ? e.message : '使用版本读取失败'
  }
}
async function checkUsage(item: any) {
  errorMessage.value = ''; statusMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/${item.id}/schema-check`, { method: 'POST', body: bodyFor(item) })
    const data = await res.json()
    checkResult.value = data.check
    if (data.ok) statusMessage.value = data.message
    else errorMessage.value = data.message
  } catch (e) {
    errorMessage.value = e instanceof Error ? e.message : '核对失败'
  }
}
async function registerUsage(item: any) {
  errorMessage.value = ''; statusMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/${item.id}/schema-usage`, { method: 'POST', body: bodyFor(item) })
    const data = await res.json()
    if (!res.ok || !data.ok) throw new Error(data.message || '登记失败')
    statusMessage.value = data.message
    await reloadUsage()
    await checkUsage(item)
  } catch (e) {
    errorMessage.value = e instanceof Error ? e.message : '登记失败'
  }
}
onMounted(async () => { await Promise.all([reload(), reloadUsage()]) })
</script>
<style scoped>
.panel { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px 14px; margin: 12px 0; }
.panel-head { display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 8px; }
.data-table.compact th, .data-table.compact td { padding: 6px 8px; font-size: 12px; }
.muted-text { color: var(--muted); font-size: 12px; }
.ok-text { color: #15803d; }
.check-result { margin-top: 10px; border-top: 1px dashed var(--border); padding-top: 8px; font-size: 12px; }
.check-result ul { margin: 6px 0 0; padding-left: 18px; }
</style>
