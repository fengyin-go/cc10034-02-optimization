<template>
  <section class="page" data-testid="filter-batch-view">
    <header class="page-head">
      <div>
        <h2>过滤批次与结果集</h2>
        <p class="page-desc">
          批次接收时冻结规则版本；同级白/黑名单冲突按黑名单优先留痕；路由分发只消费已放行事件并回写出站清单。
        </p>
      </div>
      <div class="page-actions">
        <RouterLink class="btn" to="/event_filter">返回过滤规则</RouterLink>
        <button class="btn" data-testid="refresh" type="button" @click="refreshAll">刷新</button>
        <button class="btn primary" data-testid="dispatch" type="button" :disabled="!selectedBatch" @click="runDispatch">
          分发选中批次
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">批次数</span>
        <strong class="stat-value" data-testid="stat-batches">{{ batches.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">有效事件</span>
        <strong class="stat-value" data-testid="stat-effective">{{ selectedBatch?.summary.effective_count ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">放行</span>
        <strong class="stat-value" data-testid="stat-allowed">{{ selectedBatch?.summary.allowed_count ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">拦截</span>
        <strong class="stat-value" data-testid="stat-blocked">{{ selectedBatch?.summary.blocked_count ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">出站清单</span>
        <strong class="stat-value" data-testid="stat-outbound">{{ outbound.length }}</strong>
      </article>
    </div>

    <p v-if="errorMessage" class="error-text" data-testid="filter-error">{{ errorMessage }}</p>
    <p v-if="noticeMessage" class="notice-text" data-testid="filter-notice">{{ noticeMessage }}</p>

    <table class="data-table">
      <thead>
        <tr>
          <th>批次号</th>
          <th>来源</th>
          <th>接收时间</th>
          <th>规则版本</th>
          <th>提交 / 有效</th>
          <th>放行 / 拦截</th>
          <th>重复</th>
          <th>冲突</th>
          <th>重投次数</th>
          <th>分发</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="batch in batches"
          :key="batch.batch_id"
          :data-testid="`batch-${batch.batch_id}`"
          :class="{ selected: batch.batch_id === selectedBatchId }"
          @click="selectBatch(batch.batch_id)"
        >
          <td>{{ batch.batch_id }}</td>
          <td>{{ batch.source }}</td>
          <td>{{ batch.received_at }}</td>
          <td>{{ batch.rule_version }}（{{ batch.rule_count }} 条）</td>
          <td>{{ batch.summary.total_submitted }} / {{ batch.summary.effective_count }}</td>
          <td>{{ batch.summary.allowed_count }} / {{ batch.summary.blocked_count }}</td>
          <td>{{ batch.summary.duplicate_count }}</td>
          <td>{{ batch.summary.conflict_count }}</td>
          <td>{{ batch.replay_count }}</td>
          <td>{{ batch.dispatch ? `${batch.dispatch.outbound_total} 条 → ${batch.dispatch.channel}` : '未分发' }}</td>
        </tr>
        <tr v-if="!batches.length">
          <td colspan="10" class="empty-state">暂无过滤批次，可在「变更事件」页提交过滤批次</td>
        </tr>
      </tbody>
    </table>

    <template v-if="selectedBatch">
      <h3 class="section-title">过滤结果清单 · {{ selectedBatch.batch_id }}</h3>
      <p class="page-desc">
        规则版本 {{ selectedBatch.rule_version.version_id }}；冻结后规则改动不会改写本批结论，可用「规则前后对照」查看差异。
        <button class="link" data-testid="load-rule-diff" type="button" @click="loadDiff">规则前后对照</button>
      </p>
      <table class="data-table">
        <thead>
          <tr>
            <th>记录号</th>
            <th>事件ID</th>
            <th>来源表</th>
            <th>操作</th>
            <th>结论</th>
            <th>决定性规则</th>
            <th>优先级</th>
            <th>命中记录</th>
            <th>作用域外规则</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="record in selectedBatch.records"
            :key="record.record_no"
            :data-testid="`record-${record.record_no}`"
            :data-verdict="record.verdict"
            :class="{ conflict: record.conflict.present }"
          >
            <td>{{ record.record_no }}</td>
            <td>{{ record.event_id }}</td>
            <td>{{ record.source_table }}</td>
            <td>{{ record.operation }}</td>
            <td>
              <span :class="record.allowed ? 'tag-allow' : 'tag-deny'">{{ record.verdict }}</span>
              <span v-if="record.default_allow" class="tag-muted">默认</span>
              <span v-if="record.conflict.present" class="tag-conflict" data-testid="conflict-tag">同级冲突</span>
            </td>
            <td>{{ record.decisive_rule }}</td>
            <td>{{ record.effective_priority ?? '—' }}</td>
            <td class="cell-detail">
              <div v-for="hit in record.hits" :key="`${record.record_no}-${hit.rule_id}`" class="hit-line">
                <span class="hit-name">{{ hit.rule_name }}</span>
                <span class="tag-muted">P{{ hit.priority }}</span>
                <span class="hit-cond">{{ hit.effective_condition }}</span>
                <span :class="hit.verdict === '放行' ? 'tag-allow' : 'tag-deny'">{{ hit.verdict }}</span>
              </div>
              <div v-if="record.conflict.present" class="conflict-line" data-testid="conflict-detail">
                {{ record.conflict.resolution }}
                <template v-for="level in record.conflict.levels" :key="level.priority">
                  <br />
                  P{{ level.priority }}：放行 [{{ level.allow_rules.join('、') }}] / 拦截 [{{ level.deny_rules.join('、') }}]
                  <span v-if="level.overridden_by_priority !== null">（已被更高优先级覆盖，仅留档）</span>
                </template>
              </div>
            </td>
            <td class="cell-detail">
              <div v-for="rule in record.scope_out" :key="`${record.record_no}-out-${rule.rule_id}`" class="out-line">
                <span>{{ rule.rule_name }}</span>
                <span class="tag-muted">{{ rule.reason }}</span>
              </div>
            </td>
          </tr>
        </tbody>
      </table>

      <h3 class="section-title">去重留档</h3>
      <table class="data-table">
        <thead>
          <tr><th>事件ID</th><th>来源表</th><th>操作</th><th>并入记录号</th><th>说明</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in selectedBatch.deduped" :key="item.duplicate_of">
            <td>{{ item.event_id }}</td>
            <td>{{ item.source_table }}</td>
            <td>{{ item.operation }}</td>
            <td>{{ item.duplicate_of }}</td>
            <td>{{ item.reason }}</td>
          </tr>
          <tr v-if="!selectedBatch.deduped.length">
            <td colspan="5" class="empty-state">本批没有重复提交的同源事件</td>
          </tr>
        </tbody>
      </table>

      <h3 class="section-title">一致性核对</h3>
      <div v-if="reconcile" class="reconcile" :data-consistent="String(reconcile.consistent)" :data-status="reconcile.status" data-testid="reconcile">
        <span v-if="reconcile.status === '未分发'">⏳ 批次尚未分发，出站清单为空；分发后再核对</span>
        <span v-else-if="reconcile.consistent">✅ 过滤结果清单与出站清单一致</span>
        <span v-else>❌ 清单不一致</span>
        <span>放行 {{ reconcile.allowed_count }} / 出站 {{ reconcile.outbound_count }}</span>
        <span v-if="reconcile.missing_from_outbound.length">
          缺失：{{ reconcile.missing_from_outbound.join('、') }}
        </span>
        <span v-if="reconcile.blocked_but_dispatched.length">
          误投：{{ reconcile.blocked_but_dispatched.join('、') }}
        </span>
        <span v-if="reconcile.duplicated_outbound.length">
          重复：{{ reconcile.duplicated_outbound.join('、') }}
        </span>
      </div>

      <h3 class="section-title">出站清单</h3>
      <table class="data-table">
        <thead>
          <tr><th>出站号</th><th>记录号</th><th>事件ID</th><th>来源表</th><th>操作</th><th>目标通道</th><th>规则版本</th><th>分发时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="entry in outbound" :key="entry.outbound_id" :data-testid="`outbound-${entry.event_id}`">
            <td>{{ entry.outbound_id }}</td>
            <td>{{ entry.record_no }}</td>
            <td>{{ entry.event_id }}</td>
            <td>{{ entry.source_table }}</td>
            <td>{{ entry.operation }}</td>
            <td>{{ entry.target_channel }}</td>
            <td>{{ entry.rule_version }}</td>
            <td>{{ entry.dispatched_at }}</td>
          </tr>
          <tr v-if="!outbound.length">
            <td colspan="8" class="empty-state">尚未分发；点击「分发选中批次」只投递已放行事件</td>
          </tr>
        </tbody>
      </table>

      <template v-if="ruleDiff">
        <h3 class="section-title">规则前后对照</h3>
        <p class="page-desc">
          冻结版本 {{ ruleDiff.frozen_rule_version }} → 当前版本 {{ ruleDiff.current_rule_version }}；
          变更 {{ ruleDiff.changed_count }} 条。
        </p>
        <table class="data-table">
          <thead>
            <tr><th>记录号</th><th>事件ID</th><th>冻结结论</th><th>当前结论</th><th>是否变化</th><th>冻结决定性规则</th><th>当前决定性规则</th></tr>
          </thead>
          <tbody>
            <tr v-for="item in ruleDiff.comparison" :key="item.record_no" :data-testid="`diff-${item.record_no}`">
              <td>{{ item.record_no }}</td>
              <td>{{ item.event_id }}</td>
              <td>{{ item.frozen_verdict }}</td>
              <td>{{ item.current_verdict }}</td>
              <td>{{ item.changed ? '变化' : '不变' }}</td>
              <td>{{ item.frozen_decisive_rule }}</td>
              <td>{{ item.current_decisive_rule }}</td>
            </tr>
          </tbody>
        </table>
      </template>
    </template>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { RouterLink } from 'vue-router'

import {
  dispatchBatch,
  listFilterBatches,
  loadOutbound,
  loadReconcile,
  loadResultSet,
  loadRuleDiff,
} from '@/api/backend-service'
import type {
  BatchListItem,
  BatchSummary,
  FilterRecord,
  OutboundEntry,
  ReconcileResult,
  RuleDiffResult,
} from '@/api/backend-service'

type DedupedItem = {
  event_id: string
  source_table: string
  operation: string
  duplicate_of: number
  reason: string
}

type ResultSet = {
  batch_id: string
  rule_version: { version_id: string; rule_count: number }
  summary: BatchSummary
  allowed: FilterRecord[]
  blocked: FilterRecord[]
  deduped: DedupedItem[]
}

const batches = ref<BatchListItem[]>([])
const selectedBatchId = ref('')
const selectedBatch = ref<(ResultSet & { records: FilterRecord[] }) | null>(null)
const outbound = ref<OutboundEntry[]>([])
const reconcile = ref<ReconcileResult | null>(null)
const ruleDiff = ref<RuleDiffResult | null>(null)
const errorMessage = ref('')
const noticeMessage = ref('')

function describe(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback
}

async function loadBatches() {
  const payload = await listFilterBatches()
  batches.value = payload.items
  if (!selectedBatchId.value && payload.items.length) {
    selectedBatchId.value = payload.items[0].batch_id
  }
}

async function selectBatch(batchId: string) {
  selectedBatchId.value = batchId
  ruleDiff.value = null
  await loadDetail()
}

async function loadDetail() {
  if (!selectedBatchId.value) {
    selectedBatch.value = null
    outbound.value = []
    reconcile.value = null
    return
  }
  const resultSet = await loadResultSet(selectedBatchId.value)
  selectedBatch.value = {
    ...resultSet,
    records: [...resultSet.allowed, ...resultSet.blocked].sort(
      (a, b) => a.record_no - b.record_no,
    ),
  }
  const outboundPayload = await loadOutbound(selectedBatchId.value)
  outbound.value = outboundPayload.items
  reconcile.value = await loadReconcile(selectedBatchId.value)
}

async function refreshAll() {
  errorMessage.value = ''
  try {
    await loadBatches()
    await loadDetail()
  } catch (error) {
    errorMessage.value = describe(error, '过滤批次读取失败')
  }
}

async function runDispatch() {
  errorMessage.value = ''
  noticeMessage.value = ''
  if (!selectedBatchId.value) return
  try {
    const result = await dispatchBatch(selectedBatchId.value)
    noticeMessage.value =
      `批次 ${result.batch_id} 本轮投递 ${result.dispatched.length} 条：` +
      Object.entries((result.dispatch?.by_channel as Record<string, number>) ?? {})
        .map(([channel, count]) => `${channel}=${count}`)
        .join('，')
    await refreshAll()
  } catch (error) {
    errorMessage.value = describe(error, '批次分发失败')
  }
}

async function loadDiff() {
  if (!selectedBatchId.value) return
  errorMessage.value = ''
  try {
    ruleDiff.value = await loadRuleDiff(selectedBatchId.value)
  } catch (error) {
    errorMessage.value = describe(error, '规则前后对照读取失败')
  }
}

onMounted(refreshAll)
</script>

<style scoped>
.selected { background: #eef4ff; }
.section-title { margin: 18px 0 6px; font-size: 14px; }
.cell-detail { font-size: 12px; }
.hit-line, .out-line { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 2px; }
.hit-name { font-weight: 600; }
.hit-cond { color: var(--muted); }
.conflict-line { margin-top: 4px; color: #92400e; background: #fffbeb; padding: 4px 6px; border-radius: 4px; }
.tag-allow { color: #15803d; }
.tag-deny { color: #b42318; }
.tag-muted { color: var(--muted); }
.tag-conflict { margin-left: 4px; color: #92400e; }
.notice-text { color: #15803d; }
tr.conflict td { background: #fffdf5; }
.reconcile { display: flex; flex-wrap: wrap; gap: 12px; padding: 10px 12px; background: #fff; border: 1px solid var(--border); border-radius: 8px; font-size: 13px; }
</style>
