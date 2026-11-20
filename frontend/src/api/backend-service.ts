import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

export type FilterHit = {
  rule_id: number
  rule_name: string
  filter_type: string
  expression: string
  is_whitelist: boolean
  priority: number
  scope: string
  effective_condition: string
  state: string
  reason: string
  verdict?: string
}

export type FilterRecord = {
  record_no: number
  batch_id: string
  event_id: string
  source_table: string
  operation: string
  binlog_position: string
  rule_version: string
  verdict: string
  allowed: boolean
  decisive_rule: string
  default_allow: boolean
  effective_priority: number | null
  conflict: {
    present: boolean
    decisive: boolean
    resolution: string
    levels: {
      priority: number
      allow_rules: string[]
      deny_rules: string[]
      decisive: boolean
      overridden_by_priority: number | null
    }[]
  }
  hits: FilterHit[]
  scope_out: FilterHit[]
}

export type BatchSummary = {
  total_submitted: number
  effective_count: number
  allowed_count: number
  blocked_count: number
  duplicate_count: number
  conflict_count: number
  scope_out_count: number
}

export type BatchResult = {
  batch_id: string
  replayed: boolean
  replay_of: string
  records: FilterRecord[]
  summary: BatchSummary
  rule_version: { version_id: string; digest: string; rule_count: number; rule_ids: number[] }
}

export type BatchListItem = {
  batch_id: string
  source: string
  received_at: string
  rule_version: string
  rule_digest: string
  rule_count: number
  summary: BatchSummary
  replay_count: number
  dispatch: { channel: string; dispatched_count: number; outbound_total: number; consistent: boolean } | null
}

export type OutboundEntry = {
  outbound_id: string
  batch_id: string
  record_no: number
  event_id: string
  source_table: string
  operation: string
  target_channel: string
  rule_version: string
  dispatched_at: string
}

export type ReconcileResult = {
  batch_id: string
  rule_version: string
  status: string
  allowed_count: number
  blocked_count: number
  outbound_count: number
  consistent: boolean
  missing_from_outbound: string[]
  blocked_but_dispatched: string[]
  duplicated_outbound: string[]
}

export type RuleDiffItem = {
  record_no: number
  event_id: string
  frozen_verdict: string
  current_verdict: string
  changed: boolean
  frozen_decisive_rule: string
  current_decisive_rule: string
}

export type RuleDiffResult = {
  batch_id: string
  frozen_rule_version: string
  current_rule_version: string
  rule_changed: boolean
  changed_count: number
  comparison: RuleDiffItem[]
}

async function readJson<T>(response: Response, message: string): Promise<T> {
  if (!response.ok) {
    let detail = message
    try {
      const body = await response.json()
      if (body?.detail) detail = String(body.detail)
    } catch {
      /* 保留默认提示 */
    }
    throw new Error(detail)
  }
  return (await response.json()) as T
}

export async function receiveFilterBatch(
  events: EntryRow[],
  source = 'change_event',
): Promise<BatchResult> {
  const response = await request('/api/filter/batches', {
    method: 'POST',
    body: JSON.stringify({ events, source }),
  })
  return readJson<BatchResult>(response, '过滤批次接收失败')
}

export async function listFilterBatches(): Promise<{ items: BatchListItem[]; total: number }> {
  const response = await request('/api/filter/batches')
  return readJson<{ items: BatchListItem[]; total: number }>(response, '过滤批次读取失败')
}

export async function loadResultSet(batchId: string): Promise<{
  batch_id: string
  rule_version: { version_id: string; rule_count: number }
  summary: BatchSummary
  allowed: FilterRecord[]
  blocked: FilterRecord[]
  deduped: {
    event_id: string
    source_table: string
    operation: string
    duplicate_of: number
    reason: string
  }[]
}> {
  const response = await request(`/api/filter/batches/${encodeURIComponent(batchId)}/result-set`)
  return readJson(response, '过滤结果集读取失败')
}

export async function dispatchBatch(
  batchId: string,
  channel?: string,
): Promise<{
  batch_id: string
  channel?: string
  dispatched: OutboundEntry[]
  skipped: number
  dispatch: { by_channel?: Record<string, number>; [key: string]: unknown }
}> {
  const response = await request('/api/filter/dispatch', {
    method: 'POST',
    body: JSON.stringify({ batch_id: batchId, ...(channel ? { channel } : {}) }),
  })
  return readJson(response, '批次分发失败')
}

export async function loadOutbound(batchId: string): Promise<{ items: OutboundEntry[]; total: number }> {
  const response = await request(`/api/filter/outbound?batch_id=${encodeURIComponent(batchId)}`)
  return readJson(response, '出站清单读取失败')
}

export async function loadReconcile(batchId: string): Promise<ReconcileResult> {
  const response = await request(`/api/filter/batches/${encodeURIComponent(batchId)}/reconcile`)
  return readJson(response, '一致性核对失败')
}

export async function loadRuleDiff(batchId: string): Promise<RuleDiffResult> {
  const response = await request(`/api/filter/batches/${encodeURIComponent(batchId)}/rule-diff`)
  return readJson(response, '规则前后对照读取失败')
}
