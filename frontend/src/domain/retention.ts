/** 检查点保留边界 / 状态恢复数据的页面数据层。与后端 checkpoint_retention 路由一一对应。 */

import { request } from '@/api/client'

export type RetentionSnapshot = {
  checkpoint_id: string
  snapshot_ts: number
  status: string
  available: boolean
  retain_reason: string
  state_keys: string[]
}

export type RecoverableInterval = {
  checkpoint_ids: string[]
  start_ts: number | null
  end_ts: number | null
  contiguous: boolean
  length: number
  broken_at?: string[] | null
}

export type StateRetentionItem = {
  state_row_id: number
  config: string
  operator: string
  state_keys: string[]
  state_versions: Record<string, string>
  status: string
  source: string
  retain_reason: string
  referenced_by: string[]
}

export type RetentionReport = {
  ok: boolean
  config: string
  keep: number
  available_snapshots: number
  interval: RecoverableInterval
  contiguous: boolean
  all_restorable: boolean
  referenced_state_keys: Record<string, string[]>
  snapshots: RetentionSnapshot[]
  state_retention: StateRetentionItem[]
}

export type ReplayHistoryItem = {
  task_id: number
  任务名称: string
  源检查点: string
  回放模式: string
  结果验证: string
  进度: string
  status: string
  source_available: boolean
  restorable: boolean
  linked_restore_plan?: string
}

export type RetentionConfig = {
  config: string
  status: string
  keep: string | number
  state_config: string
  available_snapshots: number
}

export type RecoveryBoundary = 'checkpoint' | 'state'

export type BundleVerification = {
  valid: boolean
  issues: string[]
  version?: string
  boundary?: string
  checkpoint_count?: number
  state_count?: number
  duplicated_state_keys?: string[]
  coverage?: {
    fully_covered: boolean
    covered_checkpoints: string[]
    uncovered_checkpoints: { checkpoint_id: string; missing_keys: string[] }[]
  }
}

export type ReplayDrillResult = {
  ok: boolean
  task_id: number
  checkpoint_id: string
  execution: {
    ok: boolean
    error?: string
    verification?: {
      all_passed: boolean
      consistency?: {
        consistent: boolean
        missing_keys: string[]
        extra_keys: string[]
      }
    }
    retention?: {
      pruned: string[]
      compacted: number
      available_after: number
    } | null
  }
  retention?: { pruned: string[]; compacted: number } | null
  report: {
    available_snapshots: number
    contiguous: boolean
    interval: RecoverableInterval
  }
}

export type RecoveryImportResult = {
  ok: boolean
  error?: string
  imported?: number
  versions_imported?: number
  duplicated?: boolean
  batch_id?: string | null
  version?: string
  covers_all_checkpoints?: boolean
  validation?: BundleVerification
}

export async function listConfigs(): Promise<RetentionConfig[]> {
  const response = await request('/api/checkpoint-retention/configs')
  if (!response.ok) throw new Error('检查点配置读取失败')
  const payload = await response.json()
  return payload.configs ?? []
}

export async function loadReport(config: string): Promise<RetentionReport> {
  const response = await request(`/api/checkpoint-retention/${encodeURIComponent(config)}/report`)
  if (!response.ok) throw new Error('保留原因读取失败')
  return await response.json()
}

export type ReplayCoverage = {
  available_checkpoints: number
  restored_checkpoints: number
  all_available_restorable: boolean
  missing_checkpoints: string[]
}

export async function loadReplayHistory(config?: string): Promise<{
  items: ReplayHistoryItem[]
  coverage: ReplayCoverage
}> {
  const query = config ? `?config=${encodeURIComponent(config)}` : ''
  const response = await request(`/api/checkpoint-retention/replays${query}`)
  if (!response.ok) throw new Error('回放历史读取失败')
  const payload = await response.json()
  return {
    items: payload.items ?? [],
    coverage: payload.coverage ?? {
      available_checkpoints: 0,
      restored_checkpoints: 0,
      all_available_restorable: true,
      missing_checkpoints: [],
    },
  }
}

export async function downloadRecoveryFile(
  config: string,
  boundary: RecoveryBoundary,
): Promise<void> {
  const response = await request(
    `/api/checkpoint-retention/${encodeURIComponent(config)}/export?boundary=${boundary}`,
  )
  if (!response.ok) throw new Error('状态恢复数据下载失败')
  const disposition = response.headers.get('Content-Disposition') ?? ''
  const matched = disposition.match(/filename="([^"]+)"/)
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = matched?.[1] ?? `recovery_${config}_${boundary}.json`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function verifyRecoveryFile(file: File): Promise<BundleVerification> {
  const bundle = await readBundle(file)
  const response = await request('/api/checkpoint-retention/verify', {
    method: 'POST',
    body: JSON.stringify({ bundle }),
  })
  if (!response.ok) throw new Error('恢复数据校验请求失败')
  return await response.json()
}

export async function importRecoveryFile(
  file: File,
  dryRun = false,
): Promise<RecoveryImportResult> {
  const bundle = await readBundle(file)
  const response = await request('/api/checkpoint-retention/import', {
    method: 'POST',
    body: JSON.stringify({ bundle, dry_run: dryRun }),
  })
  if (!response.ok) throw new Error('恢复数据导入请求失败')
  return await response.json()
}

export async function applyRetentionWindow(
  config: string,
  keep: number,
  mode: 'immediate' | 'deferred',
): Promise<{ ok: boolean; pruned: string[]; deferred: string[]; available_after: number }> {
  const response = await request(
    `/api/checkpoint-retention/${encodeURIComponent(config)}/window`,
    { method: 'POST', body: JSON.stringify({ keep, mode }) },
  )
  const payload = await response.json()
  if (!response.ok) throw new Error(payload.detail ?? '保留窗口调整失败')
  return payload
}

export async function runReplayDrill(
  config: string,
  checkpointId?: string,
): Promise<ReplayDrillResult> {
  const response = await request(
    `/api/checkpoint-retention/${encodeURIComponent(config)}/replay`,
    { method: 'POST', body: JSON.stringify({ checkpoint_id: checkpointId ?? null }) },
  )
  const payload = await response.json()
  if (!response.ok) throw new Error(payload.detail ?? '回放演练失败')
  return payload
}

export async function loadStateRetention(
  config: string,
): Promise<{ items: StateRetentionItem[]; active_state_keys: { pattern: string; count: number }[] }> {
  const response = await request(
    `/api/checkpoint-retention/${encodeURIComponent(config)}/state-retention`,
  )
  if (!response.ok) throw new Error('状态保留原因读取失败')
  return await response.json()
}

async function readBundle(file: File): Promise<Record<string, unknown>> {
  const text = await file.text()
  try {
    const parsed = JSON.parse(text)
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error('恢复数据文件根节点必须是对象')
    }
    return parsed as Record<string, unknown>
  } catch (error) {
    if (error instanceof SyntaxError) throw new Error('恢复数据文件不是合法 JSON')
    throw error
  }
}
