<template>
  <section class="page retention-page">
    <header class="page-head">
      <div>
        <h2>保留边界与状态恢复</h2>
        <p class="page-desc">
          状态压缩先看检查点保留窗口：至少保留每个可用快照引用的状态键；恢复文件可下载、可再次上传校验。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="retention-refresh" type="button" @click="reload">
          刷新
        </button>
        <button class="btn primary" data-testid="retention-export" type="button" @click="download">
          下载状态恢复数据
        </button>
      </div>
    </header>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>检查点配置</span>
        <select v-model="selectedConfig" data-testid="retention-config" @change="reload">
          <option v-for="item in configs" :key="item.config" :value="item.config">
            {{ item.config }}（可用快照 {{ item.available_snapshots }}）
          </option>
        </select>
      </label>
      <label class="filter-item">
        <span>文件边界</span>
        <select v-model="boundary" data-testid="retention-boundary">
          <option value="checkpoint">按检查点（逐快照完整导出）</option>
          <option value="state">按状态键（去重最新版本）</option>
        </select>
      </label>
      <label class="filter-item">
        <span>保留窗口缩短为</span>
        <input v-model.number="keepWindow" data-testid="retention-keep" min="0" type="number" />
      </label>
      <label class="filter-item">
        <span>裁剪时机</span>
        <select v-model="retentionMode" data-testid="retention-mode">
          <option value="immediate">立即裁剪</option>
          <option value="deferred">等新回放成功后裁剪</option>
        </select>
      </label>
      <button class="btn" data-testid="retention-apply" type="button" @click="applyWindow">
        缩短保留窗口
      </button>
      <button class="btn" data-testid="retention-drill" type="button" @click="drillReplay">
        用可用快照回放一次
      </button>
    </form>

    <p v-if="message" class="status-legend" data-testid="retention-message">{{ message }}</p>
    <p v-if="errorMessage" class="error-text" data-testid="retention-error">{{ errorMessage }}</p>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">可用快照数</span>
        <strong class="stat-value" data-testid="retention-available">{{ report?.available_snapshots ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">连续可恢复区间</span>
        <strong class="stat-value" data-testid="retention-interval">
          {{ report?.interval.length ?? 0 }}
        </strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">快照引用状态键</span>
        <strong class="stat-value">{{ referencedKeyCount }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">全部可恢复</span>
        <strong class="stat-value" data-testid="retention-restorable">
          {{ report?.all_restorable ? '是' : '否' }}
        </strong>
      </article>
    </div>

    <h3>检查点保留原因</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>检查点</th><th>快照时间</th><th>状态</th><th>可用</th>
          <th>状态键</th><th>保留原因</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="item in report?.snapshots ?? []"
          :key="item.checkpoint_id"
          :data-testid="`retention-snapshot-${item.checkpoint_id}`"
        >
          <td>{{ item.checkpoint_id }}</td>
          <td>{{ formatTs(item.snapshot_ts) }}</td>
          <td>{{ item.status }}</td>
          <td>{{ item.available ? '是' : '否' }}</td>
          <td class="mono">{{ item.state_keys.join('、') || '—' }}</td>
          <td>{{ item.retain_reason }}</td>
        </tr>
        <tr v-if="!report?.snapshots?.length">
          <td colspan="6" class="empty-state">该配置下暂无检查点快照</td>
        </tr>
      </tbody>
    </table>

    <h3>状态存储保留原因</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>状态行</th><th>算子</th><th>状态键</th><th>状态</th>
          <th>来源</th><th>引用快照</th><th>保留原因</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="item in report?.state_retention ?? []"
          :key="item.state_row_id"
          :data-testid="`state-retention-${item.state_row_id}`"
        >
          <td>{{ item.state_row_id }}</td>
          <td>{{ item.operator || '—' }}</td>
          <td class="mono">{{ item.state_keys.join('、') }}</td>
          <td>{{ item.status }}</td>
          <td>{{ item.source || 'runtime' }}</td>
          <td class="mono">{{ item.referenced_by.join('、') || '—' }}</td>
          <td>{{ item.retain_reason }}</td>
        </tr>
        <tr v-if="!report?.state_retention?.length">
          <td colspan="7" class="empty-state">该状态配置下暂无可展示的状态行</td>
        </tr>
      </tbody>
    </table>

    <h3>回放历史</h3>
    <p v-if="replayCoverage" class="status-legend" data-testid="replay-coverage">
      可用快照 {{ replayCoverage.available_checkpoints }} 个，
      已演练可恢复 {{ replayCoverage.restored_checkpoints }} 个，
      全部可恢复：{{ replayCoverage.all_available_restorable ? '是' : '否' }}
    </p>
    <table class="data-table">
      <thead>
        <tr>
          <th>任务</th><th>源检查点</th><th>回放模式</th><th>结果验证</th>
          <th>进度</th><th>状态</th><th>源快照可用</th><th>可恢复</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in replays" :key="item.task_id" :data-testid="`replay-${item.task_id}`">
          <td>{{ item.任务名称 }}</td>
          <td>{{ item.源检查点 }}</td>
          <td>{{ item.回放模式 }}</td>
          <td>{{ item.结果验证 }}</td>
          <td>{{ item.进度 }}</td>
          <td>{{ item.status }}</td>
          <td>{{ item.source_available ? '是' : '否' }}</td>
          <td>{{ item.restorable ? '是' : '否' }}</td>
        </tr>
        <tr v-if="!replays.length">
          <td colspan="8" class="empty-state">暂无回放记录，所有可用快照都尚未演练</td>
        </tr>
      </tbody>
    </table>

    <h3>再次上传校验（同源恢复文件）</h3>
    <form class="filter-bar" @submit.prevent="verifyUpload">
      <label class="filter-item">
        <span>恢复数据文件</span>
        <input data-testid="retention-file" type="file" accept="application/json,.json" @change="onFilePicked" />
      </label>
      <button class="btn" data-testid="retention-verify" type="submit">校验文件</button>
      <button class="btn primary" data-testid="retention-import" type="button" @click="importUpload">
        导入（幂等）
      </button>
    </form>
    <div v-if="verification" class="analysis-card" data-testid="retention-verification">
      <p>
        校验结果：<strong>{{ verification.valid ? '通过' : '未通过' }}</strong>
        （边界 {{ verification.boundary }}，检查点 {{ verification.checkpoint_count }} 个，
        状态条目 {{ verification.state_count }} 条）
      </p>
      <p>覆盖全部可用检查点：{{ verification.coverage?.fully_covered ? '是' : '否' }}</p>
      <ul v-if="verification.issues.length">
        <li v-for="issue in verification.issues" :key="issue" class="error-text">{{ issue }}</li>
      </ul>
    </div>
    <div v-if="importResult" class="analysis-card" data-testid="retention-import-result">
      <p v-if="importResult.ok">
        导入完成：入库 {{ importResult.imported }} 个状态键，
        {{ importResult.duplicated ? '该文件此前已导入过，本次未重复入库' : '本次为首批入库' }}。
      </p>
      <p v-else class="error-text">{{ importResult.error }}</p>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import type {
  BundleVerification,
  RecoveryBoundary,
  RecoveryImportResult,
  ReplayCoverage,
  ReplayHistoryItem,
  RetentionConfig,
  RetentionReport,
} from '@/domain/retention'
import {
  applyRetentionWindow,
  downloadRecoveryFile,
  importRecoveryFile,
  listConfigs,
  loadReplayHistory,
  loadReport,
  runReplayDrill,
  verifyRecoveryFile,
} from '@/domain/retention'

const configs = ref<RetentionConfig[]>([])
const selectedConfig = ref('')
const boundary = ref<RecoveryBoundary>('checkpoint')
const keepWindow = ref(2)
const retentionMode = ref<'immediate' | 'deferred'>('immediate')
const report = ref<RetentionReport | null>(null)
const replays = ref<ReplayHistoryItem[]>([])
const replayCoverage = ref<ReplayCoverage | null>(null)
const message = ref('')
const errorMessage = ref('')
const pickedFile = ref<File | null>(null)
const verification = ref<BundleVerification | null>(null)
const importResult = ref<RecoveryImportResult | null>(null)

const referencedKeyCount = computed(() =>
  Object.keys(report.value?.referenced_state_keys ?? {}).length,
)

function formatTs(ts: number): string {
  if (!ts) return '—'
  return new Date(ts * 1000).toLocaleString('zh-CN', { hour12: false })
}

function describeError(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback
}

async function reload() {
  errorMessage.value = ''
  try {
    if (!configs.value.length) {
      configs.value = await listConfigs()
    }
    if (!selectedConfig.value && configs.value.length) {
      selectedConfig.value = configs.value[0].config
    }
    const [reportPayload, replayPayload] = await Promise.all([
      loadReport(selectedConfig.value),
      loadReplayHistory(selectedConfig.value),
    ])
    report.value = reportPayload
    replays.value = replayPayload.items
    replayCoverage.value = replayPayload.coverage
    keepWindow.value = Number(reportPayload.keep) || keepWindow.value
  } catch (error) {
    errorMessage.value = describeError(error, '保留边界读取失败')
  }
}

async function download() {
  errorMessage.value = ''
  message.value = ''
  try {
    await downloadRecoveryFile(selectedConfig.value, boundary.value)
    message.value = `已下载「${selectedConfig.value}」的状态恢复数据（边界：${boundary.value}）`
  } catch (error) {
    errorMessage.value = describeError(error, '状态恢复数据下载失败')
  }
}

async function applyWindow() {
  errorMessage.value = ''
  message.value = ''
  try {
    const result = await applyRetentionWindow(
      selectedConfig.value, keepWindow.value, retentionMode.value)
    message.value = result.deferred.length
      ? `已登记 ${result.deferred.length} 个待裁剪快照，等新回放成功后再释放`
      : `已立即裁剪 ${result.pruned.length} 个快照，剩余可用 ${result.available_after} 个`
    await reload()
  } catch (error) {
    errorMessage.value = describeError(error, '保留窗口调整失败')
  }
}

async function drillReplay() {
  errorMessage.value = ''
  message.value = ''
  try {
    const result = await runReplayDrill(selectedConfig.value)
    if (!result.ok) {
      errorMessage.value = result.execution?.error ?? '回放演练未通过'
    } else {
      const released = result.retention?.pruned.length ?? 0
      message.value = released
        ? `回放通过，已释放 ${released} 个延迟裁剪快照，剩余可用 ${result.report.available_snapshots} 个`
        : `回放通过，剩余可用快照 ${result.report.available_snapshots} 个`
    }
    await reload()
  } catch (error) {
    errorMessage.value = describeError(error, '回放演练失败')
  }
}

function onFilePicked(event: Event) {
  const input = event.target as HTMLInputElement
  pickedFile.value = input.files?.[0] ?? null
  verification.value = null
  importResult.value = null
}

async function verifyUpload() {
  errorMessage.value = ''
  message.value = ''
  if (!pickedFile.value) {
    errorMessage.value = '请先选择恢复数据文件'
    return
  }
  try {
    verification.value = await verifyRecoveryFile(pickedFile.value)
  } catch (error) {
    errorMessage.value = describeError(error, '恢复数据校验失败')
  }
}

async function importUpload() {
  errorMessage.value = ''
  message.value = ''
  if (!pickedFile.value) {
    errorMessage.value = '请先选择恢复数据文件'
    return
  }
  try {
    importResult.value = await importRecoveryFile(pickedFile.value)
    if (importResult.value.ok) await reload()
  } catch (error) {
    errorMessage.value = describeError(error, '恢复数据导入失败')
  }
}

onMounted(reload)
</script>

<style scoped>
.mono {
  font-family: monospace;
  font-size: 12px;
}
h3 {
  margin: 18px 0 8px;
  font-size: 14px;
}
</style>
