<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>运营概览</h2>
        <p class="page-desc">汇总各业务模块的关键指标，先看总量再看异常。</p>
      </div>
    </header>
    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>
    <table class="data-table">
      <thead>
        <tr><th>业务模块</th><th>今日新增</th><th>待处理</th><th>异常量</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in moduleRows" :key="row.name">
          <td>{{ row.name }}</td>
          <td>{{ row.created }}</td>
          <td>{{ row.pending }}</td>
          <td>{{ row.abnormal }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'

type Overview = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

const cards = ref<Overview['cards']>([])
const moduleRows = ref<Overview['modules']>([])

onMounted(async () => {
  try {
    const payload = await fetchJson<Overview>('/api/overview')
    cards.value = payload.cards
    moduleRows.value = payload.modules
  } catch {
    cards.value = [{"label": "业务模块", "value": 0}, {"label": "今日新增", "value": 0}]
    moduleRows.value = [{"name": "Binlog解析", "created": 0, "pending": 0, "abnormal": 0}, {"name": "变更事件", "created": 0, "pending": 0, "abnormal": 0}, {"name": "DDL追踪", "created": 0, "pending": 0, "abnormal": 0}, {"name": "Schema同步", "created": 0, "pending": 0, "abnormal": 0}, {"name": "采集通道", "created": 0, "pending": 0, "abnormal": 0}, {"name": "检查点管理", "created": 0, "pending": 0, "abnormal": 0}, {"name": "位点存储", "created": 0, "pending": 0, "abnormal": 0}, {"name": "格式适配", "created": 0, "pending": 0, "abnormal": 0}, {"name": "缓慢变化维", "created": 0, "pending": 0, "abnormal": 0}, {"name": "去重窗口", "created": 0, "pending": 0, "abnormal": 0}, {"name": "路由分发", "created": 0, "pending": 0, "abnormal": 0}, {"name": "回填位点", "created": 0, "pending": 0, "abnormal": 0}, {"name": "延迟监控", "created": 0, "pending": 0, "abnormal": 0}, {"name": "快照比对", "created": 0, "pending": 0, "abnormal": 0}, {"name": "事件过滤", "created": 0, "pending": 0, "abnormal": 0}, {"name": "分区同步", "created": 0, "pending": 0, "abnormal": 0}, {"name": "审计轨迹", "created": 0, "pending": 0, "abnormal": 0}, {"name": "故障切换", "created": 0, "pending": 0, "abnormal": 0}]
  }
})
</script>
