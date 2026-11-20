import { createRouter, createWebHistory } from 'vue-router'

import FilterBatchView from '@/views/FilterBatchView.vue'
import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/event_filter/batches', name: 'filter-batches', component: FilterBatchView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
