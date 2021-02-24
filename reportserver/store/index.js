'use strict'

import { WsManager } from '~/plugins/wsmanager.client'

export const state = () => ({
  reports: [],
  debugMode: false,
})

export const mutations = {
  enableDebugMode(state) {
    state.debugMode = true
  },
  disableDebugMode(state) {
    state.debugMode = false
  },
  addReport(state, reportData) {
    // Check to make sure the report doesn't already exist.  Delete it if it does.
    const existingIdx = state.reports.findIndex(
      (foundReportData) => foundReportData.id === reportData.id
    )
    if (existingIdx !== -1) {
      state.reports.splice(existingIdx, 1)
    }

    // Add the new report to the end of the reports listing.
    state.reports.push(reportData)
  },
}

export const actions = {
  nuxtClientInit(_, context) {
    context.store.wsManager = new WsManager(context.store, context.app)
  },
}
