<template>
  <b-container fluid>
    <b-row>
      <b-col>
        <b-card>
          <template #header>
            <span>Description</span>
          </template>
          <pre>{{ test.desc }}</pre>
        </b-card>
      </b-col>
      <b-col>
        <b-card>
          <template #header>
            <span>Result</span>
          </template>

          <span class="big-status">
            <ReportStateIcon :status="test.status" class="mr-2" />

            <span v-if="test.status === 'success'">Passed</span>
            <span v-else-if="test.status === 'failed'">Failed</span>
            <span v-else-if="test.status === 'skipped'">Skipped</span>
            <span v-else>Unknown ({{ test.status }})</span>
          </span>
        </b-card>
        <b-card class="mt-3">
          <template #header>
            <span>Logs</span>
          </template>
          <b-table-simple small striped hover>
            <b-tbody>
              <b-tr><b-td>Test Started</b-td></b-tr>
              <b-tr v-for="(logText, logIdx) in test.logs" :key="logIdx">
                <b-td>
                  <pre>{{ logText }}</pre>
                </b-td>
              </b-tr>
              <b-tr><b-td>Test Ended</b-td></b-tr>
            </b-tbody>
          </b-table-simple>
        </b-card>
      </b-col>
    </b-row>

    <b-row v-if="debugMode">
      <b-col>
        <b-card class="mt-3">
          <template #header>Debug Data</template>
          <pre>{{ test }}</pre>
        </b-card>
      </b-col>
    </b-row>
  </b-container>
</template>

<style scoped>
.big-status {
  font-size: 2rem;
  font-weight: bold;
}
</style>

<script>
export default {
  props: {
    report: {
      type: Object,
      required: true,
    },
    test: {
      type: Object,
      required: true,
    },
  },
  computed: {
    debugMode() {
      return this.$store.state.debugMode
    },
  },
}
</script>
