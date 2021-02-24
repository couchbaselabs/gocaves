<template>
  <b-container>
    <b-row>
      <b-col>
        <b-card>
          <div class="d-flex flex-row">
            <div>
              <ReportStateIcon :status="report.tests.status" :reason="report.tests.statusReason" class="title-icon" />
            </div>
            <div class="flex-grow-1 ml-4">
              <div><b>Report ID:</b> {{ report.id }}</div>
              <div><b>Client:</b> {{ report.client }}</div>
              <div><b>Date:</b> {{ report.createdAt | date('MMM D, YYYY h:mm A') }}</div>
            </div>
            <div class="text-center test-count-box ml-3">
              <div class="value">{{ report.tests.numSuccess }}</div>
              <div class="title">Passed</div>
            </div>
            <div class="text-center test-count-box ml-3">
              <div class="value">{{ report.tests.numFailed }}</div>
              <div class="title">Failed</div>
            </div>
            <div class="text-center test-count-box ml-3">
              <div class="value">{{ report.tests.numSkipped }}</div>
              <div class="title">Skipped</div>
            </div>
          </div>
        </b-card>
      </b-col>
    </b-row>
    <b-row>
      <b-col>
        <b-card class="mt-3">
          <template #header>
            <div class="d-flex flex-row align-items-end">
              <div class="flex-grow-1">Tests</div>
              <div>
                <b-form-checkbox v-model="hideSuccessfulTests">Hide Successful Tests</b-form-checkbox>
              </div>
            </div>
          </template>

          <TestGroup :report-id="report.id" :group="report.tests" :hide-successful="hideSuccessfulTests" />
        </b-card>
      </b-col>
    </b-row>

    <b-row v-if="debugMode">
      <b-col>
        <b-card class="mt-3">
          <template #header>Debug Data</template>
          <pre>{{ report }}</pre>
        </b-card>
      </b-col>
    </b-row>
  </b-container>
</template>

<style scoped>
.title-icon {
  font-size: 3rem;
}

.test-count-box {
  width: 6rem;
  border: 1px solid rgba(0, 0, 0, 0.125);
  border-radius: 0.25rem;
}

.test-count-box .value {
  font-size: 2rem;
}

.test-count-box .title {
  font-size: 0.8rem;
}
</style>

<script>
export default {
  data() {
    return {
      hideSuccessfulTests: true,
    }
  },
  props: {
    report: {
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
