<template>
  <div>
    <b-breadcrumb>
      <b-breadcrumb-item to="/">Home</b-breadcrumb-item>
    </b-breadcrumb>

    <b-container>
      <b-row>
        <b-col>
          The following is a list of all reports that have been submitted to this reporting server. Click to view one.
          The most recently received reports appear at the top of the list.
        </b-col>
      </b-row>

      <b-row class="mt-3">
        <b-col>
          <b-table-simple hover small>
            <b-thead>
              <b-tr>
                <b-th></b-th>
                <b-th>Date</b-th>
                <b-th>Report ID</b-th>
                <b-th>Client</b-th>
                <b-th>Passed</b-th>
                <b-th>Skipped</b-th>
              </b-tr>
            </b-thead>
            <b-tbody>
              <b-tr v-for="report in reports" :key="report.id" class="clickable" @click.prevent="gotoReport(report.id)">
                <b-td>
                  <ReportStateIcon :status="report.tests.status" :reason="report.tests.statusReason" />
                </b-td>
                <b-td>{{ report.createdAt | date('MMM D, YYYY h:mm A') }}</b-td>
                <b-td>{{ report.id }}</b-td>
                <b-td>{{ report.client }}</b-td>
                <b-td>
                  <span>{{ report.tests.numSuccess }} / {{ report.tests.numAttempted }}</span>
                  <span>({{ report.tests.numFailed }} failed)</span>
                </b-td>
                <b-td>{{ report.tests.numSkipped }}</b-td>
              </b-tr>
            </b-tbody>
          </b-table-simple>
        </b-col>
      </b-row>
    </b-container>
  </div>
</template>

<style scoped>
.clickable {
  cursor: pointer;
}
</style>

<script>
export default {
  computed: {
    reports() {
      return this.$store.state.reports
    },
  },
  methods: {
    gotoReport(reportId) {
      this.$router.push(`/report/${reportId}`)
    },
  },
}
</script>
