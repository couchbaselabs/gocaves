<template>
  <div>
    <b-breadcrumb>
      <b-breadcrumb-item to="/">Home</b-breadcrumb-item>
      <b-breadcrumb-item active>Reports</b-breadcrumb-item>
      <b-breadcrumb-item active>{{ reportId }}</b-breadcrumb-item>
    </b-breadcrumb>

    <b-container v-if="!!report" fluid>
      <ReportView :report="report" />
    </b-container>

    <b-container v-else fluid>
      <p>We were unable to find this report</p>
    </b-container>
  </div>
</template>

<script>
export default {
  async asyncData({ params }) {
    const reportId = params.reportId
    return { reportId }
  },
  data() {
    return {
      reportId: '',
    }
  },
  computed: {
    report() {
      const reports = this.$store.state.reports
      return reports.find((reportData) => reportData.id === this.reportId)
    },
  },
}
</script>
