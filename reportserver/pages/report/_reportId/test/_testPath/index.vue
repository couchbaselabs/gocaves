<template>
  <div>
    <b-breadcrumb>
      <b-breadcrumb-item to="/">Home</b-breadcrumb-item>
      <b-breadcrumb-item active>Reports</b-breadcrumb-item>
      <b-breadcrumb-item :to="`/report/${reportId}`">{{ reportId }}</b-breadcrumb-item>
      <b-breadcrumb-item active>Tests</b-breadcrumb-item>
      <b-breadcrumb-item v-for="testPart in testName.split('/')" :key="testPart" active>
        {{ testPart }}
      </b-breadcrumb-item>
    </b-breadcrumb>

    <div v-if="!!report && !!test">
      <TestView :report="report" :test="test" />
    </div>
    <b-container v-else>
      <p>We were unable to find this test</p>
    </b-container>
  </div>
</template>

<script>
function deepForEachTest(group, callback) {
  group.tests.forEach(callback)
  group.groups.forEach((childGroup) => {
    deepForEachTest(childGroup, callback)
  })
}

function deepFindTest(group, callback) {
  let foundTest = null
  deepForEachTest(group, (test) => {
    if (!foundTest && callback(test)) {
      foundTest = test
    }
  })
  return foundTest
}

export default {
  async asyncData({ params }) {
    const reportId = params.reportId
    const testName = params.testPath.replace(/\-/g, '/')
    return { reportId, testName }
  },
  data() {
    return {
      reportId: '',
      testName: '',
    }
  },
  computed: {
    report() {
      const reports = this.$store.state.reports
      return reports.find((reportData) => reportData.id === this.reportId)
    },
    test() {
      if (!this.report) {
        return null
      }

      return deepFindTest(this.report.tests, (test) => test.name === this.testName)
    },
  },
}
</script>
