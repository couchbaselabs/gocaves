<template>
  <div>
    <div v-for="subgroup in group.groups" :key="subgroup.name">
      <b-card v-if="isSubgroupVisible(subgroup)" class="mb-2">
        <template #header>
          <fa :icon="['fas', 'caret-down']" />

          <ReportStateIcon :status="subgroup.status" :reason="subgroup.statusReason" />
          <span>{{ subgroup.name }}</span>
        </template>

        <TestGroup :report-id="reportId" :group="subgroup" :hide-successful="hideSuccessful" />
      </b-card>
    </div>
    <ul class="mb-0">
      <li v-for="test in group.tests" :key="test.name">
        <nuxt-link :to="`${reportId}/test/${testNameToPath(test.name)}`">
          <div class="py-1">
            <ReportStateIcon :status="test.status" />

            <span>{{ test.name }}</span>
          </div>
        </nuxt-link>
      </li>
    </ul>
  </div>
</template>

<script>
export default {
  name: 'TestGroup',
  props: {
    reportId: {
      type: String,
      required: true,
    },
    group: {
      type: Object,
      required: true,
    },
    hideSuccessful: {
      type: Boolean,
      default: false,
    },
  },
  methods: {
    isSubgroupVisible(subgroup) {
      if (!this.hideSuccessful) {
        return true
      }

      return subgroup.numTests > subgroup.numSuccess
    },
    testNameToPath(name) {
      return name.replace(/\//g, '-')
    },
  },
}
</script>
