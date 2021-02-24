<template>
  <span>
    <span :id="elId()">
      <fa v-if="status === 'success'" :icon="['fas', 'check-circle']" class="text-success" />
      <fa v-else-if="status === 'warning'" :icon="['fas', 'exclamation-circle']" class="text-warning" />
      <fa v-else-if="status === 'failed'" :icon="['fas', 'times-circle']" class="text-danger" />
      <fa v-else :icon="['fas', 'question-circle']" class="text-secondary" />
    </span>
    <b-tooltip v-if="!!reason" :target="elId()">{{ reason }}</b-tooltip>
  </span>
</template>

<script>
export default {
  props: {
    status: {
      type: String,
      required: true,
    },
    reason: {
      type: String,
    },
  },
  methods: {
    elId() {
      return `reportstateicon-${this._uid}`
    },
  },
}
</script>
