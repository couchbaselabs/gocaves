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
            <span>Check Logs</span>
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

    <b-row>
      <b-col>
        <b-card class="mt-3" body-class="packets-card">
          <template #header>Packets</template>

          <b-table-simple small>
            <b-thead>
              <b-tr>
                <b-th></b-th>
                <b-th>ID</b-th>
                <b-th>Source</b-th>
                <b-th>Dest</b-th>
                <b-th>Magic</b-th>
                <b-th>Opaque</b-th>
                <b-th>Command</b-th>
                <b-th>Vb ID</b-th>
                <b-th>Status</b-th>
                <b-th>Datatype</b-th>
                <b-th>CAS</b-th>
                <b-th>CID</b-th>
                <b-th>Key</b-th>
              </b-tr>
            </b-thead>
            <b-tbody>
              <template v-for="(pak, pakIdx) in test.packets">
                <b-tr
                  :class="{ 'packet-in': pak.dir === 'in', 'packet-out': pak.dir === 'out' }"
                  :key="`${pak.id}-summary`"
                  @click="togglePacketDetails(pakIdx)"
                >
                  <b-td>
                    <fa v-if="pak.dir === 'in'" :icon="['fas', 'chevron-right']" />
                    <fa v-if="pak.dir === 'out'" :icon="['fas', 'chevron-left']" />
                    {{ pak.isTls ? '(TLS)' : '' }}
                  </b-td>
                  <b-td>{{ pak.id }}</b-td>
                  <b-td>
                    <span v-if="!!pak.srcName">{{ pak.srcName }}</span>
                    <span v-else>{{ pak.srcAddr }}</span>
                  </b-td>
                  <b-td>
                    <span v-if="!!pak.destName">{{ pak.destName }}</span>
                    <span v-else>{{ pak.destAddr }}</span>
                  </b-td>
                  <b-td>{{ pak.magicString }} ({{ pak.magic }})</b-td>
                  <b-td>{{ pak.opaque }}</b-td>
                  <b-td>{{ pak.commandString }} ({{ pak.command }})</b-td>
                  <b-td>
                    <span v-if="pak.magicString.indexOf('request') !== -1">
                      {{ pak.vbId }}
                    </span>
                  </b-td>
                  <b-td>
                    <span v-if="pak.magicString.indexOf('response') !== -1">
                      {{ pak.statusString }} ({{ pak.status }})
                    </span>
                  </b-td>
                  <b-td>{{ pak.datatype }}</b-td>
                  <b-td>{{ formatHexCas(pak.casHex) }}</b-td>
                  <b-td>{{ pak.collectionId }}</b-td>
                  <b-td>{{ pak.keyBase64 | atob }}</b-td>
                </b-tr>
                <b-tr :key="`${pak.id}-details`" class="packet-details m-0 p-0">
                  <b-td colspan="13" class="m-0 p-0">
                    <div ref="pak-details" class="packet-hide-container">
                      <div class="packet-card">
                        <div>
                          <u><b>Description</b></u>
                        </div>
                        <b-table-simple class="packet-desc" small bordered fluid>
                          <b-tbody>
                            <b-tr>
                              <b-th>Src Addr</b-th>
                              <b-td>
                                <span>{{ pak.srcAddr }}</span>
                                <span v-if="!!pak.srcName">({{ pak.srcName }})</span>
                              </b-td>
                            </b-tr>
                            <b-tr>
                              <b-th>Dest Addr</b-th>
                              <b-td>
                                <span>{{ pak.destAddr }}</span>
                                <span v-if="!!pak.destName">({{ pak.destName }})</span>
                              </b-td>
                            </b-tr>
                            <b-tr>
                              <b-th>TLS</b-th>
                              <b-td>{{ pak.isTls ? 'Yes' : 'No' }}</b-td>
                            </b-tr>
                            <b-tr>
                              <b-th>Selected Bucket</b-th>
                              <b-td>{{ pak.selectedBucketName }}</b-td>
                            </b-tr>
                            <b-tr>
                              <b-th>Resolved Scope</b-th>
                              <b-td>{{ pak.resolvedScopeName }}</b-td>
                            </b-tr>
                            <b-tr>
                              <b-th>Resolved Collection</b-th>
                              <b-td>{{ pak.resolvedCollectionName }}</b-td>
                            </b-tr>
                          </b-tbody>
                        </b-table-simple>

                        <div>
                          <u><b>Key</b></u>
                        </div>
                        <div>
                          <DataDump :base64="pak.keyBase64" />
                        </div>

                        <div>
                          <u><b>Value</b></u>
                        </div>
                        <div>
                          <DataDump :base64="pak.valueBase64" />
                        </div>

                        <div>
                          <u><b>Extras</b></u>
                        </div>
                        <div>
                          <DataDump :base64="pak.extrasBase64" />
                        </div>

                        <div>
                          <u><b>Ext. Frames</b></u>
                        </div>
                        <div v-if="!!pak.extFrames">
                          {{ pak.extFrames }}
                        </div>
                        <div v-else>None</div>
                      </div>
                    </div>
                  </b-td>
                </b-tr>
              </template>
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
.packets-card {
  height: 50rem;
  overflow-y: scroll;
}
.packet-in {
  cursor: pointer;
  background-color: rgba(0, 0, 255, 0.07);
}
.packet-out {
  cursor: pointer;
  background-color: rgba(0, 255, 0, 0.07);
}
.packet-hide-container {
  overflow: hidden;
  max-height: 0;
  transition: all 0.1s ease-in-out;
}
.packet-hide-container.active {
  max-height: 1000px;
}
.packet-desc th {
  width: 14rem;
}
.packet-card {
  margin: 0 0.5rem 0.5rem 0.5rem;
  padding: 0.5rem;
  border: 1px solid #dee2e6;
  border-top: none;
  border-bottom-left-radius: 0.25rem;
  border-bottom-right-radius: 0.25rem;
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
  methods: {
    togglePacketDetails(pakIdx) {
      const pakDetails = this.$refs['pak-details'][pakIdx]
      if (pakDetails) {
        pakDetails.classList.toggle('active')
      }
    },
    formatHexCas(cas) {
      if (!cas || cas === '0') {
        return '0'
      }

      while (cas.length < 16) {
        cas = '0' + cas
      }
      return '0x' + cas.toUpperCase()
    },
  },
}
</script>
