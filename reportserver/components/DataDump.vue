<template>
  <div class="d-flex flex-row">
    <div>
      <div><u>Hex</u></div>
      <pre>{{ hex }}</pre>
    </div>
    <div class="ml-3">
      <div><u>ASCII</u></div>
      <pre>{{ ascii }}</pre>
    </div>
    <div class="ml-5 flex-grow-1">
      <div><u>Text</u></div>
      <pre class="raw-text">{{ text }}</pre>
    </div>
  </div>
</template>

<style scoped>
.raw-text {
  max-width: 30rem;
  white-space: pre-wrap;
}
</style>

<script>
export default {
  props: {
    base64: {
      type: String,
      required: true,
    },
  },
  computed: {
    hex() {
      const bytes = this.bytes
      let output = ''
      for (var i = 0; ; ) {
        for (var j = 0; j < 4; ++j) {
          for (var k = 0; k < 4; ++k, ++i) {
            if (i >= bytes.length) {
              output += '   '
            } else {
              const hexByte = ('0' + (bytes.charCodeAt(i) & 0xff).toString(16)).slice(-2)
              output += hexByte.toUpperCase() + ' '
            }
          }
          output += '  '
        }
        output += '\n'

        if (i >= bytes.length) {
          break
        }
      }
      return output
    },
    ascii() {
      const bytes = this.bytes
      let output = ''
      for (var i = 0; ; ) {
        for (var j = 0; j < 4; ++j) {
          for (var k = 0; k < 4; ++k, ++i) {
            if (i >= bytes.length) {
              output += '  '
            } else {
              if ((bytes.charCodeAt(i) < 32) | (bytes.charCodeAt(i) > 126)) {
                output += '  '
              } else {
                output += bytes[i] + ' '
              }
            }
          }
        }
        output += '\n'

        if (i >= bytes.length) {
          break
        }
      }
      return output
    },
    text() {
      const bytes = this.bytes
      let output = ''
      for (var i = 0; i < bytes.length; ++i) {
        if ((bytes.charCodeAt(i) < 32) | (bytes.charCodeAt(i) > 126)) {
          // skip this non-displayable character
        } else {
          output += bytes[i]
        }
      }
      return output
    },
    bytes() {
      return atob(this.base64)
    },
  },
}
</script>
