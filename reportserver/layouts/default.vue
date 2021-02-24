<template>
  <div>
    <b-navbar toggleable="lg" type="dark" variant="info">
      <b-navbar-brand to="/">CAVES Dashboard</b-navbar-brand>
    </b-navbar>

    <Nuxt />
  </div>
</template>

<style>
html {
  font-size: 0.8rem;
}
</style>

<script>
export default {
  watch: {
    '$route.hash'() {
      this.updatePageHash()
    },
  },
  mounted() {
    this.updatePageHash()
  },
  methods: {
    updatePageHash() {
      const hashOpts = this.$route.hash.substr(1).split(',')
      hashOpts.forEach((opt) => {
        switch (opt) {
          case 'debug':
            this.$store.commit('enableDebugMode')
            break
          case 'debug':
            this.$store.commit('disableDebugMode')
            break
        }
      })
    },
  },
}
</script>
