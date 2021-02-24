export default {
  ssr: false,
  server: {
    port: 9659,
  },

  head: {
    title: 'CAVES Reporting',
    meta: [
      { charset: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      { hid: 'description', name: 'description', content: '' },
    ],
    link: [{ rel: 'icon', type: 'image/x-icon', href: '/favicon.ico' }],
  },

  components: true,

  buildModules: ['@nuxtjs/fontawesome'],

  modules: [
    'bootstrap-vue/nuxt',
    'nuxt-client-init-module',
    '~/modules/api.js',
  ],

  plugins: ['~/plugins/filters.js'],

  fontawesome: {
    component: 'Fa',
    icons: {
      solid: [
        'faCheckCircle',
        'faQuestionCircle',
        'faTimesCircle',
        'faExclamationCircle',
        'faCaretDown',
        'faCaretRight',
        'faChevronLeft',
        'faChevronRight',
      ],
    },
  },
}
