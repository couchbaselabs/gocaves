'use strict'

import Vue from 'vue'
import dayjs from 'dayjs'

function vfDate(val, fmt) {
  if (!val) {
    return 'unknown'
  }

  if (typeof val === 'number') {
    val = new Date(val)
  }

  return dayjs(val).format(fmt)
}
Vue.filter('date', vfDate)
Vue.filter('atob', window.atob)
