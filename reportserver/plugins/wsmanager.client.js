/* global $nuxt */
'use strict'

function tryParseDate(str) {
  try {
    return new Date(str)
  } catch (e) {
    return null
  }
}

function findGroupsByPath(root, pathParts) {
  if (!pathParts || pathParts.length === 0) {
    return [root]
  }

  const newPathParts = [...pathParts]
  const rootPath = root.name ? root.name + '/' : ''
  const groupName = rootPath + newPathParts.shift()

  let existingGroup = root.groups.find((group) => group.name === groupName)
  if (!existingGroup) {
    const newGroup = {
      status: 'unknown',
      statusReason: '',
      name: groupName,
      tests: [],
      groups: [],
      numTests: 0,
      numAttempted: 0,
      numSuccess: 0,
      numFailed: 0,
      numSkipped: 0,
    }
    root.groups.push(newGroup)

    existingGroup = newGroup
  }

  const groupChain = findGroupsByPath(existingGroup, newPathParts)
  groupChain.push(root)
  return groupChain
}

function recursiveUpdateStatus(testGroup) {
  if (testGroup.numFailed > 0) {
    testGroup.status = 'failed'
    testGroup.statusReason = `At least one test failed.`
  } else if (testGroup.numSkipped === testGroup.numTests) {
    testGroup.status = 'skipped'
    testGroup.statusReason = `All tests were skipped`
  } else if (testGroup.numSkipped > 0) {
    testGroup.status = 'warning'
    testGroup.statusReason = `All tests which ran passed, but some tests were skipped`
  } else {
    testGroup.status = 'success'
    testGroup.statusReason = `All tests were run and passed.`
  }

  testGroup.groups.forEach((subGroup) => {
    recursiveUpdateStatus(subGroup)
  })
}

function testsToTestGroup(tests) {
  const rootTestGroup = {
    status: 'unknown',
    statusReason: '',
    tests: [],
    groups: [],
    numTests: 0,
    numAttempted: 0,
    numSuccess: 0,
    numFailed: 0,
    numSkipped: 0,
  }

  tests.forEach((test) => {
    const nameParts = test.name.split('/')
    const groupNameParts = nameParts.slice(0, nameParts.length - 1)
    const testGroups = findGroupsByPath(rootTestGroup, groupNameParts)

    testGroups.forEach((testGroup) => {
      if (test.status === 'success') {
        testGroup.numSuccess++
        testGroup.numAttempted++
      } else if (test.status === 'failed') {
        testGroup.numFailed++
        testGroup.numAttempted++
      } else if (test.status === 'skipped') {
        testGroup.numSkipped++
      }

      testGroup.numTests++
    })

    testGroups[0].tests.push(test)
  })

  recursiveUpdateStatus(rootTestGroup)

  return rootTestGroup
}

export class WsManager {
  constructor(store, app) {
    this.store = store
    this.app = app

    this.ws = null

    this.connectWs()
  }

  parseReportTest(testData) {
    return {
      ...testData,
    }
  }

  parseReport(reportData) {
    if (reportData.minversion > 1) {
      throw new Error('cannot parse report, unknown minversion')
    }

    const tests = reportData.tests.map((test) => this.parseReportTest(test))

    // Sort tests in ascending order
    tests.sort((a, b) => a.name.localeCompare(b))

    // Group the tests
    const rootTestGroup = testsToTestGroup(tests)

    return {
      ...reportData,
      createdAt: tryParseDate(reportData.createdAt),
      tests: rootTestGroup,
    }
  }

  _handleMessage(msg) {
    if (msg.type === 'new_report') {
      this.store.commit('addReport', this.parseReport(msg.report))
    }
  }

  connectWs() {
    if (!WebSocket) {
      return
    }

    if (this.ws) {
      this.ws.close()
      this.ws = null
    }

    const loc = window.location
    const wsProto = loc.protocol === 'https' ? 'wss' : 'ws'
    const wsApiBase = `${wsProto}://${loc.host}`

    const ws = new WebSocket(`${wsApiBase}/api/stream`)

    ws.addEventListener('message', (event) => {
      const msg = JSON.parse(event.data)
      console.debug('received new message:', msg)

      try {
        this._handleMessage(msg)
      } catch (e) {
        console.error('failed to handle message: ', e)
      }
    })

    ws.addEventListener('close', () => {
      this.ws = null

      setTimeout(() => {
        this.connectWs()
      }, 1000)
    })

    this.ws = ws
  }
}
