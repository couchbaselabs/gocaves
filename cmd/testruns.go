package cmd

import "github.com/couchbaselabs/gocaves/checks"

type testRun struct {
	RunID      string
	ClientName string
	RunGroup   *checks.TestRunGroup
}

type testRunManager struct {
	Runs []*testRun
}

func (m *testRunManager) NewRun(runID, clientName string) (*testRun, error) {
	runGroup, err := checks.NewTestRunGroup()
	if err != nil {
		return nil, err
	}

	run := &testRun{
		RunID:      runID,
		ClientName: clientName,
		RunGroup:   runGroup,
	}
	m.Runs = append(m.Runs, run)

	return run, nil
}

func (m *testRunManager) Get(runID string) *testRun {
	for _, run := range m.Runs {
		if run.RunID == runID {
			return run
		}
	}
	return nil
}

var testRuns testRunManager
