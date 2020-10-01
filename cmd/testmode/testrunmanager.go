package testmode

import (
	"errors"
	"time"

	"github.com/couchbaselabs/gocaves/checks"
)

type testRunManager struct {
	Runs []*testRun
}

func (m *testRunManager) NewRun(runID, clientName string) (*testRun, error) {
	runGroup, err := checks.NewTestRunner()
	if err != nil {
		return nil, err
	}

	run := &testRun{
		RunID:      runID,
		StartTime:  time.Now(),
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

func (m *testRunManager) TimeTravel(runID string, amount time.Duration) error {
	run := m.Get(runID)
	if run == nil {
		return errors.New("invalid run specified")
	}

	run.RunGroup.DefaultCluster().Chrono().TimeTravel(amount)

	return nil
}

func (m *testRunManager) StartTest(runID, testName string) (*checks.TestStartedSpec, error) {
	run := m.Get(runID)
	if run == nil {
		return nil, errors.New("invalid run specified")
	}

	return run.RunGroup.StartTest(testName)
}

func (m *testRunManager) EndCurrentTest(runID string, result interface{}) error {
	run := m.Get(runID)
	if run == nil {
		return errors.New("invalid run specified")
	}

	return run.RunGroup.EndRunningTest(result)
}

func (m *testRunManager) EndRun(runID string) (*jsonRunReport, error) {
	run := m.Get(runID)
	if run == nil {
		return nil, errors.New("invalid run specified")
	}

	run.RunGroup.End()
	report := run.GenerateReport()

	return &report, nil
}
