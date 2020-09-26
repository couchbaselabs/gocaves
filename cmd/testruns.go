package cmd

import (
	"time"

	"github.com/couchbaselabs/gocaves/checks"
)

type testRun struct {
	RunID      string
	StartTime  time.Time
	ClientName string
	RunGroup   *checks.TestRunner
}

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

type jsonTest struct {
	Name        string   `json:"name"`
	Description string   `json:"desc"`
	Skipped     bool     `json:"skipped"`
	Success     bool     `json:"success"`
	Logs        []string `json:"logs"`
}

type jsonRunReport struct {
	MinVersion int        `json:"minversion"`
	Version    int        `json:"version"`
	ID         string     `json:"id"`
	When       string     `json:"when"`
	ClientName string     `json:"client"`
	Tests      []jsonTest `json:"tests"`
}

func (m *testRun) GenerateReport() jsonRunReport {
	var jrun jsonRunReport
	jrun.MinVersion = 1
	jrun.Version = 1
	jrun.ID = m.RunID
	jrun.When = m.StartTime.Format(time.RFC3339)
	jrun.ClientName = m.ClientName

	results := m.RunGroup.Results()

	for _, result := range results {
		var jtest jsonTest
		jtest.Name = result.Name
		jtest.Description = result.Description
		jtest.Skipped = result.Skipped
		jtest.Success = result.Success
		jtest.Logs = result.Logs

		jrun.Tests = append(jrun.Tests, jtest)
	}

	return jrun
}

var testRuns testRunManager
