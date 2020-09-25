package cmd

import "github.com/couchbaselabs/gocaves/checks"

type testRun struct {
	RunID      string
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

type jsonResult struct {
	Name        string   `json:"name"`
	Description string   `json:"desc"`
	Skipped     bool     `json:"skipped"`
	Success     bool     `json:"success"`
	Logs        []string `json:"logs"`
}

type jsonRun struct {
	RunID      string       `json:"run"`
	ClientName string       `json:"client"`
	Results    []jsonResult `json:"results"`
}

type jsonReport struct {
	Runs []jsonRun
}

func (m *testRunManager) GenerateReport() jsonReport {
	var report jsonReport

	for _, run := range m.Runs {
		var jrun jsonRun
		jrun.RunID = run.RunID
		jrun.ClientName = run.ClientName

		for _, result := range run.RunGroup.Results() {
			var jresult jsonResult
			jresult.Name = result.Name
			jresult.Description = result.Description
			jresult.Skipped = result.Success
			jresult.Success = result.Success
			jresult.Logs = result.Logs

			jrun.Results = append(jrun.Results, jresult)
		}

		report.Runs = append(report.Runs, jrun)
	}

	return report
}

var testRuns testRunManager
