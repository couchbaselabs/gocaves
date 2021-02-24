package testmode

import (
	"fmt"
	"time"

	"github.com/couchbaselabs/gocaves/checks"
)

type testRun struct {
	RunID      string
	StartTime  time.Time
	ClientName string
	RunGroup   *checks.TestRunner
}

type jsonTest struct {
	Name        string   `json:"name"`
	Description string   `json:"desc"`
	Status      string   `json:"status"`
	Logs        []string `json:"logs"`
}

type jsonRunReport struct {
	MinVersion int        `json:"minversion"`
	Version    int        `json:"version"`
	ID         string     `json:"id"`
	CreatedAt  string     `json:"createdAt"`
	ClientName string     `json:"client"`
	Tests      []jsonTest `json:"tests"`
}

func testStatusToString(status checks.TestStatus) string {
	switch status {
	case checks.TestStatusSkipped:
		return "skipped"
	case checks.TestStatusFailed:
		return "failed"
	case checks.TestStatusSuccess:
		return "success"
	default:
		return fmt.Sprintf("unknown:%d", status)
	}
}

func (m *testRun) GenerateReport() jsonRunReport {
	var jrun jsonRunReport
	jrun.MinVersion = 1
	jrun.Version = 1
	jrun.ID = m.RunID
	jrun.CreatedAt = m.StartTime.Format(time.RFC3339)
	jrun.ClientName = m.ClientName

	results := m.RunGroup.Results()

	for _, result := range results {
		var jtest jsonTest
		jtest.Name = result.Name
		jtest.Description = result.Description
		jtest.Status = testStatusToString(result.Status)
		jtest.Logs = result.Logs

		jrun.Tests = append(jrun.Tests, jtest)
	}

	return jrun
}
