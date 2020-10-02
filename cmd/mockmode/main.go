package mockmode

import (
	"encoding/json"
	"log"

	"github.com/couchbaselabs/gocaves/mock/mockimpl"
)

type stdinData struct {
	ConnStr string `json:"connstr"`
}

// Main wraps the linkmode cmd
type Main struct {
}

// Go starts the app
func (m *Main) Go() {
	// When running in mock-only mode, we simply start-up, write the output
	// and then we wait indefinitely until someone kills us.
	cluster, err := mockimpl.NewDefaultCluster()
	if err != nil {
		log.Printf("Failed to start mock cluster: %s", err)
		return
	}

	logData := stdinData{
		ConnStr: cluster.ConnectionString(),
	}
	logBytes, _ := json.Marshal(logData)
	log.Writer().Write(logBytes)
	log.Writer().Write([]byte("\n"))

	// Let's wait forever
	<-make(chan struct{})
}
