package mockmode

import (
	"encoding/json"
	"log"

	"github.com/couchbaselabs/gocaves/mock/mockimpl"
)

type stdoutData struct {
	ConnStr string `json:"connstr"`
}

func writeStdoutData(data *stdoutData) error {
	logBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	logBytes = append(logBytes, []byte("\n")...)

	_, err = log.Writer().Write(logBytes)
	if err != nil {
		return err
	}

	return nil
}

// Main wraps the linkmode cmd
type Main struct {
}

// Go starts the app
func (m *Main) Go(port int) {
	// When running in mock-only mode, we simply start-up, write the output
	// and then we wait indefinitely until someone kills us.
	cluster, err := mockimpl.NewDefaultCluster(port)
	if err != nil {
		log.Printf("Failed to start mock cluster: %s", err)
		return
	}

	err = writeStdoutData(&stdoutData{
		ConnStr: cluster.ConnectionString(),
	})
	if err != nil {
		panic(err)
	}

	// Let's wait forever
	<-make(chan struct{})
}
