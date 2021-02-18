package reporting

import (
	"log"
)

// Main wraps the linkmode cmd
type Main struct {
}

// Go starts the app
func (m *Main) Go() {
	srv, err := NewServer(NewServerOptions{
		ListenPort: 9659,
	})
	if err != nil {
		log.Printf("failed to start reporting server: %s", err)
	}

	log.Printf("reporting server started: %+v", srv)
	log.Printf("Access the Web UI here: %s", "http://localhost:9659/")

	<-make(chan struct{})
}
