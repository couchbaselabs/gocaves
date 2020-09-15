package mock

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
)

// HTTPService is a generic implementation of an HTTP service used by
// the various other HTTP services in this mock.
type HTTPService struct {
	serviceName string
	listenPort  int
	localAddr   string
	listener    net.Listener
	server      *http.Server
}

// ServiceName returns the name of this service
func (h *HTTPService) ServiceName() string {
	if h.serviceName == "" {
		return "Unknown Service"
	}
	return h.serviceName
}

// Start will start this HTTP service
func (h *HTTPService) Start() error {
	// Generate a listen address, listenPort defaults to 0, which means by default
	// we will be using a random port, future attempts to start this same server
	// should however reuse the same port that we originally had used.
	listenAddr := fmt.Sprintf(":%d", h.listenPort)

	lsnr, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Printf("failed to start listening for http `%s` service: %s", h.ServiceName(), err)
		return err
	}

	addr := lsnr.Addr()
	tcpAddr := addr.(*net.TCPAddr)
	h.listenPort = tcpAddr.Port
	h.localAddr = addr.String()
	h.listener = lsnr

	srv := &http.Server{
		Handler: http.HandlerFunc(h.handleHTTP),
	}
	h.server = srv

	log.Printf("starting listener for http `%s` service on port %d", h.ServiceName(), h.listenPort)
	go func() {
		err := srv.Serve(h.listener)
		if err != nil {
			log.Printf("listener for http `%s` failed to serve: %s", h.ServiceName(), err)
		}
	}()

	return nil
}

// Stop will stop this HTTP service
func (h *HTTPService) Stop() error {
	if h.server == nil {
		log.Printf("attempted to stop a stopped http `%s` service", h.ServiceName())
		return errors.New("cannot stop a stopped service")
	}

	err := h.server.Close()
	if err != nil {
		log.Printf("failed to stop listening for http `%s` service: %s", h.ServiceName(), err)
		return err
	}

	h.server = nil

	return nil
}

func (h *HTTPService) handleHTTP(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte("lol!"))
}
