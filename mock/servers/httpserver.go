package servers

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
)

// HTTPRequest encapsulates an HTTP request.
type HTTPRequest struct {
	Method string
	URL    *url.URL
	Header http.Header
	Body   io.Reader
}

// HTTPResponse encapsulates an HTTP response.
type HTTPResponse struct {
	StatusCode int
	Header     http.Header
	Body       io.Reader
}

// HTTPServerHandlers provides all the handlers for the http server
type HTTPServerHandlers struct {
	NewRequestHandler func(*HTTPRequest) *HTTPResponse
}

// HTTPServer is a generic implementation of an HTTP server used by
// the various HTTP servers in this mock.
type HTTPServer struct {
	serviceName string
	listenPort  int
	localAddr   string
	listener    net.Listener
	handlers    HTTPServerHandlers
	server      *http.Server
}

// NewHTTPServiceOptions enables the specification of default options for a new http server.
type NewHTTPServiceOptions struct {
	Name     string
	Handlers HTTPServerHandlers
}

// NewHTTPServer instantiates a new instance of the memd server.
func NewHTTPServer(opts NewHTTPServiceOptions) (*HTTPServer, error) {
	svc := &HTTPServer{
		serviceName: opts.Name,
		handlers:    opts.Handlers,
	}

	err := svc.start()
	if err != nil {
		return nil, err
	}

	return svc, nil
}

// ServiceName returns the name of this service
func (s *HTTPServer) ServiceName() string {
	if s.serviceName == "" {
		return "Unknown Service"
	}
	return s.serviceName
}

// ListenPort returns the port this server is listening on.
func (s *HTTPServer) ListenPort() int {
	return s.listenPort
}

// Start will start this HTTP server
func (s *HTTPServer) start() error {
	// Generate a listen address, listenPort defaults to 0, which means by default
	// we will be using a random port, future attempts to start this same server
	// should however reuse the same port that we originally had used.
	listenAddr := fmt.Sprintf(":%d", s.listenPort)

	lsnr, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Printf("failed to start listening for http `%s` server: %s", s.ServiceName(), err)
		return err
	}

	addr := lsnr.Addr()
	tcpAddr := addr.(*net.TCPAddr)
	s.listenPort = tcpAddr.Port
	s.localAddr = addr.String()
	s.listener = lsnr

	srv := &http.Server{
		Handler: http.HandlerFunc(s.handleHTTP),
	}
	s.server = srv

	log.Printf("starting listener for http `%s` server on port %d", s.ServiceName(), s.listenPort)
	go func() {
		err := srv.Serve(s.listener)
		if err != nil {
			log.Printf("listener for http `%s` failed to serve: %s", s.ServiceName(), err)
		}
	}()

	return nil
}

// Stop will stop this HTTP server
func (s *HTTPServer) Stop() error {
	if s.server == nil {
		log.Printf("attempted to stop a stopped http `%s` server", s.ServiceName())
		return errors.New("cannot stop a stopped server")
	}

	err := s.server.Close()
	if err != nil {
		log.Printf("failed to stop listening for http `%s` server: %s", s.ServiceName(), err)
		return err
	}

	s.server = nil

	return nil
}

func (s *HTTPServer) handleHTTP(w http.ResponseWriter, req *http.Request) {
	resp := s.handlers.NewRequestHandler(&HTTPRequest{
		Method: req.Method,
		URL:    req.URL,
		Header: req.Header,
		Body:   req.Body,
	})

	for headerName, headerValues := range resp.Header {
		for _, headerValue := range headerValues {
			w.Header().Add(headerName, headerValue)
		}
	}

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
