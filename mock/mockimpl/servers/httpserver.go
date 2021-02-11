package servers

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"

	"github.com/couchbaselabs/gocaves/mock"
)

// HTTPServerHandlers provides all the handlers for the http server
type HTTPServerHandlers struct {
	NewRequestHandler func(*mock.HTTPRequest) *mock.HTTPResponse
}

// HTTPServer is a generic implementation of an HTTP server used by
// the various HTTP servers in this mock.
type HTTPServer struct {
	name       string
	listenPort int
	localAddr  string
	listener   net.Listener
	handlers   HTTPServerHandlers
	server     *http.Server
	tlsConfig  *tls.Config
}

// NewHTTPServiceOptions enables the specification of default options for a new http server.
type NewHTTPServiceOptions struct {
	Name      string
	Handlers  HTTPServerHandlers
	TLSConfig *tls.Config
}

// NewHTTPServer instantiates a new instance of the memd server.
func NewHTTPServer(opts NewHTTPServiceOptions) (*HTTPServer, error) {
	svc := &HTTPServer{
		name:      opts.Name,
		handlers:  opts.Handlers,
		tlsConfig: opts.TLSConfig,
	}

	err := svc.start()
	if err != nil {
		return nil, err
	}

	return svc, nil
}

// serviceName returns the name of this service
func (s *HTTPServer) serviceName() string {
	if s.name == "" {
		return "Unknown Service"
	}
	return s.name
}

// ListenPort returns the port this server is listening on.
func (s *HTTPServer) ListenPort() int {
	return s.listenPort
}

// Start will start this HTTP server
func (s *HTTPServer) start() error {
	listenAddr := fmt.Sprintf(":%d", s.listenPort)

	var lsnr net.Listener
	var err error
	if s.tlsConfig != nil {
		lsnr, err = tls.Listen("tcp", listenAddr, s.tlsConfig)
	} else {
		lsnr, err = net.Listen("tcp", listenAddr)
	}
	if err != nil {
		if s.tlsConfig != nil {
			log.Printf("failed to start listening for http `%s` TLS server: %s", s.serviceName(), err)
		} else {
			log.Printf("failed to start listening for http `%s` server: %s", s.serviceName(), err)
		}
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

	log.Printf("starting listener for %s (http) server on port %d", s.serviceName(), s.listenPort)
	go func() {
		err := srv.Serve(s.listener)
		if err != nil {
			log.Printf("listener for http `%s` failed to serve: %s", s.serviceName(), err)
		}
	}()

	return nil
}

// Close will stop this HTTP server
func (s *HTTPServer) Close() error {
	if s.server == nil {
		log.Printf("attempted to stop a stopped http `%s` server", s.serviceName())
		return errors.New("cannot stop a stopped server")
	}

	err := s.server.Close()
	if err != nil {
		log.Printf("failed to stop listening for http `%s` server: %s", s.serviceName(), err)
		return err
	}

	s.server = nil

	return nil
}

func (s *HTTPServer) handleHTTP(w http.ResponseWriter, req *http.Request) {
	if err := req.ParseForm(); err != nil {
		// If the content type isn't form then ParseForm will not error, to get here something
		// is wrong with the request.
		w.WriteHeader(400)
		return
	}
	resp := s.handlers.NewRequestHandler(&mock.HTTPRequest{
		IsTLS:  s.tlsConfig != nil,
		Method: req.Method,
		URL:    req.URL,
		Header: req.Header,
		Body:   req.Body,
		Form:   req.Form,
	})
	if resp == nil {
		// If nobody decides to answer the request, we write 501 Unsupported.
		w.WriteHeader(501)
		return
	}

	for headerName, headerValues := range resp.Header {
		for _, headerValue := range headerValues {
			w.Header().Add(headerName, headerValue)
		}
	}

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}
