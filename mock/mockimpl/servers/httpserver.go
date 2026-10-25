package servers

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"

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

	mochiwebTargets bool
}

// NewHTTPServiceOptions enables the specification of default options for a new http server.
type NewHTTPServiceOptions struct {
	Name      string
	Handlers  HTTPServerHandlers
	TLSConfig *tls.Config

	// MochiwebTargets keeps a "%" that starts no escape in a request
	// target, as ns_server's mochiweb does, instead of refusing the
	// request.
	MochiwebTargets bool
}

// NewHTTPServer instantiates a new instance of the memd server.
func NewHTTPServer(opts NewHTTPServiceOptions) (*HTTPServer, error) {
	svc := &HTTPServer{
		name:            opts.Name,
		handlers:        opts.Handlers,
		tlsConfig:       opts.TLSConfig,
		mochiwebTargets: opts.MochiwebTargets,
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
	if s.tlsConfig != nil && !s.mochiwebTargets {
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
	if s.mochiwebTargets {
		lsnr = mochiwebTargetListener{Listener: lsnr, tlsConfig: s.tlsConfig}
	}
	s.listener = lsnr

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w, answered := prepareMochiwebRequest(w, req)
			if answered {
				return
			}
			s.handleHTTP(w, req)
		}),
		ConnContext: mochiwebTargetConnContext,
	}
	if s.mochiwebTargets {
		readMochiwebHeads(srv)
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
	// On ns_server's ports a handler reads the body when it needs it, as
	// mochiweb_request:recv_body does, and gets the form from it. Other
	// services read a form body whole, within net/http's limit.
	contentType := req.Header.Get("Content-Type")
	var body io.Reader = req.Body
	form := url.Values{}
	switch {
	case s.mochiwebTargets:
		body = &mochiwebBody{src: req.Body, contentType: contentType, form: form}
		// ns_server answers a request whose handler exits as an unexpected
		// error.
		defer func() {
			if r := recover(); r != nil {
				if r != errBodyTooLarge {
					panic(r)
				}
				w.Header().Set("Connection", "close")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(500)
				_, _ = io.WriteString(w, `["Unexpected server error, request logged."]`)
			}
		}()
	case strings.HasPrefix(contentType, "application/x-www-form-urlencoded"):
		data, err := io.ReadAll(io.LimitReader(req.Body, maxFormBody+1))
		if err != nil || len(data) > maxFormBody {
			w.WriteHeader(400)
			return
		}
		body = bytes.NewReader(data)
		form = postForm(contentType, data)
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		w.WriteHeader(500)
		return
	}

	resp := s.handlers.NewRequestHandler(&mock.HTTPRequest{
		IsTLS:   s.tlsConfig != nil,
		Method:  req.Method,
		Target:  req.RequestURI,
		URL:     req.URL,
		Header:  req.Header,
		Body:    body,
		Form:    form,
		Context: req.Context(),
		Flusher: flusher,
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
	_, err := io.Copy(w, resp.Body)
	if err != nil {
		log.Printf("failed to write http response: %s", err)
	}
}

// maxMochiwebBody is mochiweb_request's MAX_RECV_BODY, and maxFormBody the
// form body net/http's ParseForm reads.
const (
	maxMochiwebBody = 20 << 20
	maxFormBody     = 10 << 20
)

// errBodyTooLarge ends a handler that reads a body over maxMochiwebBody,
// as mochiweb_request:recv_body exits the process serving the request.
var errBodyTooLarge = errors.New("body too large")

// mochiwebBody is the body of a request to an ns_server port. The first
// read takes it whole, as recv_body does, and parses the form of the
// request from it; a body over maxMochiwebBody ends the handler.
type mochiwebBody struct {
	src         io.Reader
	contentType string
	form        url.Values
	r           io.Reader
}

func (b *mochiwebBody) Read(p []byte) (int, error) {
	if b.r == nil {
		data, err := io.ReadAll(io.LimitReader(b.src, maxMochiwebBody+1))
		if err != nil {
			return 0, err
		}
		if len(data) > maxMochiwebBody {
			panic(errBodyTooLarge)
		}
		for key, values := range postForm(b.contentType, data) {
			b.form[key] = values
		}
		b.r = bytes.NewReader(data)
	}
	return b.r.Read(p)
}

// postForm returns the parameters of a form body as
// mochiweb_request:parse_post does, which leaves the query out.
func postForm(contentType string, body []byte) url.Values {
	form := url.Values{}
	for _, param := range mock.ParsePost(contentType, body) {
		form.Add(param.Key, param.Value)
	}
	return form
}
