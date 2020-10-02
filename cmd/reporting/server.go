package reporting

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/couchbaselabs/gocaves/cmd/reporting/webapp"

	"github.com/gorilla/websocket"
)

// Server represents a running reporting server.
type Server struct {
	srv     *http.Server
	reports []json.RawMessage
	clients []*websocket.Conn
}

// NewServerOptions specifies options for instantiating a reporting server.
type NewServerOptions struct {
	ListenPort int
}

// NewServer instantiates a new reporting server.
func NewServer(opts NewServerOptions) (*Server, error) {
	if opts.ListenPort == 0 {
		opts.ListenPort = 9659
	}

	mux := http.NewServeMux()

	httpSrv := &http.Server{
		Addr:           fmt.Sprintf(":%d", opts.ListenPort),
		Handler:        mux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	srv := &Server{
		srv: httpSrv,
	}

	mux.Handle("/", http.FileServer(webapp.AssetFile()))
	//mux.Handle("/", http.FileServer(http.Dir("./reporting/webapp")))
	mux.HandleFunc("/stream", srv.handleStream)
	mux.HandleFunc("/submit", srv.handleReportSubmission)

	go func() {
		err := httpSrv.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Printf("reporting server listen failure: %s", err)
		}
	}()

	return srv, nil
}

func (s *Server) addClient(c *websocket.Conn) {
	s.clients = append(s.clients, c)
}

func (s *Server) removeClient(c *websocket.Conn) {
	newClients := make([]*websocket.Conn, 0)
	for _, client := range s.clients {
		if client != c {
			newClients = append(newClients, client)
		}
	}
	s.clients = newClients
}

var upgrader = websocket.Upgrader{}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("stream upgrade error:", err)
		return
	}
	s.addClient(c)
	defer func() {
		s.removeClient(c)
		c.Close()
	}()

	log.Printf("stream client connected")

	for _, report := range s.reports {
		msgOut := make(map[string]interface{})
		msgOut["type"] = "newreport"
		msgOut["report"] = report
		msgBytes, _ := json.Marshal(msgOut)
		c.WriteMessage(websocket.TextMessage, msgBytes)
	}

	for {
		_, _, err := c.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, 1001) {
				log.Println("read error:", err)
			}
			break
		}
	}
}

func (s *Server) handleReportSubmission(w http.ResponseWriter, req *http.Request) {
	reportBytes, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.Write([]byte(`{"error":"bad input"}`))
		return
	}

	log.Printf("received new report: %s", reportBytes)

	s.reports = append(s.reports, reportBytes)

	msgOut := make(map[string]interface{})
	msgOut["type"] = "newreport"
	msgOut["report"] = json.RawMessage(reportBytes)
	msgBytes, _ := json.Marshal(msgOut)

	for _, client := range s.clients {
		client.WriteMessage(websocket.TextMessage, msgBytes)
	}

	w.Write([]byte(`{"success":true}`))
}

// Close will shut down the running reporting server.
func (s *Server) Close() error {
	return s.srv.Close()
}
