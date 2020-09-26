package cmd

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/couchbaselabs/gocaves/api"
	"github.com/couchbaselabs/gocaves/checksuite"
	"github.com/couchbaselabs/gocaves/reporting"
	//checksuite "github.com/couchbaselabs/gocaves/checksuite"
)

type stdinData struct {
	ConnStr string `json:"connstr"`
}

var controlPortFlag = flag.Int("control-port", 0, "specifies we are running in a test-framework")
var linkAddrFlag = flag.String("link-addr", "", "specifies a caves dev server to connect to")
var reportingAddrFlag = flag.String("reporting-addr", "", "specifies a caves reporting server to use")
var mockOnlyFlag = flag.Bool("mock-only", false, "specifies only to use the mock")

func handleAPIRequest(pkt interface{}) interface{} {
	switch pktTyped := pkt.(type) {
	case *api.CmdGetConnStr:
		cluster, err := globalCluster.Get()
		if err != nil {
			log.Printf("failed to get global cluster: %s", err)
			return &api.CmdConnStr{}
		}
		return &api.CmdConnStr{
			ConnStr: cluster.ConnectionString(),
		}
	case *api.CmdStartTesting:
		run, err := testRuns.NewRun(pktTyped.RunID, pktTyped.ClientName)
		if run == nil {
			log.Printf("failed to start testing: %s", err)
			return &api.CmdStartedTesting{}
		}

		return &api.CmdStartedTesting{
			ConnStr: run.RunGroup.DefaultCluster().ConnectionString(),
		}

	case *api.CmdEndTesting:
		run := testRuns.Get(pktTyped.RunID)
		if run == nil {
			log.Printf("failed to end testing, bad run id: %s", pktTyped.RunID)
			return &api.CmdEndedTesting{}
		}

		run.RunGroup.End()

		report := run.GenerateReport()

		log.Printf("ended test run; full report:\n%+v", report)

		if reportingAddrFlag != nil {
			reportingURI := fmt.Sprintf("http://%s/submit", *reportingAddrFlag)
			jsonReport, _ := json.Marshal(report)
			_, err := http.Post(reportingURI, "text/javascript", bytes.NewReader(jsonReport))
			if err != nil {
				log.Printf("failed to send report to `%s`: %s", reportingURI, err)
			}
		}

		return &api.CmdEndedTesting{
			Report: report,
		}

	case *api.CmdStartTest:
		run := testRuns.Get(pktTyped.RunID)
		if run == nil {
			log.Printf("failed to start test, bad run id: %s", pktTyped.RunID)
			return &api.CmdStartedTest{}
		}

		spec, err := run.RunGroup.StartTest(pktTyped.TestName)
		if err != nil {
			log.Printf("failed to start test: %s", err)
			return &api.CmdStartedTest{}
		}

		return &api.CmdStartedTest{
			ConnStr:        spec.Cluster.ConnectionString(),
			BucketName:     spec.BucketName,
			ScopeName:      spec.ScopeName,
			CollectionName: spec.CollectionName,
		}
	case *api.CmdEndTest:
		run := testRuns.Get(pktTyped.RunID)
		if run == nil {
			log.Printf("failed to end test, bad run id: %s", pktTyped.RunID)
			return &api.CmdStartedTest{}
		}

		run.RunGroup.EndRunningTest(pktTyped.Result)
		return &api.CmdEndedTest{}
	}

	return nil
}

func startMockMode() {
	// When running in mock-only mode, we simply start-up, write the output
	// and then we wait indefinitely until someone kills us.

	cluster, err := globalCluster.Get()
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

func startSDKLinkedMode() {
	sdkPort := *controlPortFlag
	linkAddr := *linkAddrFlag

	srvConn, err := net.Dial("tcp", linkAddr)
	if err != nil {
		log.Printf("failed to connect to caves server: %s", err)
		return
	}

	cliConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", sdkPort))
	if err != nil {
		log.Printf("failed to connect to the sdk: %s", err)
		return
	}

	// TODO(brett19): Should probably use the API package to do this...
	cliConn.Write([]byte(`{"type":"hello"}`))

	go func() {
		log.Printf("Starting server to client copying")
		io.Copy(srvConn, cliConn)
		cliConn.Close()
		srvConn.Close()
	}()

	log.Printf("Starting client to server copying")
	io.Copy(cliConn, srvConn)
	cliConn.Close()
	srvConn.Close()
}

func startSDKMode() {
	sdkPort := *controlPortFlag

	cli, err := api.ConnectAsServer(api.ConnectAsServerOptions{
		Address: fmt.Sprintf("127.0.0.1:%d", sdkPort),
		Handler: handleAPIRequest,
	})
	if err != nil {
		log.Printf("failed to connect to the sdk: %s", err)
		return
	}

	log.Printf("connected to the sdk")

	cli.WaitForClose()

	log.Printf("sdk disconnected")
}

func startDevMode() {
	srv, err := reporting.NewServer(reporting.NewServerOptions{
		ListenPort: 9659,
	})
	if err != nil {
		log.Printf("failed to start reporting server: %s", err)
	}

	log.Printf("reporting server started: %+v", srv)
	log.Printf("Access the Web UI here: %s", "http://localhost:9659/")

	<-make(chan struct{})
}

// Start will start the CAVES system.
func Start() {
	flag.Parse()

	log.SetPrefix("GOCAVES ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetOutput(os.Stderr)

	checksuite.RegisterCheckFuncs()

	/*
		defaultCluster, err := mockimpl.NewDefaultCluster()
		if err != nil {
			log.Printf("failed to start default cluster")
		}

		log.Printf("got default cluster: %+v", defaultCluster)
		globalCluster = defaultCluster
	*/

	if mockOnlyFlag != nil && *mockOnlyFlag {
		// Mock-only mode
		startMockMode()
	} else if controlPortFlag != nil && *controlPortFlag > 0 {
		// Test-suite mode inside an SDK
		if linkAddrFlag != nil && *linkAddrFlag != "" {
			// Test-suite inside an SDK linked to a dev mod instance
			startSDKLinkedMode()
		} else {
			// Standard test-suite mode
			startSDKMode()
		}
	} else {
		// Development mode
		startDevMode()
	}
}
