package cmd

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/couchbaselabs/gocaves/api"
	"github.com/couchbaselabs/gocaves/mockimpl"
	//checksuite "github.com/couchbaselabs/gocaves/checksuite"
)

/*
	--sdk
	Runs it as if its inside an SDK.  This doesn't start the web browser system.

	--mock-only
	Runs it for specific purposes of mocking.  Does not start the web browser
	system, and does not run the testing system.

*/

type stdinData struct {
	ConnStr string `json:"connstr"`
}

var controlPortFlag = flag.Int("control-port", 0, "specifies we are running in a test-framework")
var linkAddrFlag = flag.String("link-addr", "", "specifies a caves dev server to connect to")
var mockOnlyFlag = flag.Bool("mock-only", false, "specifies only to use the mock")

var globalCluster *mockimpl.Cluster

func handleAPIRequest(pkt interface{}) interface{} {
	switch pkt.(type) {
	case *api.CmdGetConnStr:
		return &api.CmdConnStr{
			ConnStr: globalCluster.ConnectionString(),
		}
	}

	return nil
}

func createDefaultCluster() *mockimpl.Cluster {
	cluster, err := mockimpl.NewCluster(mockimpl.NewClusterOptions{
		InitialNode: mockimpl.NewNodeOptions{},
	})
	if err != nil {
		panic(err)
	}

	//cluster.AddNode(mockimpl.NewNodeOptions{})
	//cluster.AddNode(mockimpl.NewNodeOptions{})

	_, err = cluster.AddBucket(mockimpl.NewBucketOptions{
		Name:        "default",
		Type:        mockimpl.BucketTypeCouchbase,
		NumReplicas: 1,
	})
	if err != nil {
		log.Printf("Failed to create bucket: %+v", err)
	}

	return cluster
}

func startMockMode() {
	// When running in mock-only mode, we simply start-up, write the output
	// and then we wait indefinitely until someone kills us.

	logData := stdinData{
		ConnStr: globalCluster.ConnectionString(),
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

	go func() {
		io.Copy(srvConn, cliConn)
		cliConn.Close()
		srvConn.Close()
	}()

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
	apiSrv, err := api.NewServer(api.NewServerOptions{
		ListenPort: 9649,
		Handler:    handleAPIRequest,
	})
	if err != nil {
		log.Printf("failed to start api server: %s", err)
		return
	}

	log.Printf("started api server: %+v", apiSrv)

	// Let's wait forever
	<-make(chan struct{})
}

// Start will start the CAVES system.
func Start() {
	flag.Parse()

	log.SetPrefix("GOCAVES ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.SetOutput(os.Stderr)

	defaultCluster := createDefaultCluster()
	log.Printf("get default cluster: %+v", defaultCluster)
	globalCluster = defaultCluster

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
