package client

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path"
	"time"
)

// Client represents a single CAVES client instance.
type Client struct {
	conn       net.Conn
	reader     *bufio.Reader
	shutdownCh chan struct{}
}

// NewClientOptions provides options for the NewClient method.
type NewClientOptions struct {
	Path          string
	Version       string
	CavesAddr     string
	ReportingAddr string
}

// NewClient instantiates a new CAVES instance and returns an interface to control it.
func NewClient(opts NewClientOptions) (*Client, error) {
	log.Printf("Starting CAVES")

	cavesLsnr, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Printf("failed to start listener for caves: %s", err)
		return nil, err
	}

	// Save the local listening address
	addr := cavesLsnr.Addr()
	tcpAddr := addr.(*net.TCPAddr)
	cavesListenPort := tcpAddr.Port

	var cavesProc *exec.Cmd
	if opts.Path == "" {
		// We should download the latest caves version.
		return nil, errors.New("not supported")
	} else if path.Ext(opts.Path) == ".go" {
		// If the path ends in .go, it means to use a local caves.
		cavesProc = exec.Command("go", "run", path.Base(opts.Path))
		cavesProc.Dir = path.Dir(opts.Path) + "/"
	} else {
		// Otherwise, assume its a direct path to a caves executable.
		cavesProc = exec.Command(opts.Path)
	}

	cavesProc.Args = append(cavesProc.Args, fmt.Sprintf("--control-port=%d", cavesListenPort))

	if opts.CavesAddr != "" {
		cavesProc.Args = append(cavesProc.Args, fmt.Sprintf("--link-addr=%s", opts.CavesAddr))
	}

	if opts.ReportingAddr != "" {
		cavesProc.Args = append(cavesProc.Args, fmt.Sprintf("--reporting-addr=%s", opts.ReportingAddr))
	}

	log.Printf("EXECUTING: %+v", cavesProc)

	shutdownCh := make(chan struct{}, 1)

	cavesProc.Stdout = os.Stdin
	cavesProc.Stderr = os.Stderr
	go func() {
		err := cavesProc.Run()
		if err != nil {
			panic(err)
		}

		close(shutdownCh)
	}()

	log.Printf("Started CAVES")

	conn, err := cavesLsnr.Accept()
	if err != nil {
		log.Printf("failed to accept the caves process: %s", err)
		return nil, err
	}

	log.Printf("CAVES connected")

	cli := &Client{
		conn:       conn,
		shutdownCh: shutdownCh,
		reader:     bufio.NewReader(conn),
	}

	helloCmd, err := cli.readCommand()
	if err != nil {
		log.Printf("failed to receive caves hello: %s", err)
		return nil, err
	}

	if helloCmd["type"] != "hello" {
		log.Printf("first caves command was not hello: %+v", helloCmd)
		return nil, errors.New("no hello")
	}

	return cli, nil
}

// Shutdown will shutdown the CAVES client.
func (c *Client) Shutdown() error {
	err := c.conn.Close()
	<-c.shutdownCh
	return err
}

func (c *Client) writeCommand(req map[string]interface{}) error {
	reqBytes, err := json.Marshal(req)
	if err != nil {
		log.Printf("fail to encode request bytes: %s", err)
		return err
	}

	reqBytes = append(reqBytes, byte(0))

	_, err = c.conn.Write(reqBytes)
	if err != nil {
		log.Printf("fail to write request bytes: %s", err)
		return err
	}

	return nil
}

func (c *Client) readCommand() (map[string]interface{}, error) {
	respBytes, err := c.reader.ReadSlice(0)
	if err != nil {
		log.Printf("fail to read response bytes: %s", err)
		return nil, err
	}

	respBytes = respBytes[:len(respBytes)-1]

	var resp map[string]interface{}
	err = json.Unmarshal(respBytes, &resp)
	if err != nil {
		log.Printf("fail to parse response bytes: %s", err)
		return nil, err
	}

	return resp, nil
}

func (c *Client) roundTripCommand(req map[string]interface{}) (map[string]interface{}, error) {
	err := c.writeCommand(req)
	if err != nil {
		return nil, err
	}

	resp, err := c.readCommand()
	if err != nil {
		return nil, err
	}

	return resp, nil
}

type CreateClusterResult struct {
	ConnStr         string
	ManagementAddrs []string
}

// CreateCluster instantiates a new CAVES test cluster.
func (c *Client) CreateCluster(clusterID string) (*CreateClusterResult, error) {
	resp, err := c.roundTripCommand(map[string]interface{}{
		"type": "createcluster",
		"id":   clusterID,
	})
	if err != nil {
		return nil, err
	}

	mgmtInterfaces := resp["mgmt_addrs"].([]interface{})
	var mgmtAddrs []string
	for _, m := range mgmtInterfaces {
		mgmtAddrs = append(mgmtAddrs, m.(string))
	}
	return &CreateClusterResult{
		ConnStr:         resp["connstr"].(string),
		ManagementAddrs: mgmtAddrs,
	}, nil
}

// StartTesting begins a test suite run within CAVES.
func (c *Client) StartTesting(runID string, clientName string) (string, error) {
	resp, err := c.roundTripCommand(map[string]interface{}{
		"type":   "starttesting",
		"run":    runID,
		"client": clientName,
	})

	if err != nil {
		return "", err
	}

	connStr := resp["connstr"].(string)
	return connStr, nil
}

// EndTesting stops an already running CAVES test suite.
func (c *Client) EndTesting(runID string) (interface{}, error) {
	resp, err := c.roundTripCommand(map[string]interface{}{
		"type": "endtesting",
		"run":  runID,
	})
	if err != nil {
		return nil, err
	}

	report := resp["report"]
	return report, err
}

// TestStartedSpec provides information about a specific started test.
type TestStartedSpec struct {
	ConnStr        string
	BucketName     string
	ScopeName      string
	CollectionName string
}

// StartTest begins a specific test within a CAVES test suite.
func (c *Client) StartTest(runID, testName string) (*TestStartedSpec, error) {
	resp, err := c.roundTripCommand(map[string]interface{}{
		"type": "starttest",
		"run":  runID,
		"test": testName,
	})
	if err != nil {
		return nil, err
	}

	connStr := resp["connstr"].(string)
	bucketName := resp["bucket"].(string)
	scopeName := resp["scope"].(string)
	collectionName := resp["collection"].(string)
	return &TestStartedSpec{
		ConnStr:        connStr,
		BucketName:     bucketName,
		ScopeName:      scopeName,
		CollectionName: collectionName,
	}, nil
}

// EndTest stops a running CAVES test suite test.
func (c *Client) EndTest(runID string) error {
	_, err := c.roundTripCommand(map[string]interface{}{
		"type": "endtest",
		"run":  runID,
	})
	return err
}

// TimeTravelRun allows a specific run to be time-travelled.
func (c *Client) TimeTravelRun(runID string, duration time.Duration) error {
	_, err := c.roundTripCommand(map[string]interface{}{
		"type":      "timetravel",
		"run":       runID,
		"amount_ms": duration.Milliseconds(),
	})
	return err
}

// TimeTravelCluster allows a specific cluster to be time-travelled.
func (c *Client) TimeTravelCluster(clusterID string, duration time.Duration) error {
	_, err := c.roundTripCommand(map[string]interface{}{
		"type":      "timetravel",
		"cluster":   clusterID,
		"amount_ms": duration.Milliseconds(),
	})
	return err
}

// AddBucketCluster adds a bucket to a specific cluster
func (c *Client) AddBucketCluster(clusterID string, name, typ string, replicas uint) error {
	_, err := c.roundTripCommand(map[string]interface{}{
		"type":        "addbucket",
		"cluster":     clusterID,
		"name":        name,
		"bucket_type": typ,
		"replicas":    replicas,
	})
	return err
}
