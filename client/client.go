package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path"
)

type Client struct {
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
}

type NewClientOptions struct {
	Path    string
	Version string
}

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

	log.Printf("EXECUTING: %+v", cavesProc)

	cavesProc.Stdout = os.Stdin
	cavesProc.Stderr = os.Stderr
	go func() {
		err := cavesProc.Run()
		if err != nil {
			panic(err)
		}
	}()

	log.Printf("Started CAVES")

	conn, err := cavesLsnr.Accept()
	if err != nil {
		log.Printf("failed to accept the caves process: %s", err)
		return nil, err
	}

	log.Printf("CAVES connected")

	cli := &Client{
		conn:    conn,
		encoder: json.NewEncoder(conn),
		decoder: json.NewDecoder(conn),
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

func (c *Client) writeCommand(req map[string]interface{}) error {
	return c.encoder.Encode(req)
}

func (c *Client) readCommand() (map[string]interface{}, error) {
	var resp map[string]interface{}

	err := c.decoder.Decode(&resp)
	if err != nil {
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

func (c *Client) GetConnStr() (string, error) {
	resp, err := c.roundTripCommand(map[string]interface{}{
		"type": "getconnstr",
	})
	if err != nil {
		return "", err
	}

	connStr := resp["connstr"].(string)
	return connStr, nil
}

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

func (c *Client) EndTesting(runID string) error {
	_, err := c.roundTripCommand(map[string]interface{}{
		"type": "endtesting",
		"run":  runID,
	})
	return err
}

type TestStartedSpec struct {
	ConnStr        string
	BucketName     string
	ScopeName      string
	CollectionName string
}

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

func (c *Client) EndTest(runID string) error {
	_, err := c.roundTripCommand(map[string]interface{}{
		"type": "endtest",
		"run":  runID,
	})
	return err
}
