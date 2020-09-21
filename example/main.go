package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/gocaves/cmd"
)

type cavesClient struct {
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
}

func fakeFunc() {
	// We secretely have a fake function which depends on the CAVES
	// command line such that build errors occur here rather than when
	// we try to actually start up CAVES...
	cmd.Start()
}

func newCavesClient() (*cavesClient, error) {
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

	cavesExecStr := []string{
		"run",
		"main.go",
		fmt.Sprintf("--control-port=%d", cavesListenPort),
	}
	cavesProc := exec.Command("go", cavesExecStr...)
	cavesProc.Dir = "../"
	cavesProc.Stdout = os.Stdin
	cavesProc.Stderr = os.Stderr
	go cavesProc.Run()

	log.Printf("Started CAVES")

	conn, err := cavesLsnr.Accept()
	if err != nil {
		log.Printf("failed to accept the caves process: %s", err)
		return nil, err
	}

	log.Printf("CAVES connected")

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	var helloCmd map[string]interface{}
	err = decoder.Decode(&helloCmd)
	if err != nil {
		log.Printf("failed to receive caves hello: %s", err)
		return nil, err
	}

	if helloCmd["type"] != "hello" {
		log.Printf("first caves command was not hello: %+v", helloCmd)
		return nil, errors.New("no hello")
	}

	return &cavesClient{
		conn:    conn,
		encoder: encoder,
		decoder: decoder,
	}, nil
}

func (c *cavesClient) roundTripCommand(req map[string]interface{}) (map[string]interface{}, error) {
	err := c.encoder.Encode(req)
	if err != nil {
		return nil, err
	}

	var resp map[string]interface{}
	err = c.decoder.Decode(&resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *cavesClient) GetConnStr() (string, error) {
	req := make(map[string]interface{})
	req["type"] = "getconnstr"

	resp, err := c.roundTripCommand(req)
	if err != nil {
		return "", err
	}

	return resp["connstr"].(string), nil
}

func main() {
	gocb.SetLogger(gocb.DefaultStdioLogger())

	caves, err := newCavesClient()
	if err != nil {
		log.Printf("failed to setup caves: %s", err)
		return
	}

	connStr, err := caves.GetConnStr()
	if err != nil {
		log.Printf("failed to get connstr: %s", err)
		return
	}

	connStr = connStr + "?auth_mechanisms=PLAIN"

	log.Printf("got connection string: %s", connStr)

	cluster, err := gocb.Connect(connStr, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
	})
	if err != nil {
		log.Println("Failed to connect:")
		log.Fatalln(err)
	}

	bucket := cluster.Bucket("default")
	collection := bucket.DefaultCollection()

	bucket.WaitUntilReady(10*time.Second, nil)

	// Write a key to the bucket
	testDoc := map[string]interface{}{
		"foo": "bar",
	}
	mutRes, err := collection.Upsert("test-doc", testDoc, nil)
	if err != nil {
		log.Printf("Failed to upsert: %s", err)
	}
	log.Printf("MutRes: %+v", mutRes)

	// Get a key from the bucket
	doc, err := collection.Get("test-doc", nil)
	if err != nil {
		log.Printf("Failed to get: %s", err)
	}

	log.Printf("Doc: %+v", doc)
}
