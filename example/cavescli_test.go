package example

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
)

type cavesClient struct {
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
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

	cli := &cavesClient{
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

func (c *cavesClient) writeCommand(req map[string]interface{}) error {
	return c.encoder.Encode(req)
}

func (c *cavesClient) readCommand() (map[string]interface{}, error) {
	var resp map[string]interface{}

	err := c.decoder.Decode(&resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *cavesClient) roundTripCommand(req map[string]interface{}) (map[string]interface{}, error) {
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

func (c *cavesClient) GetConnStr() (string, error) {
	resp, err := c.roundTripCommand(map[string]interface{}{
		"type": "getconnstr",
	})
	if err != nil {
		return "", err
	}

	connStr := resp["connstr"].(string)
	return connStr, nil
}
