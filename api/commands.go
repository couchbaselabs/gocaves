package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// CmdHello represents a hello to the server
type CmdHello struct {
}

// CmdCreateCluster requests a new mock cluster be created.
type CmdCreateCluster struct {
	ClusterID string `json:"id"`
}

// CmdCreatedCluster represents the reply to a create cluster request.
type CmdCreatedCluster struct {
	ConnStr string `json:"connstr"`
}

// CmdStartTesting indicates to start a new report.
type CmdStartTesting struct {
	RunID      string `json:"run"`
	ClientName string `json:"client"`
}

// CmdStartedTesting indicates a new report was started.
type CmdStartedTesting struct {
	ConnStr string `json:"connstr"`
}

// CmdEndTesting indicates to stop a particular report.
type CmdEndTesting struct {
	RunID string `json:"run"`
}

// CmdEndedTesting indicates a particular report has ended.
type CmdEndedTesting struct {
	Report interface{} `json:"report"`
}

// CmdStartTest indicates to start a particular test.
type CmdStartTest struct {
	RunID    string `json:"run"`
	TestName string `json:"test"`
}

// CmdStartedTest is returned when a test has been started.
type CmdStartedTest struct {
	ConnStr        string `json:"connstr"`
	BucketName     string `json:"bucket"`
	ScopeName      string `json:"scope"`
	CollectionName string `json:"collection"`
}

// CmdEndTest indicates to end a particular test.
type CmdEndTest struct {
	RunID  string `json:"run"`
	Result interface{}
}

// CmdEndedTest is returned when a test has been stopped.
type CmdEndedTest struct {
	Error string
}

var cmdsMap = map[string]reflect.Type{
	"hello":          reflect.TypeOf(CmdHello{}),
	"createcluster":  reflect.TypeOf(CmdCreateCluster{}),
	"createdcluster": reflect.TypeOf(CmdCreatedCluster{}),
	"starttesting":   reflect.TypeOf(CmdStartTesting{}),
	"startedtesting": reflect.TypeOf(CmdStartedTesting{}),
	"endtesting":     reflect.TypeOf(CmdEndTesting{}),
	"endedtesting":   reflect.TypeOf(CmdEndedTesting{}),
	"starttest":      reflect.TypeOf(CmdStartTest{}),
	"startedtest":    reflect.TypeOf(CmdStartedTest{}),
	"endtest":        reflect.TypeOf(CmdEndTest{}),
	"endedtest":      reflect.TypeOf(CmdEndedTest{}),
}

func encodeCommandPacket(command interface{}) ([]byte, error) {
	typeofCmd := reflect.TypeOf(command)

	cmdType := ""
	for name, inst := range cmdsMap {
		if inst == typeofCmd || reflect.PtrTo(inst) == typeofCmd {
			cmdType = name
		}
	}

	if cmdType == "" {
		return nil, errors.New("unsupported packet type")
	}

	cmdBytes, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}

	typePrefix := fmt.Sprintf("{\"type\":\"%s\"", cmdType)
	typePrefixBytes := []byte(typePrefix)

	// If there are fields in the encoded command, we need to add a comma after
	// the prefix data to lead into the remaining fields.
	if len(cmdBytes) > 2 {
		typePrefixBytes = append(typePrefixBytes, []byte(",")...)
	}

	// We do a bit of a hack here where we encode the object starting with the
	// type field, then we strip the object-start from the marshalled command
	// and insert the type prefix in front of it!
	return append(typePrefixBytes, cmdBytes[1:]...), nil
}

func decodeCommandPacket(data []byte) (interface{}, error) {
	var header cmdHeader
	err := json.Unmarshal(data, &header)
	if err != nil {
		return nil, err
	}

	var cmdType reflect.Type

	for name, inst := range cmdsMap {
		if name == header.Type {
			cmdType = inst
		}
	}

	if cmdType == nil {
		return nil, errors.New("unsupported packet type")
	}

	cmdObjInst := reflect.New(cmdType)
	cmdObj := cmdObjInst.Interface()
	err = json.Unmarshal(data, &cmdObj)

	return cmdObj, err
}

type cmdHeader struct {
	Type string `json:"type"`
}

type cmdDecoder struct {
	command interface{}
}

func (p *cmdDecoder) UnmarshalJSON(data []byte) error {
	command, err := decodeCommandPacket(data)
	if err != nil {
		return err
	}

	p.command = command
	return nil
}
