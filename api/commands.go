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

// CmdGetConnStr represents a connstr request
type CmdGetConnStr struct {
}

// CmdConnStr represents the reply to a connstr request.
type CmdConnStr struct {
	ConnStr string `json:"connstr"`
}

var cmdsMap = map[string]reflect.Type{
	"hello":      reflect.TypeOf(CmdHello{}),
	"getconnstr": reflect.TypeOf(CmdGetConnStr{}),
	"connstr":    reflect.TypeOf(CmdConnStr{}),
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
