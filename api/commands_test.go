package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnStrCommands(t *testing.T) {
	testObj := &CmdConnStr{
		ConnStr: "hello-world",
	}
	testBytes := []byte(`{"type":"connstr","connstr":"hello-world"}`)

	encodedBytes, err := encodeCommandPacket(testObj)
	if err != nil {
		t.Fatalf("failed to encode bytes: %s", err)
	}

	assert.Equal(t, testBytes, encodedBytes)

	decodedObj, err := decodeCommandPacket(encodedBytes)
	if err != nil {
		t.Fatalf("failed to decode command: %s", err)
	}

	assert.Equal(t, testObj, decodedObj)
}
