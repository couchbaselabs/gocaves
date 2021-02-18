package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnStrCommands(t *testing.T) {
	testObj := &CmdCreatedCluster{
		ConnStr: "hello-world",
	}
	testBytes := []byte(`{"type":"createdcluster","connstr":"hello-world"}`)

	encodedBytes, err := EncodeCommandPacket(testObj)
	if err != nil {
		t.Fatalf("failed to encode bytes: %s", err)
	}

	assert.Equal(t, testBytes, encodedBytes)

	decodedObj, err := DecodeCommandPacket(encodedBytes)
	if err != nil {
		t.Fatalf("failed to decode command: %s", err)
	}

	assert.Equal(t, testObj, decodedObj)
}
