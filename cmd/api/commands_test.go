package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnStrCommands(t *testing.T) {
	testObj := &CmdCreatedCluster{
		ConnStr: "hello-world",
		MgmtAddrs: []string{
			"http://some-host:1111",
		},
	}
	testBytes := []byte(`{"type":"createdcluster","mgmt_addrs":["http://some-host:1111"],"connstr":"hello-world"}`)

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
