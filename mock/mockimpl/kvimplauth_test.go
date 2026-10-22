package mockimpl

import (
	"testing"

	"github.com/couchbase/gocbcore/v10/memd"
)

func TestSelectMissingBucketIsAccessError(t *testing.T) {
	c := newTestKvConn(t)

	resp := c.Do(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("missing")})
	if resp.Status != memd.StatusAccessError {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusAccessError), uint16(resp.Status))
	}
}
