package mockimpl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

// testKvTimeout bounds each request, so a missing reply fails the test
// instead of hanging the package.
const testKvTimeout = 10 * time.Second

// testKvConn is an authenticated memd connection to the first node of a
// default cluster, with the "default" bucket selected.
type testKvConn struct {
	t       *testing.T
	cluster mock.Cluster
	netConn net.Conn
	conn    *memd.Conn
	opaque  uint32
	// vbID is a vbucket for which the connected node is the master.
	vbID uint16
}

func newTestKvConn(t *testing.T, features ...memd.HelloFeature) *testKvConn {
	cluster, err := NewDefaultCluster()
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	t.Cleanup(func() { closeTestCluster(t, cluster) })

	return connectTestKv(t, cluster, features...)
}

// connectTestKv opens a test connection to the first node of cluster, which
// must have a "default" bucket and the Administrator user that
// NewDefaultCluster creates.
func connectTestKv(t *testing.T, cluster mock.Cluster, features ...memd.HelloFeature) *testKvConn {
	node := cluster.Nodes()[0]
	netConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", node.KvService().ListenPort()))
	if err != nil {
		t.Fatalf("failed to dial kv service: %v", err)
	}
	// Cleanups run last-in first-out, so the connection closes before the
	// cluster stops.
	t.Cleanup(func() {
		// A test may close the connection itself.
		if err := netConn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("failed to close kv connection: %v", err)
		}
	})

	c := &testKvConn{
		t:       t,
		cluster: cluster,
		netConn: netConn,
		conn:    memd.NewConn(netConn),
	}

	helloValue := make([]byte, 2*len(features))
	for i, feature := range features {
		binary.BigEndian.PutUint16(helloValue[2*i:], uint16(feature))
	}
	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdHello, Value: helloValue})
	accepted := make(map[memd.HelloFeature]bool)
	for i := 0; i+1 < len(resp.Value); i += 2 {
		accepted[memd.HelloFeature(binary.BigEndian.Uint16(resp.Value[i:]))] = true
	}
	for _, feature := range features {
		if !accepted[feature] {
			t.Fatalf("server did not accept HELLO feature 0x%02x", uint16(feature))
		}
		c.conn.EnableFeature(feature)
	}

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSASLAuth,
		Key:     []byte("PLAIN"),
		Value:   []byte("\x00Administrator\x00password"),
	})
	c.mustSucceed(&memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})

	c.vbID = c.masterVbucket(node)
	return c
}

// closeTestCluster stops the services of every node of cluster.
func closeTestCluster(t *testing.T, cluster mock.Cluster) {
	for _, node := range cluster.Nodes() {
		n := node.(*clusterNodeInst)
		var closers []func() error
		if n.kvService != nil {
			closers = append(closers, n.kvService.Close)
		}
		if n.mgmtService != nil {
			closers = append(closers, n.mgmtService.Close)
		}
		if n.viewService != nil {
			closers = append(closers, n.viewService.Close)
		}
		if n.queryService != nil {
			closers = append(closers, n.queryService.Close)
		}
		if n.searchService != nil {
			closers = append(closers, n.searchService.Close)
		}
		if n.analyticsService != nil {
			closers = append(closers, n.analyticsService.Close)
		}
		for _, closeService := range closers {
			if err := closeService(); err != nil {
				t.Errorf("failed to close a service of node %s: %v", n.ID(), err)
			}
		}
	}
}

func (c *testKvConn) masterVbucket(node mock.ClusterNode) uint16 {
	for vbID, replicaIdx := range c.cluster.GetBucket("default").VbucketOwnership(node) {
		if replicaIdx == 0 {
			return uint16(vbID)
		}
	}
	c.t.Fatalf("node does not own any vbucket as master")
	return 0
}

// Do sends a request and returns its response.
func (c *testKvConn) Do(req *memd.Packet) *memd.Packet {
	c.t.Helper()

	c.opaque++
	req.Magic = memd.CmdMagicReq
	req.Opaque = c.opaque
	if err := c.netConn.SetDeadline(time.Now().Add(testKvTimeout)); err != nil {
		c.t.Fatalf("failed to set deadline: %v", err)
	}
	if err := c.conn.WritePacket(req); err != nil {
		c.t.Fatalf("failed to write %s: %v", req.Command.Name(), err)
	}

	resp, _, err := c.conn.ReadPacket()
	if err != nil {
		c.t.Fatalf("failed to read response to %s: %v", req.Command.Name(), err)
	}
	if resp.Opaque != req.Opaque {
		c.t.Fatalf("response opaque %d does not match request opaque %d", resp.Opaque, req.Opaque)
	}
	return resp
}

func (c *testKvConn) mustSucceed(req *memd.Packet) *memd.Packet {
	c.t.Helper()

	resp := c.Do(req)
	if resp.Status != memd.StatusSuccess {
		c.t.Fatalf("%s failed with status 0x%02x", req.Command.Name(), uint16(resp.Status))
	}
	return resp
}

// subdocSpec is one spec of a SUBDOC_MULTI_MUTATION request.
type subdocSpec struct {
	Op    memd.SubDocOpType
	Flags memd.SubdocFlag
	Path  string
	Value string
}

func encodeMutationSpecs(specs ...subdocSpec) []byte {
	var buf []byte
	for _, spec := range specs {
		hdr := make([]byte, 8)
		hdr[0] = uint8(spec.Op)
		hdr[1] = uint8(spec.Flags)
		binary.BigEndian.PutUint16(hdr[2:], uint16(len(spec.Path)))
		binary.BigEndian.PutUint32(hdr[4:], uint32(len(spec.Value)))
		buf = append(buf, hdr...)
		buf = append(buf, spec.Path...)
		buf = append(buf, spec.Value...)
	}
	return buf
}

func TestKvConnRoundTrip(t *testing.T) {
	c := newTestKvConn(t)

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSet,
		Vbucket: c.vbID,
		Key:     []byte("roundtrip"),
		Value:   []byte(`{"a":1}`),
		Extras:  make([]byte, 8),
	})

	c.mustSucceed(&memd.Packet{
		Command: memd.CmdSubDocMultiMutation,
		Vbucket: c.vbID,
		Key:     []byte("roundtrip"),
		Value:   encodeMutationSpecs(subdocSpec{Op: memd.SubDocOpDictAdd, Path: "b", Value: "2"}),
	})

	resp := c.mustSucceed(&memd.Packet{Command: memd.CmdGet, Vbucket: c.vbID, Key: []byte("roundtrip")})
	if string(resp.Value) != `{"a":1,"b":2}` {
		t.Fatalf("unexpected value %q", resp.Value)
	}
}
