package mockimpl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/mock"
)

// maxPacketSizeForTest is kv_engine's max_packet_size, header included.
const maxPacketSizeForTest = 30 * 1024 * 1024

// expectClosed checks that the server closes conn without sending anything.
func expectClosed(t *testing.T, conn net.Conn) {
	t.Helper()

	if err := conn.SetReadDeadline(time.Now().Add(testKvTimeout)); err != nil {
		t.Fatalf("failed to set deadline: %v", err)
	}
	n, err := conn.Read(make([]byte, 1))
	if n != 0 || !(errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET)) {
		t.Fatalf("expected the connection closed, got %d bytes and error %v", n, err)
	}
}

// expectServing checks that the cluster serves a new connection.
func expectServing(t *testing.T, c *testKvConn) {
	t.Helper()

	connectTestKv(t, c.cluster).mustSucceed(&memd.Packet{Command: memd.CmdNoop})
}

// A header kv_engine's Header::isValid refuses, or one announcing more than
// max_packet_size, closes the connection before its body is read.
func TestMalformedHeaderClosesConnection(t *testing.T) {
	header := func(magic byte, keyLen uint16, extLen uint8, bodyLen uint32) []byte {
		h := make([]byte, 24)
		h[0] = magic
		h[1] = byte(memd.CmdNoop)
		binary.BigEndian.PutUint16(h[2:], keyLen)
		h[4] = extLen
		binary.BigEndian.PutUint32(h[8:], bodyLen)
		return h
	}
	// The alternative magic carries the framing extras length in the high
	// byte of the key length.
	for _, test := range []struct {
		name   string
		packet []byte
	}{
		{name: "framing extras past the body", packet: header(0x08, 5<<8, 0, 0)},
		{name: "extras and key past the body", packet: append(header(0x80, 10, 4, 8), make([]byte, 8)...)},
		{name: "alternative key past the body", packet: append(header(0x08, 1<<8|8, 0, 8), make([]byte, 8)...)},
		// Refused before the body, which is never sent.
		{name: "illegal magic", packet: header(0x42, 0, 0, 100)},
		{name: "body over max_packet_size", packet: header(0x80, 0, 0, maxPacketSizeForTest-24+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestKvConn(t)
			if _, err := c.netConn.Write(test.packet); err != nil {
				t.Fatalf("failed to write packet: %v", err)
			}
			expectClosed(t, c.netConn)
			expectServing(t, c)
		})
	}
}

// A request of max_packet_size is read; only a larger one closes the
// connection.
func TestPacketAtMaxPacketSizeIsRead(t *testing.T) {
	c := newTestKvConn(t)
	resp := c.Do(&memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Extras: make([]byte, 8), Value: make([]byte, maxPacketSizeForTest-24-8-3)})
	if resp.Status != memd.StatusTooBig {
		t.Fatalf("expected status 0x%02x, got 0x%02x", uint16(memd.StatusTooBig), uint16(resp.Status))
	}
}

// kv_engine closes a connection that sends a packet over 16 KiB, header
// included, before it authenticates, and checks the size once the requests
// before it have run.
func TestUnauthenticatedPacketLimit(t *testing.T) {
	c := newTestKvConn(t)
	node := c.cluster.Nodes()[0]
	dial := func() (net.Conn, *memd.Conn) {
		t.Helper()
		netConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", node.KvService().ListenPort()))
		if err != nil {
			t.Fatalf("failed to dial kv service: %v", err)
		}
		t.Cleanup(func() {
			if err := netConn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("failed to close kv connection: %v", err)
			}
		})
		if err := netConn.SetDeadline(time.Now().Add(testKvTimeout)); err != nil {
			t.Fatalf("failed to set deadline: %v", err)
		}
		return netConn, memd.NewConn(netConn)
	}
	write := func(conn *memd.Conn, pak *memd.Packet) {
		t.Helper()
		pak.Magic = memd.CmdMagicReq
		if err := conn.WritePacket(pak); err != nil {
			t.Fatalf("failed to write %s: %v", pak.Command.Name(), err)
		}
	}
	read := func(conn *memd.Conn) *memd.Packet {
		t.Helper()
		resp, _, err := conn.ReadPacket()
		if err != nil {
			t.Fatalf("failed to read response: %v", err)
		}
		return resp
	}

	_, atLimit := dial()
	write(atLimit, &memd.Packet{Command: memd.CmdNoop, Value: make([]byte, 16*1024-24)})
	read(atLimit)

	overLimit, overLimitConn := dial()
	write(overLimitConn, &memd.Packet{Command: memd.CmdNoop, Value: make([]byte, 16*1024-24+1)})
	expectClosed(t, overLimit)

	// The limit applies before the body is read, so a header alone that
	// announces a large body closes the connection.
	headerOnly, _ := dial()
	header := make([]byte, 24)
	header[0] = byte(memd.CmdMagicReq)
	header[1] = byte(memd.CmdNoop)
	binary.BigEndian.PutUint32(header[8:], maxPacketSizeForTest-24)
	if _, err := headerOnly.Write(header); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	expectClosed(t, headerOnly)

	// The size of a request is checked against the authentication of those
	// before it, so a large request pipelined behind a SASL_AUTH is served.
	// The SASL_AUTH is held, so the reader reaches the large request first.
	c.cluster.KvInHooks().Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		if pak.Command == memd.CmdSASLAuth {
			time.Sleep(200 * time.Millisecond)
		}
		next()
	})
	_, pipelined := dial()
	write(pipelined, &memd.Packet{Command: memd.CmdSASLAuth, Key: []byte("PLAIN"), Value: []byte("\x00Administrator\x00password")})
	write(pipelined, &memd.Packet{Command: memd.CmdSelectBucket, Key: []byte("default")})
	write(pipelined, &memd.Packet{Command: memd.CmdSet, Vbucket: c.vbID, Key: []byte("doc"), Extras: make([]byte, 8), Value: make([]byte, 32*1024)})
	for _, command := range []string{"SASL_AUTH", "SELECT_BUCKET", "SET"} {
		if resp := read(pipelined); resp.Status != memd.StatusSuccess {
			t.Fatalf("%s failed with status 0x%02x", command, uint16(resp.Status))
		}
	}
}

// kv_engine answers framing extras that end inside a frame with invalid
// arguments, once the frames before that point have passed, and keeps the
// connection.
func TestFramingExtrasEndingInsideFrame(t *testing.T) {
	for _, test := range []struct {
		name   string
		frames []byte
		// status is invalid arguments when unset.
		status  memd.StatusCode
		context string
	}{
		{name: "escaped id without its byte", frames: []byte{0xf0}, context: "Invalid encoding in FrameExtras"},
		{name: "escaped size without its byte", frames: []byte{0x0f}, context: "Invalid encoding in FrameExtras"},
		{name: "escaped size past the end", frames: []byte{0x0f, 0x01}, context: "Invalid encoding in FrameExtras"},
		{name: "body past the end", frames: []byte{0x15, 0x01}, context: "Invalid encoding in FrameExtras"},
		{name: "after a valid barrier", frames: []byte{0x00, 0x05}, context: "Invalid encoding in FrameExtras"},
		{name: "after a barrier with a value", frames: []byte{0x01, 0xaa, 0x05}, context: "Barrier should not contain value"},
		{name: "after a PreserveTtl frame", frames: []byte{0x50, 0xf0}, status: memd.StatusNotSupported, context: "This command does not support PreserveTtl"},
		{name: "after a durability frame", frames: []byte{0x11, 0x01, 0x05}, context: "The requested command does not support durability requirements"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestKvConn(t, memd.FeatureAltRequests)
			writeRawRequest(t, c, memd.CmdNoop, 1, test.frames, nil, nil, nil)
			status := test.status
			if status == memd.StatusSuccess {
				status = memd.StatusInvalidArgs
			}
			resp := readResponse(t, c)
			if resp.Opaque != 1 || resp.Status != status {
				t.Fatalf("expected status 0x%02x for opaque 1, got 0x%02x for opaque %d", uint16(status), uint16(resp.Status), resp.Opaque)
			}
			expectErrorContext(t, resp, test.context)
			c.mustSucceed(&memd.Packet{Command: memd.CmdNoop})
		})
	}
}

// The reader of a connection that has not authenticated cannot see it close
// while it waits for the requests before a large packet, so the wait is
// bounded and the packet then refused.
func TestUnauthenticatedWaitIsBounded(t *testing.T) {
	saved := unauthenticatedWaitLimit
	t.Cleanup(func() { unauthenticatedWaitLimit = saved })
	unauthenticatedWaitLimit = 200 * time.Millisecond

	c := newTestKvConn(t)
	c.cluster.KvInHooks().Add(func(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
		if pak.Command == memd.CmdSASLAuth {
			time.Sleep(2 * time.Second)
		}
		next()
	})
	netConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", c.cluster.Nodes()[0].KvService().ListenPort()))
	if err != nil {
		t.Fatalf("failed to dial kv service: %v", err)
	}
	t.Cleanup(func() {
		if err := netConn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("failed to close kv connection: %v", err)
		}
	})
	conn := memd.NewConn(netConn)
	for _, pak := range []*memd.Packet{
		{Magic: memd.CmdMagicReq, Command: memd.CmdSASLAuth, Key: []byte("PLAIN"), Value: []byte("\x00Administrator\x00password")},
		{Magic: memd.CmdMagicReq, Command: memd.CmdNoop, Value: make([]byte, 32*1024)},
	} {
		if err := conn.WritePacket(pak); err != nil {
			t.Fatalf("failed to write %s: %v", pak.Command.Name(), err)
		}
	}

	// Closed before the SASL_AUTH, held for two seconds, is answered.
	start := time.Now()
	expectClosed(t, netConn)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("the connection closed after %v, the wait limit is %v", elapsed, unauthenticatedWaitLimit)
	}
}
