package servers

import (
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
)

// The reader keeps no reference to a packet once it is decoded, so an idle
// connection does not hold the last packet it read.
func TestReadPacketReleasesBuffer(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() {
		for _, conn := range []net.Conn{server, client} {
			if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("failed to close pipe: %v", err)
			}
		}
	})
	c := newMemdClient(&MemdServer{}, server)

	written := make(chan error, 1)
	go func() {
		written <- memd.NewConn(client).WritePacket(&memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdSet, Key: []byte("doc"), Extras: make([]byte, 8), Value: make([]byte, 1024)})
	}()
	pak, size, err := c.readPacket()
	if err != nil {
		t.Fatalf("failed to read packet: %v", err)
	}
	if err := <-written; err != nil {
		t.Fatalf("failed to write packet: %v", err)
	}
	if pak.Command != memd.CmdSet || len(pak.Value) != 1024 || size != 24+8+3+1024 {
		t.Fatalf("decoded %s with a %d-byte value from %d bytes", pak.Command.Name(), len(pak.Value), size)
	}
	if c.stream.pending != nil {
		t.Errorf("expected the decoded packet released")
	}
}

// A body length near 4 GiB is refused whatever the size of int, so it cannot
// wrap past the limit on a 32-bit build.
func TestCheckHeaderRefusesLargeBodyLength(t *testing.T) {
	for _, bodyLen := range []uint32{maxPacketSize - headerSize + 1, 0x7fffffff, 0xffffffe8, 0xffffffff} {
		header := make([]byte, headerSize)
		header[0] = byte(memd.CmdMagicReq)
		header[1] = byte(memd.CmdNoop)
		binary.BigEndian.PutUint32(header[8:], bodyLen)
		if _, _, err := checkHeader(header); err == nil {
			t.Errorf("body length 0x%x: expected the header refused", bodyLen)
		}
	}
}

// A frame's data comes from the framing extras alone, so a queued request does
// not keep a second copy of its body alive.
func TestFrameDataHoldsNoBody(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() {
		for _, conn := range []net.Conn{server, client} {
			if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("failed to close pipe: %v", err)
			}
		}
	})
	c := newMemdClient(&MemdServer{}, server)
	if err := server.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("failed to set deadline: %v", err)
	}

	written := make(chan error, 1)
	go func() {
		conn := memd.NewConn(client)
		conn.EnableFeature(memd.FeatureAltRequests)
		conn.EnableFeature(memd.FeatureSyncReplication)
		written <- conn.WritePacket(&memd.Packet{
			Magic:                memd.CmdMagicReq,
			Command:              memd.CmdSet,
			Key:                  []byte("doc"),
			Extras:               make([]byte, 8),
			Value:                make([]byte, 1024*1024),
			DurabilityLevelFrame: &memd.DurabilityLevelFrame{DurabilityLevel: memd.DurabilityLevelMajority},
		})
	}()
	pak, _, err := c.readPacket()
	if err != nil {
		t.Fatalf("failed to read packet: %v", err)
	}
	if err := <-written; err != nil {
		t.Fatalf("failed to write packet: %v", err)
	}
	if len(pak.UnsupportedFrames) != 1 {
		t.Fatalf("expected one frame, got %d", len(pak.UnsupportedFrames))
	}
	if data := pak.UnsupportedFrames[0].Data; cap(data) > 255 {
		t.Errorf("frame data has a capacity of %d bytes, more than framing extras can hold", cap(data))
	}
}
