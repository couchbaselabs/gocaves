package services

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v9/memd"
	"github.com/stretchr/testify/assert"
)

func TestMemdBasic(t *testing.T) {
	assert := assert.New(t)

	var newClientInvokes []*MemdClient
	var lostClientInvokes []*MemdClient
	var packetInvokes []*memd.Packet

	svc, err := NewMemdService(NewMemdServiceOptions{
		Handlers: MemdServiceHandlers{
			NewClientHandler: func(cli *MemdClient) {
				newClientInvokes = append(newClientInvokes, cli)
			},
			LostClientHandler: func(cli *MemdClient) {
				lostClientInvokes = append(lostClientInvokes, cli)
			},
			PacketHandler: func(cli *MemdClient, pak *memd.Packet) {
				packetInvokes = append(packetInvokes, pak)

				cli.WritePacket(&memd.Packet{
					Magic:   memd.CmdMagicRes,
					Command: memd.CmdGetClusterConfig,
					Opaque:  pak.Opaque,
				})
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to start memd service: %v", err)
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", svc.ListenPort()))
	if err != nil {
		t.Fatalf("failed to dial memd service: %v", err)
	}
	mconn := memd.NewConn(conn)

	time.Sleep(100 * time.Millisecond)

	assert.Len(newClientInvokes, 1)
	assert.Len(lostClientInvokes, 0)
	assert.Len(packetInvokes, 0)

	err = mconn.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicReq,
		Command: memd.CmdGetClusterConfig,
		Opaque:  1,
	})
	if err != nil {
		t.Fatalf("failed to write packet: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	assert.Len(newClientInvokes, 1)
	assert.Len(lostClientInvokes, 0)
	assert.Len(packetInvokes, 1)

	pak, _, err := mconn.ReadPacket()
	if err != nil {
		t.Fatalf("failed to read packet: %v", err)
	}

	assert.Equal(pak.Magic, memd.CmdMagicRes)
	assert.Equal(pak.Command, memd.CmdGetClusterConfig)
	assert.Equal(pak.Opaque, uint32(1))

	conn.Close()

	time.Sleep(100 * time.Millisecond)

	assert.Len(newClientInvokes, 1)
	assert.Len(lostClientInvokes, 1)
	assert.Len(packetInvokes, 1)
}
