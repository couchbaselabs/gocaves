package servers

import (
	"fmt"
	"net"
	"sync"
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
	var lock sync.Mutex

	svc, err := NewMemdService(NewMemdServerOptions{
		Handlers: MemdServerHandlers{
			NewClientHandler: func(cli *MemdClient) {
				lock.Lock()
				newClientInvokes = append(newClientInvokes, cli)
				lock.Unlock()
			},
			LostClientHandler: func(cli *MemdClient) {
				lock.Lock()
				lostClientInvokes = append(lostClientInvokes, cli)
				lock.Unlock()
			},
			PacketHandler: func(cli *MemdClient, pak *memd.Packet) {
				lock.Lock()
				packetInvokes = append(packetInvokes, pak)
				lock.Unlock()

				err := cli.WritePacket(&memd.Packet{
					Magic:   memd.CmdMagicRes,
					Command: memd.CmdGetClusterConfig,
					Opaque:  pak.Opaque,
				})
				if err != nil {
					t.Fatalf("failed to write packet: %v", err)
				}
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to start memd server: %v", err)
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", svc.ListenPort()))
	if err != nil {
		t.Fatalf("failed to dial memd server: %v", err)
	}
	mconn := memd.NewConn(conn)

	time.Sleep(100 * time.Millisecond)

	lock.Lock()
	assert.Len(newClientInvokes, 1)
	assert.Len(lostClientInvokes, 0)
	assert.Len(packetInvokes, 0)
	lock.Unlock()

	err = mconn.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicReq,
		Command: memd.CmdGetClusterConfig,
		Opaque:  1,
	})
	if err != nil {
		t.Fatalf("failed to write packet: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	lock.Lock()
	assert.Len(newClientInvokes, 1)
	assert.Len(lostClientInvokes, 0)
	assert.Len(packetInvokes, 1)
	lock.Unlock()

	pak, _, err := mconn.ReadPacket()
	if err != nil {
		t.Fatalf("failed to read packet: %v", err)
	}

	assert.Equal(pak.Magic, memd.CmdMagicRes)
	assert.Equal(pak.Command, memd.CmdGetClusterConfig)
	assert.Equal(pak.Opaque, uint32(1))

	conn.Close()

	time.Sleep(100 * time.Millisecond)

	lock.Lock()
	assert.Len(newClientInvokes, 1)
	assert.Len(lostClientInvokes, 1)
	assert.Len(packetInvokes, 1)
	lock.Unlock()
}
