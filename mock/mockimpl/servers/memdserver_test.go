package servers

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10/memd"
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

	if err := conn.Close(); err != nil {
		t.Fatalf("failed to close connection: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	lock.Lock()
	assert.Len(newClientInvokes, 1)
	assert.Len(lostClientInvokes, 1)
	assert.Len(packetInvokes, 1)
	lock.Unlock()
}

// startTestMemdServer starts a memd server that is closed when the test ends.
func startTestMemdServer(t *testing.T, handlers MemdServerHandlers) *MemdServer {
	t.Helper()

	svc, err := NewMemdService(NewMemdServerOptions{Handlers: handlers})
	if err != nil {
		t.Fatalf("failed to start memd server: %v", err)
	}
	t.Cleanup(func() {
		if err := svc.Close(); err != nil {
			t.Errorf("failed to close memd server: %v", err)
		}
	})
	return svc
}

// dialTestMemdServer connects to svc. Every read and write on the connection
// must finish within five seconds, and the connection is closed when the test
// ends, before the server.
func dialTestMemdServer(t *testing.T, svc *MemdServer) *memd.Conn {
	t.Helper()

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", svc.ListenPort()))
	if err != nil {
		t.Fatalf("failed to dial memd server: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("failed to close connection: %v", err)
		}
	})
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("failed to set deadline: %v", err)
	}
	return memd.NewConn(conn)
}

// echoStatus answers every request with an empty success response.
func echoStatus(t *testing.T, cli *MemdClient, pak *memd.Packet) {
	err := cli.WritePacket(&memd.Packet{
		Magic:   memd.CmdMagicRes,
		Command: pak.Command,
		Opaque:  pak.Opaque,
	})
	if err != nil {
		t.Errorf("failed to write response: %v", err)
	}
}

func TestPacketWaitsForNewClientHandler(t *testing.T) {
	registered := make(chan struct{})

	svc := startTestMemdServer(t, MemdServerHandlers{
		NewClientHandler: func(cli *MemdClient) {
			// Widens the window in which the first packet could be
			// dispatched before the client is registered.
			time.Sleep(50 * time.Millisecond)
			close(registered)
		},
		LostClientHandler: func(cli *MemdClient) {},
		PacketHandler: func(cli *MemdClient, pak *memd.Packet) {
			select {
			case <-registered:
			default:
				t.Errorf("packet dispatched before NewClientHandler returned")
			}
			echoStatus(t, cli, pak)
		},
	})

	mconn := dialTestMemdServer(t, svc)
	err := mconn.WritePacket(&memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdHello, Opaque: 1})
	if err != nil {
		t.Fatalf("failed to write packet: %v", err)
	}
	if _, _, err := mconn.ReadPacket(); err != nil {
		t.Fatalf("failed to read packet: %v", err)
	}
}

func TestNewClientHandlerCanRejectClient(t *testing.T) {
	var lock sync.Mutex
	accepted := 0
	lost := 0

	svc := startTestMemdServer(t, MemdServerHandlers{
		NewClientHandler: func(cli *MemdClient) {
			lock.Lock()
			accepted++
			reject := accepted == 1
			lock.Unlock()

			if reject {
				if err := cli.Close(); err != nil {
					t.Errorf("failed to close rejected client: %v", err)
				}
			}
		},
		LostClientHandler: func(cli *MemdClient) {
			lock.Lock()
			lost++
			lock.Unlock()
		},
		PacketHandler: func(cli *MemdClient, pak *memd.Packet) {
			echoStatus(t, cli, pak)
		},
	})

	// The first connection is rejected during registration and sees the
	// connection close.
	rejected := dialTestMemdServer(t, svc)
	if _, _, err := rejected.ReadPacket(); err == nil {
		t.Fatalf("rejected connection received a packet")
	}

	// The accept loop is still running, so a second connection is served.
	mconn := dialTestMemdServer(t, svc)
	err := mconn.WritePacket(&memd.Packet{Magic: memd.CmdMagicReq, Command: memd.CmdHello, Opaque: 1})
	if err != nil {
		t.Fatalf("failed to write packet: %v", err)
	}
	if _, _, err := mconn.ReadPacket(); err != nil {
		t.Fatalf("failed to read packet: %v", err)
	}

	lock.Lock()
	defer lock.Unlock()
	if lost != 1 {
		t.Errorf("expected the rejected client to be reported lost once, got %d", lost)
	}
}

func TestCloseWaitsForRegistration(t *testing.T) {
	registering := make(chan struct{})
	var lock sync.Mutex
	lost := 0

	svc, err := NewMemdService(NewMemdServerOptions{
		Handlers: MemdServerHandlers{
			NewClientHandler: func(cli *MemdClient) {
				close(registering)
				// The server is closed while this handler runs.
				time.Sleep(50 * time.Millisecond)
			},
			LostClientHandler: func(cli *MemdClient) {
				lock.Lock()
				lost++
				lock.Unlock()
			},
			PacketHandler: func(cli *MemdClient, pak *memd.Packet) {
				echoStatus(t, cli, pak)
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to start memd server: %v", err)
	}

	dialTestMemdServer(t, svc)
	select {
	case <-registering:
	case <-time.After(5 * time.Second):
		t.Fatalf("client was not registered")
	}

	if err := svc.Close(); err != nil {
		t.Fatalf("failed to close memd server: %v", err)
	}

	if clients := svc.GetAllClients(); len(clients) != 0 {
		t.Errorf("expected no clients after close, got %d", len(clients))
	}
	lock.Lock()
	defer lock.Unlock()
	if lost != 1 {
		t.Errorf("expected the client to be reported lost once before Close returned, got %d", lost)
	}
}
