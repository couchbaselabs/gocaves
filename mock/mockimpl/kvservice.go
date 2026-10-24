package mockimpl

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"
	"unsafe"

	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/couchbaselabs/gocaves/contrib/scramserver"
	"github.com/couchbaselabs/gocaves/mock"
	"github.com/couchbaselabs/gocaves/mock/mockauth"
	"github.com/couchbaselabs/gocaves/mock/mockimpl/servers"
	"github.com/golang/snappy"
)

// kvClient represents all the state about a connected kv client.
type kvClient struct {
	// lock guards the fields below it. Replies written by RunDetached read
	// them while the dispatcher handles later requests, and after the
	// connection is lost.
	lock                  sync.Mutex
	client                *servers.MemdClient
	authenticatedUserName string
	selectedBucketName    string
	features              []memd.HelloFeature

	// Set before the reader starts, and not changed afterwards. registered
	// stays set after the connection is lost, for its context store.
	service    *kvService
	isTLS      bool
	localAddr  net.Addr
	remoteAddr net.Addr
	registered *servers.MemdClient

	// detached counts the functions RunDetached started that have not
	// returned.
	detached sync.WaitGroup

	// requests carries packets from the connection's reader to its
	// dispatcher, so the reader goes on reading, and sees the connection
	// close, while a request waits for earlier ones. The lost-client handler
	// closes it.
	requests requestQueue

	// dispatching is the charge of the request the dispatcher is handling.
	// Only the dispatcher goroutine uses it; RunDetached, which handlers call
	// on that goroutine, takes it over for the detached function.
	dispatching int

	// slots holds a token for each request in flight: the one the dispatcher
	// is handling and each detached reply. Only the dispatcher takes one, and
	// RunDetached hands it to the detached function, as it does dispatching.
	slots       chan struct{}
	holdingSlot bool
}

// maxConcurrentCommands is kv_engine's default
// max_concurrent_commands_per_connection: it starts no further request on a
// connection while this many are in flight.
const maxConcurrentCommands = 32

// maxOutstandingRequestBytes bounds the memory the requests a connection holds
// at once take: a request is charged from when the reader takes it until it
// completes, its detached reply included. A client that sends more is
// disconnected, rather than its reader blocked.
var maxOutstandingRequestBytes = 64 * 1024 * 1024

// queuedRequest is a packet, its size on the wire and its charge. done, when
// set, is closed once the request has been dispatched.
type queuedRequest struct {
	pak    *memd.Packet
	size   int
	charge int
	done   chan struct{}
}

// requestOverhead is the memory a request holds besides its bytes on the wire:
// the decoded packet, and its queue entry, which the queue's slice holds up to
// twice over as it grows.
const requestOverhead = int(unsafe.Sizeof(memd.Packet{}) + 2*unsafe.Sizeof(queuedRequest{}))

// requestQueue is a FIFO of packets whose outstanding requests are bounded by
// maxOutstandingRequestBytes. push never blocks, so the reader keeps reading,
// and sees the connection close, whatever the dispatcher waits for.
type requestQueue struct {
	lock        sync.Mutex
	requests    []queuedRequest
	outstanding int
	limit       int
	closed      bool
	ready       chan struct{}
}

// push queues req, charging it req.charge, and reports false, queueing
// nothing, if that would exceed the limit.
func (q *requestQueue) push(req queuedRequest) bool {
	q.lock.Lock()
	if q.outstanding+req.charge > q.limit {
		q.lock.Unlock()
		return false
	}
	q.requests = append(q.requests, req)
	q.outstanding += req.charge
	q.lock.Unlock()
	q.signal()
	return true
}

// release returns the charge of a request that has completed.
func (q *requestQueue) release(size int) {
	q.lock.Lock()
	q.outstanding -= size
	q.lock.Unlock()
}

func (q *requestQueue) close() {
	q.lock.Lock()
	q.closed = true
	q.lock.Unlock()
	q.signal()
}

func (q *requestQueue) signal() {
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// pop returns the oldest request, waiting for one, and false once the queue is
// closed and drained. The request stays charged until it is released.
func (q *requestQueue) pop() (queuedRequest, bool) {
	for {
		q.lock.Lock()
		if len(q.requests) > 0 {
			req := q.requests[0]
			q.requests[0] = queuedRequest{}
			q.requests = q.requests[1:]
			q.lock.Unlock()
			return req, true
		}
		closed := q.closed
		q.lock.Unlock()
		if closed {
			return queuedRequest{}, false
		}
		<-q.ready
	}
}

// LocalAddr returns the local address of this client.
func (c *kvClient) LocalAddr() net.Addr {
	return c.localAddr
}

// RemoteAddr returns the remote address of this client.
func (c *kvClient) RemoteAddr() net.Addr {
	return c.remoteAddr
}

// IsTLS returns whether this client is connected via TLS
// TODO(brett19): Make this return the TLS config for cert-auth.
func (c *kvClient) IsTLS() bool {
	return c.isTLS
}

// ScramServer returns a SCRAM server object specific to this user.
func (c *kvClient) ScramServer() *scramserver.ScramServer {
	var scramServer *scramserver.ScramServer
	c.registered.GetContext(&scramServer)
	return scramServer
}

// SetAuthenticatedUserName sets the name of the user who is authenticated.
func (c *kvClient) SetAuthenticatedUserName(userName string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.authenticatedUserName = userName
}

// AuthenticatedUserName gets the name of the user who is authenticated.
func (c *kvClient) AuthenticatedUserName() string {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.authenticatedUserName
}

// CheckAuthenticated verifies that the currently authenticated user has the specified permissions.
func (c *kvClient) CheckAuthenticated(permission mockauth.Permission, collectionID uint32) bool {
	user := c.service.Node().Cluster().Users().GetUser(c.AuthenticatedUserName())
	if user == nil {
		return false
	}

	b := c.SelectedBucket()
	var bucket, scope, col string
	if b != nil {
		scope, col = b.CollectionManifest().GetByID(collectionID)
		bucket = b.Name()
	}
	if !user.HasPermission(permission, bucket, scope, col) {
		return false
	}

	return true
}

// SetSelectedBucketName sets the currently selected bucket's name.
func (c *kvClient) SetSelectedBucketName(bucketName string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.selectedBucketName = bucketName
}

// SelectedBucketName returns the currently selected bucket's name.
func (c *kvClient) SelectedBucketName() string {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.selectedBucketName
}

// SelectedBucket returns the currently selected bucket.
func (c *kvClient) SelectedBucket() mock.Bucket {
	return c.service.clusterNode.cluster.GetBucket(c.SelectedBucketName())
}

func (c *kvClient) SetFeatures(features []memd.HelloFeature) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.features = features
}

func (c *kvClient) HasFeature(feature memd.HelloFeature) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, foundFeature := range c.features {
		if foundFeature == feature {
			return true
		}
	}
	return false
}

// Source returns the KvService which owns this client.
func (c *kvClient) Source() mock.KvService {
	return c.service
}

var errKvClientLost = errors.New("kv client connection lost")

func (c *kvClient) memdClient() *servers.MemdClient {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.client
}

func (c *kvClient) setMemdClient(cli *servers.MemdClient) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.client = cli
}

// WritePacket tries to write data to the underlying connection. A packet for a
// lost connection is dropped before the outgoing hooks see it.
func (c *kvClient) WritePacket(pak *memd.Packet) error {
	if c.memdClient() == nil {
		return errKvClientLost
	}
	if !c.service.clusterNode.cluster.handleKvPacketOut(c, pak) {
		return nil
	}
	cli := c.memdClient()
	if cli == nil {
		return errKvClientLost
	}
	return cli.WritePacket(pak)
}

// Closed reports whether the connection has been closed or lost.
func (c *kvClient) Closed() bool {
	cli := c.memdClient()
	return cli == nil || cli.IsClosed()
}

// RunDetached runs fn on its own goroutine, counted until it returns. It must
// be called from a request handler, on the dispatcher goroutine.
func (c *kvClient) RunDetached(fn func()) {
	size := c.dispatching
	c.dispatching = 0
	slot := c.holdingSlot
	c.holdingSlot = false
	c.detached.Add(1)
	c.service.workers.Add(1)
	go func() {
		defer c.service.workers.Done()
		defer c.detached.Done()
		defer c.requests.release(size)
		if slot {
			defer func() { <-c.slots }()
		}
		fn()
	}()
}

// Close closes the connection. It does not wait for the reader to exit: a
// handler calls it on the dispatcher, and the reader may be waiting for the
// dispatcher to run a queued HELLO.
func (c *kvClient) Close() error {
	cli := c.memdClient()
	if cli == nil {
		return errKvClientLost
	}
	return cli.Abort()
}

// kvService represents an instance of the kv service.
type kvService struct {
	clusterNode *clusterNodeInst
	server      *servers.MemdServer
	tlsServer   *servers.MemdServer

	// workers counts the dispatchers and detached replies of its clients, so
	// Close returns only once no handler is running.
	workers sync.WaitGroup
}

// newKvServiceOptions enables the specification of default options for a new kv service.
type newKvServiceOptions struct {
}

// newKvService instantiates a new instance of the kv service.
func newKvService(parent *clusterNodeInst, opts newKvServiceOptions) (*kvService, error) {
	svc := &kvService{
		clusterNode: parent,
	}

	srv, err := servers.NewMemdService(servers.NewMemdServerOptions{
		Handlers: servers.MemdServerHandlers{
			NewClientHandler:  svc.handleNewMemdClient,
			LostClientHandler: svc.handleLostMemdClient,
			PacketHandler:     svc.handleMemdPacket,
		},
	})
	if err != nil {
		return nil, err
	}
	svc.server = srv

	if parent.HasFeature(mock.ClusterNodeFeatureTLS) {
		tlsSrv, err := servers.NewMemdService(servers.NewMemdServerOptions{
			Handlers: servers.MemdServerHandlers{
				NewClientHandler:  svc.handleNewTLSMemdClient,
				LostClientHandler: svc.handleLostMemdClient,
				PacketHandler:     svc.handleMemdPacket,
			},
			TLSConfig: parent.cluster.tlsConfig,
		})
		if err != nil {
			return nil, err
		}
		svc.tlsServer = tlsSrv
	}

	return svc, nil
}

// Node returns the ClusterNode which owns this service.
func (s *kvService) Node() mock.ClusterNode {
	return s.clusterNode
}

// Hostname returns the hostname where this service can be accessed.
func (s *kvService) Hostname() string {
	return "127.0.0.1"
}

// ListenPort returns the port this service is listening on.
func (s *kvService) ListenPort() int {
	if s.server == nil {
		return -1
	}
	return s.server.ListenPort()
}

// ListenPortTLS returns the TLS port this service is listening on.
func (s *kvService) ListenPortTLS() int {
	if s.tlsServer == nil {
		return -1
	}
	return s.tlsServer.ListenPort()
}

// GetAllClients returns a list of all the clients connected to this service.
func (s *kvService) GetAllClients() []mock.KvClient {
	var allKvClients []mock.KvClient

	if s.server != nil {
		allClients := s.server.GetAllClients()
		for _, client := range allClients {
			kvCli := s.getKvClient(client)
			allKvClients = append(allKvClients, kvCli)
		}
	}

	if s.tlsServer != nil {
		allClients := s.tlsServer.GetAllClients()
		for _, client := range allClients {
			kvCli := s.getKvClient(client)
			allKvClients = append(allKvClients, kvCli)
		}
	}

	return allKvClients
}

// Close closes the servers, which closes every connection and ends their
// durability waits, then waits for the handlers still running.
func (s *kvService) Close() error {
	var errOut error
	if s.server != nil {
		errOut = s.server.Close()
	}
	if s.tlsServer != nil {
		errOut = s.tlsServer.Close()
	}
	s.workers.Wait()
	return errOut
}

func (s *kvService) getKvClient(cli *servers.MemdClient) *kvClient {
	var kvCli *kvClient
	cli.GetContext(&kvCli)
	return kvCli
}

func (s *kvService) handleNewMemdClient(cli *servers.MemdClient) {
	kvCli := s.getKvClient(cli)
	kvCli.setMemdClient(cli)
	kvCli.service = s
	kvCli.isTLS = false
	kvCli.registered = cli
	kvCli.localAddr = cli.LocalAddr()
	kvCli.remoteAddr = cli.RemoteAddr()
	s.startDispatcher(kvCli)
}

func (s *kvService) handleNewTLSMemdClient(cli *servers.MemdClient) {
	kvCli := s.getKvClient(cli)
	kvCli.setMemdClient(cli)
	kvCli.service = s
	kvCli.isTLS = true
	kvCli.registered = cli
	kvCli.localAddr = cli.LocalAddr()
	kvCli.remoteAddr = cli.RemoteAddr()
	s.startDispatcher(kvCli)
}

func (s *kvService) handleLostMemdClient(cli *servers.MemdClient) {
	kvCli := s.getKvClient(cli)
	kvCli.setMemdClient(nil)
	kvCli.requests.close()

	// The reader ends on EOF from a peer that half-closed its side, while a
	// response may still be blocked writing to it. Closing the socket unblocks
	// that write, so the dispatcher, and service shutdown, can finish.
	if err := cli.Abort(); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Printf("failed to close lost memd client: %s", err)
	}
}

// startDispatcher runs the client's requests in the order its reader queued
// them, until the lost-client handler closes the queue.
func (s *kvService) startDispatcher(kvCli *kvClient) {
	kvCli.requests.ready = make(chan struct{}, 1)
	kvCli.requests.limit = maxOutstandingRequestBytes
	kvCli.slots = make(chan struct{}, maxConcurrentCommands)
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		for {
			req, ok := kvCli.requests.pop()
			if !ok {
				return
			}
			kvCli.dispatching = req.charge
			s.dispatchPacket(kvCli, req.pak)
			kvCli.requests.release(kvCli.dispatching)
			kvCli.dispatching = 0
			if req.done != nil {
				close(req.done)
			}
		}
	}()
}

// reorderableCommands are the commands named in gocbcore's memd package that
// kv_engine's opcode.cc marks Attribute::Reorder: under unordered execution
// kv_engine may run them out of order. Any other command, such as HELLO,
// SELECT_BUCKET or SASL_AUTH, is a barrier.
var reorderableCommands = map[memd.CmdCode]bool{
	memd.CmdGet: true, memd.CmdSet: true, memd.CmdAdd: true, memd.CmdReplace: true,
	memd.CmdDelete: true, memd.CmdIncrement: true, memd.CmdDecrement: true,
	memd.CmdAppend: true, memd.CmdPrepend: true, memd.CmdTouch: true, memd.CmdGAT: true,
	memd.CmdGetReplica: true, memd.CmdGetLocked: true, memd.CmdUnlockKey: true,
	memd.CmdGetClusterConfig: true, memd.CmdGetAllVBSeqnos: true, memd.CmdRangeScanCreate: true,
	memd.CmdSubDocGet: true, memd.CmdSubDocExists: true, memd.CmdSubDocGetCount: true,
	memd.CmdSubDocDictAdd: true, memd.CmdSubDocDictSet: true, memd.CmdSubDocDelete: true,
	memd.CmdSubDocReplace: true, memd.CmdSubDocArrayPushLast: true,
	memd.CmdSubDocArrayPushFirst: true, memd.CmdSubDocArrayInsert: true,
	memd.CmdSubDocArrayAddUnique: true, memd.CmdSubDocCounter: true,
	memd.CmdSubDocMultiLookup: true, memd.CmdSubDocMultiMutation: true,
	memd.CmdSubDocReplaceBodyWithXattr: true,
}

func (s *kvService) handleMemdPacket(cli *servers.MemdClient, pak *memd.Packet, size int) {
	req := queuedRequest{pak: pak, size: size, charge: size + requestOverhead}
	// The features a HELLO enables change how the connection decodes the
	// requests after it, and take effect when its response is written. The
	// reader reads nothing further until then. A HELLO queued behind a stalled
	// request therefore holds the reader, and disconnect detection, with it.
	if pak.Command == memd.CmdHello {
		req.done = make(chan struct{})
		defer func() {
			if req.done != nil {
				<-req.done
			}
		}()
	}
	kvCli := s.getKvClient(cli)
	if !kvCli.requests.push(req) {
		req.done = nil
		log.Printf("disconnecting a client whose outstanding requests take more than %d bytes", kvCli.requests.limit)
		if err := cli.Abort(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("failed to abort memd client: %s", err)
		}
	}
}

func (s *kvService) dispatchPacket(kvCli *kvClient, pak *memd.Packet) {
	if kvCli.memdClient() == nil {
		return
	}

	// On an ordered connection every request starts once the earlier ones
	// have completed. Under unordered execution only a barrier waits, and no
	// later request starts until the barrier has completed too. kv_engine
	// refuses an invalid request as it reads it, so that is never a barrier.
	ordered := !kvCli.HasFeature(memd.FeatureUnorderedExec)
	barrier := (pak.BarrierFrame != nil || !reorderableCommands[pak.Command]) &&
		s.clusterNode.cluster.validKvRequest(kvCli, pak)
	if ordered || barrier {
		kvCli.detached.Wait()
	}
	if !ordered && barrier {
		defer kvCli.detached.Wait()
	}
	// The request waits for a slot while the connection's requests in flight
	// are at kv_engine's limit. A detached reply holds its slot until it is
	// written, and ends once the connection closes.
	kvCli.slots <- struct{}{}
	kvCli.holdingSlot = true
	defer func() {
		if kvCli.holdingSlot {
			<-kvCli.slots
			kvCli.holdingSlot = false
		}
	}()
	// A request queued behind the waits is dropped if the connection was
	// closed or lost during them.
	if kvCli.Closed() {
		return
	}

	s.clusterNode.cluster.handleKvPacketIn(kvCli, pak)
}

// maxInflatedSize is the largest value kv_engine inflates, its maximum packet
// size. A value that inflates to more than the store's 20 MiB item limit but
// within this one is answered too_big by the store, as kv_engine answers it.
const maxInflatedSize = 30 * 1024 * 1024

// inflateRequestValue decompresses a snappy-compressed request value, as
// kv_engine does before dispatch. It refuses a client that did not negotiate
// snappy, and a value whose header claims more than maxInflatedSize, before
// the decoder allocates that length.
func inflateRequestValue(source mock.KvClient, value []byte) ([]byte, bool) {
	if !source.HasFeature(memd.FeatureSnappy) {
		return nil, false
	}
	size, err := snappy.DecodedLen(value)
	if err != nil || size > maxInflatedSize {
		return nil, false
	}
	inflated, err := snappy.Decode(nil, value)
	if err != nil {
		return nil, false
	}
	return inflated, true
}

// inflateHook replaces a compressed request value with the inflated one while
// the built-in handlers run, and restores it when they return. The cluster
// adds it after the handlers, so it runs just before them: hooks added later,
// such as the packet recording of checks, see the request as the client sent
// it, both before and after they call next.
func inflateHook(source mock.KvClient, pak *memd.Packet, start time.Time, next func()) {
	if pak.Datatype&uint8(memd.DatatypeFlagCompressed) == 0 {
		next()
		return
	}

	value, ok := inflateRequestValue(source, pak.Value)
	if !ok {
		err := source.WritePacket(&memd.Packet{
			Magic:   memd.CmdMagicRes,
			Command: pak.Command,
			Opaque:  pak.Opaque,
			Status:  memd.StatusInvalidArgs,
		})
		if err != nil {
			log.Printf("failed to write invalid arguments packet: %s", err)
		}
		return
	}
	compressed, datatype := pak.Value, pak.Datatype
	pak.Value = value
	pak.Datatype &^= uint8(memd.DatatypeFlagCompressed)
	next()
	pak.Value, pak.Datatype = compressed, datatype
}
